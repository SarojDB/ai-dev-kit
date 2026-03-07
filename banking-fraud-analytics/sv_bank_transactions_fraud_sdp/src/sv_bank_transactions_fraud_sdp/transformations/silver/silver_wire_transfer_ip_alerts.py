"""
Silver: Wire Transfer IP-Change Alerts (Materialized View)

Business Rule (core requirement):
  Flag any wire transfer over INR 50,000 (amount > 50,000) that occurs
  within 60 minutes of an IP address risk event (MFA change, VPN, TOR,
  foreign login, or flagged suspicious login) for the same user.

Logic:
  1. high_value_wires  — silver_transactions WHERE is_high_value_wire = true
  2. ip_risk_logins    — silver_login_logs WHERE is_ip_risk = true
  3. Join on user_id WHERE:
       wire.event_timestamp BETWEEN login.event_timestamp
                                AND login.event_timestamp + INTERVAL 60 MINUTES

Output:
  One alert row per (transaction_id, login event) pair where the window is met.
  minutes_after_login shows how long after the suspicious login the wire was placed.
  alert_risk_level: HIGH (TOR/foreign), MEDIUM (VPN/MFA), LOW (suspicious flag).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")


@dp.materialized_view(
    name=f"{silver_schema}.silver_wire_transfer_ip_alerts",
    comment=(
        "Fraud alert: wire transfers >INR 5,00,000 occurring within 60 min of an "
        "IP risk login event for the same user_id. Refreshed as a materialized view "
        "on each pipeline run."
    ),
    cluster_by=["user_id", "transaction_id"],
    table_properties={
        "layer": "silver",
        "domain": "fraud_alerts",
        "alert_type": "wire_transfer_ip_change",
    },
)
def silver_wire_transfer_ip_alerts():
    wire_txns = (
        spark.read.table(f"{silver_schema}.silver_transactions")
        .filter(F.col("is_high_value_wire") == True)
        .select(
            "transaction_id",
            "user_id",
            "merchant_id",
            "merchant_category",
            F.col("amount").cast("double").alias("amount"),
            "transaction_type",
            "channel",
            "txn_city",
            "txn_state",
            F.col("ip_address").alias("txn_ip_address"),
            "device_id",
            "is_fraud",
            "fraud_type",
            "transaction_status",
            F.col("event_timestamp").alias("transaction_timestamp"),
            "created_timestamp",
        )
        .alias("txn")
    )

    ip_risk_logins = (
        spark.read.table(f"{silver_schema}.silver_login_logs")
        .filter(F.col("is_ip_risk") == True)
        .filter(F.col("login_status") == "Success")   # Only successful risky logins matter for ATO
        .select(
            "online_login_id",
            "user_id",
            F.col("ip_address").alias("login_ip_address"),
            "device_id",
            F.col("event_timestamp").alias("login_timestamp"),
            "mfa_change_flag",
            "is_vpn",
            "is_tor",
            "country_code",
            "is_suspicious",
            "login_risk_level",
            "city",
        )
        .alias("login")
    )

    sixty_mins = F.expr("INTERVAL 60 MINUTES")

    joined = wire_txns.join(
        ip_risk_logins,
        (F.col("txn.user_id") == F.col("login.user_id"))
        & (F.col("txn.transaction_timestamp") >= F.col("login.login_timestamp"))
        & (F.col("txn.transaction_timestamp") <= F.col("login.login_timestamp") + sixty_mins),
        "inner",
    )

    return (
        joined
        .withColumn(
            "minutes_after_login",
            F.round(
                (F.col("txn.transaction_timestamp").cast("long")
                 - F.col("login.login_timestamp").cast("long")) / 60.0,
                1,
            ),
        )
        .withColumn(
            "alert_risk_level",
            F.when(
                F.col("login.is_tor")
                | F.col("login.country_code").isin("NG", "RU", "UA", "CN"),
                F.lit("HIGH"),
            )
            .when(
                F.col("login.is_vpn") | F.col("login.mfa_change_flag"),
                F.lit("MEDIUM"),
            )
            .otherwise(F.lit("LOW")),
        )
        .withColumn("alert_generated_at", F.current_timestamp())
        .select(
            F.col("txn.transaction_id"),
            F.col("txn.user_id"),
            F.col("txn.amount"),
            F.col("txn.transaction_type"),
            F.col("txn.channel"),
            F.col("txn.txn_city"),
            F.col("txn.txn_state"),
            F.col("txn.txn_ip_address"),
            F.col("txn.device_id").alias("txn_device_id"),
            F.col("txn.merchant_id"),
            F.col("txn.merchant_category"),
            F.col("txn.transaction_status"),
            F.col("txn.is_fraud"),
            F.col("txn.fraud_type"),
            F.col("txn.transaction_timestamp"),
            F.col("login.online_login_id").alias("triggering_login_id"),
            F.col("login.login_ip_address"),
            F.col("login.device_id").alias("login_device_id"),
            F.col("login.login_timestamp"),
            F.col("login.mfa_change_flag"),
            F.col("login.is_vpn"),
            F.col("login.is_tor"),
            F.col("login.country_code").alias("login_country_code"),
            F.col("login.login_risk_level"),
            F.col("login.city").alias("login_city"),
            "minutes_after_login",
            "alert_risk_level",
            "alert_generated_at",
        )
    )
