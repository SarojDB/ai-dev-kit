"""
Silver: Cleaned login log records with enriched IP risk signals.

Transformations:
  - Cast timestamps to proper types
  - Derive is_ip_risk: any login flagged as suspicious/VPN/TOR/foreign/MFA-change
  - Derive login_risk_level: LOW / MEDIUM / HIGH
  - Derive is_domestic: country_code == 'IN'
  - Filter nulls on business keys

is_ip_risk is the primary flag used by silver_wire_transfer_ip_alerts
to detect logins where IP context changed before a high-value wire transfer.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.silver_login_logs",
    comment=(
        "Cleaned login events with typed fields and derived IP risk flags. "
        "is_ip_risk=true when mfa_change_flag, is_vpn, is_tor, foreign country, or is_suspicious."
    ),
    cluster_by=["online_login_id", "user_id"],
    table_properties={"layer": "silver", "domain": "login_logs"},
)
def silver_login_logs():
    is_ip_risk_expr = (
        F.col("mfa_change_flag")
        | F.col("is_vpn")
        | F.col("is_tor")
        | (F.col("country_code") != "IN")
        | F.col("is_suspicious")
    )

    return (
        spark.readStream.table("bronze_login_logs")
        .filter(F.col("online_login_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .withColumn("event_timestamp",   F.to_timestamp("event_timestamp"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        .withColumn("failed_attempt_num", F.col("failed_attempt_num").cast("int"))
        .withColumn("session_duration_sec", F.col("session_duration_sec").cast("int"))
        # IP risk signals
        .withColumn("is_ip_risk", is_ip_risk_expr)
        .withColumn("is_domestic", F.col("country_code") == "IN")
        # Risk level derived from individual signals
        .withColumn(
            "login_risk_level",
            F.when(
                F.col("is_tor") | (F.col("country_code").isin("NG", "RU", "UA")),
                F.lit("HIGH"),
            )
            .when(
                F.col("is_vpn") | F.col("mfa_change_flag") | (F.col("country_code") != "IN"),
                F.lit("MEDIUM"),
            )
            .when(
                F.col("is_suspicious") | (F.col("failed_attempt_num") >= 3),
                F.lit("LOW"),
            )
            .otherwise(F.lit("NORMAL")),
        )
        # Failed login flag
        .withColumn("is_failed_login", F.col("login_status") == "Failed")
        .drop("_rescued_data")
    )
