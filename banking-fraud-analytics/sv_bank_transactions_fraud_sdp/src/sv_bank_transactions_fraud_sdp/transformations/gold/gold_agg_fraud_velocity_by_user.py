"""
Gold Aggregate 1: agg_fraud_velocity_by_user

Customer-level fraud velocity and risk score aggregation.

Metrics:
  - total_transactions, fraud_count, fraud_rate
  - wire_transfer_count, high_value_wire_count, wire_fraud_count
  - off_hours_txn_count (transactions between 22:00–08:00)
  - failed_txn_count
  - total_amount, avg_amount, max_amount
  - distinct_channels, distinct_cities  (velocity indicators)
  - wire_transfer_ip_alert_count        (cross-table fraud signal)
  - composite fraud_velocity_score (0–100)
  - fraud_risk_tier (HIGH / MEDIUM / LOW)

Used for: fraud triage dashboards, ML feature store, customer risk ranking.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")
gold_schema   = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.agg_fraud_velocity_by_user",
    comment=(
        "Customer-level fraud velocity and composite risk score. "
        "Aggregates transaction behavior, wire transfer patterns, off-hours activity, "
        "and IP-change alert counts into a single fraud_velocity_score (0–100)."
    ),
    cluster_by=["user_id"],
    table_properties={
        "layer": "gold",
        "domain": "fraud_analytics",
        "grain": "user_id",
        "refresh_type": "materialized_view",
    },
)
def agg_fraud_velocity_by_user():
    txns = (
        spark.read.table(f"{silver_schema}.silver_transactions")
        .groupBy("user_id")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum(F.when(F.col("is_fraud"), 1).otherwise(0)).alias("fraud_count"),
            F.sum(F.when(F.col("is_wire_transfer"), 1).otherwise(0)).alias("wire_transfer_count"),
            F.sum(F.when(F.col("is_high_value_wire"), 1).otherwise(0)).alias("high_value_wire_count"),
            F.sum(F.when(F.col("is_fraud") & F.col("is_wire_transfer"), 1).otherwise(0)).alias("wire_fraud_count"),
            F.sum(F.when(F.col("is_off_hours"), 1).otherwise(0)).alias("off_hours_txn_count"),
            F.sum(F.when(F.col("transaction_status") == "Failed", 1).otherwise(0)).alias("failed_txn_count"),
            F.sum(F.col("amount").cast("double")).alias("total_amount"),
            F.avg(F.col("amount").cast("double")).alias("avg_amount"),
            F.max(F.col("amount").cast("double")).alias("max_amount"),
            F.countDistinct("channel").alias("distinct_channels"),
            F.countDistinct("txn_city").alias("distinct_cities"),
            F.countDistinct("device_id").alias("distinct_devices"),
            F.min("event_timestamp").alias("first_txn_ts"),
            F.max("event_timestamp").alias("last_txn_ts"),
        )
    )

    alerts = (
        spark.read.table(f"{silver_schema}.silver_wire_transfer_ip_alerts")
        .groupBy("user_id")
        .agg(
            F.count("transaction_id").alias("wire_transfer_ip_alert_count"),
            F.max("alert_risk_level").alias("max_alert_risk_level"),
        )
    )

    users = spark.read.table(f"{silver_schema}.silver_users").select(
        "user_id", "credit_tier", "balance_segment", "is_fraud_linked", "is_new_account"
    )

    joined = (
        txns
        .join(alerts, "user_id", "left")
        .join(users,  "user_id", "left")
        .fillna(0, subset=["wire_transfer_ip_alert_count"])
        .fillna("", subset=["max_alert_risk_level"])
    )

    fraud_rate_col       = F.when(F.col("total_transactions") > 0, F.col("fraud_count") / F.col("total_transactions")).otherwise(0.0)
    off_hours_rate_col   = F.when(F.col("total_transactions") > 0, F.col("off_hours_txn_count") / F.col("total_transactions")).otherwise(0.0)
    failed_rate_col      = F.when(F.col("total_transactions") > 0, F.col("failed_txn_count") / F.col("total_transactions")).otherwise(0.0)
    velocity_score_col = F.round(
        (fraud_rate_col * 35.0)
        + (F.col("wire_fraud_count").cast("double") / F.greatest(F.col("wire_transfer_count").cast("double"), F.lit(1.0)) * 20.0)
        + (off_hours_rate_col * 10.0)
        + (failed_rate_col * 10.0)
        + (F.least(F.col("wire_transfer_ip_alert_count").cast("double") * 5.0, F.lit(15.0)))
        + (F.when(F.col("is_fraud_linked"), F.lit(10.0)).otherwise(F.lit(0.0))),
        2,
    )

    return (
        joined
        .withColumn("fraud_rate",             F.round(fraud_rate_col * 100, 2))
        .withColumn("off_hours_rate",         F.round(off_hours_rate_col * 100, 2))
        .withColumn("failed_txn_rate",        F.round(failed_rate_col * 100, 2))
        .withColumn("total_amount",           F.round("total_amount", 2))
        .withColumn("avg_amount",             F.round("avg_amount", 2))
        .withColumn("max_amount",             F.round("max_amount", 2))
        .withColumn("fraud_velocity_score",   F.least(velocity_score_col, F.lit(100.0)))
        .withColumn(
            "fraud_risk_tier",
            F.when(F.col("fraud_velocity_score") >= 40, F.lit("HIGH"))
             .when(F.col("fraud_velocity_score") >= 15, F.lit("MEDIUM"))
             .otherwise(F.lit("LOW")),
        )
        .withColumn("refreshed_at", F.current_timestamp())
        .select(
            "user_id", "credit_tier", "balance_segment", "is_fraud_linked", "is_new_account",
            "total_transactions", "fraud_count", "fraud_rate",
            "wire_transfer_count", "high_value_wire_count", "wire_fraud_count",
            "off_hours_txn_count", "off_hours_rate",
            "failed_txn_count", "failed_txn_rate",
            "total_amount", "avg_amount", "max_amount",
            "distinct_channels", "distinct_cities", "distinct_devices",
            "wire_transfer_ip_alert_count", "max_alert_risk_level",
            "fraud_velocity_score", "fraud_risk_tier",
            "first_txn_ts", "last_txn_ts", "refreshed_at",
        )
    )
