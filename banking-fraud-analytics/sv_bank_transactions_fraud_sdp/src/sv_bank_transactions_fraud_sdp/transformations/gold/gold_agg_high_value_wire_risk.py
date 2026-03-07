"""
Gold Aggregate 2: agg_high_value_wire_risk

Daily summary of high-value wire transfer risk by city and merchant category.

Metrics:
  - date, txn_city, merchant_category
  - wire_count, wire_amount_total, wire_amount_avg
  - flagged_count   (transactions where is_fraud=true among wires)
  - off_hours_wire_count
  - alert_count     (wires that triggered an IP-change alert)
  - risk_score      (composite city-category risk for the day)
  - risk_tier

Used for: geographic fraud heatmaps, merchant category risk dashboards,
          operational triage queues, AML reporting.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")
gold_schema   = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.agg_high_value_wire_risk",
    comment=(
        "Daily high-value wire transfer risk aggregated by city and merchant category. "
        "Surfaces geographic hot spots and high-risk merchant categories for fraud ops. "
        "alert_count cross-joins the IP-change alert table."
    ),
    cluster_by=["txn_date", "txn_city"],
    table_properties={
        "layer": "gold",
        "domain": "fraud_analytics",
        "grain": "date_city_category",
        "refresh_type": "materialized_view",
    },
)
def agg_high_value_wire_risk():
    wires = (
        spark.read.table(f"{silver_schema}.silver_transactions")
        .filter(F.col("is_wire_transfer") == True)
        .withColumn("txn_date", F.to_date("event_timestamp"))
        .groupBy("txn_date", "txn_city", "txn_state", "txn_region", "merchant_category")
        .agg(
            F.count("transaction_id").alias("wire_count"),
            F.sum(F.col("amount").cast("double")).alias("wire_amount_total"),
            F.avg(F.col("amount").cast("double")).alias("wire_amount_avg"),
            F.max(F.col("amount").cast("double")).alias("wire_amount_max"),
            F.sum(F.when(F.col("is_high_value_wire"), 1).otherwise(0)).alias("high_value_wire_count"),
            F.sum(F.when(F.col("is_fraud"), 1).otherwise(0)).alias("flagged_count"),
            F.sum(F.when(F.col("is_off_hours"), 1).otherwise(0)).alias("off_hours_wire_count"),
            F.sum(F.when(F.col("transaction_status") == "Failed", 1).otherwise(0)).alias("failed_count"),
            F.countDistinct("user_id").alias("distinct_users"),
        )
    )

    # Count of IP-change wire alerts per city per day
    alerts_by_city = (
        spark.read.table(f"{silver_schema}.silver_wire_transfer_ip_alerts")
        .withColumn("txn_date", F.to_date("transaction_timestamp"))
        .groupBy("txn_date", "txn_city", "merchant_category")
        .agg(F.count("transaction_id").alias("alert_count"))
    )

    joined = wires.join(alerts_by_city, ["txn_date", "txn_city", "merchant_category"], "left").fillna(0, subset=["alert_count"])

    flagged_rate_col = F.when(F.col("wire_count") > 0, F.col("flagged_count") / F.col("wire_count")).otherwise(0.0)
    hv_rate_col      = F.when(F.col("wire_count") > 0, F.col("high_value_wire_count") / F.col("wire_count")).otherwise(0.0)

    risk_score_col = F.round(
        (flagged_rate_col * 40.0)
        + (hv_rate_col * 25.0)
        + (F.least(F.col("alert_count").cast("double") * 10.0, F.lit(20.0)))
        + (F.when(F.col("off_hours_wire_count") > 0, F.lit(15.0)).otherwise(F.lit(0.0))),
        2,
    )

    return (
        joined
        .withColumn("flagged_rate",       F.round(flagged_rate_col * 100, 2))
        .withColumn("high_value_rate",    F.round(hv_rate_col * 100, 2))
        .withColumn("wire_amount_total",  F.round("wire_amount_total", 2))
        .withColumn("wire_amount_avg",    F.round("wire_amount_avg", 2))
        .withColumn("wire_amount_max",    F.round("wire_amount_max", 2))
        .withColumn("risk_score",         F.least(risk_score_col, F.lit(100.0)))
        .withColumn(
            "risk_tier",
            F.when(F.col("risk_score") >= 40, F.lit("HIGH"))
             .when(F.col("risk_score") >= 15, F.lit("MEDIUM"))
             .otherwise(F.lit("LOW")),
        )
        .withColumn("refreshed_at", F.current_timestamp())
        .select(
            "txn_date", "txn_city", "txn_state", "txn_region", "merchant_category",
            "wire_count", "high_value_wire_count",
            "wire_amount_total", "wire_amount_avg", "wire_amount_max",
            "flagged_count", "flagged_rate",
            "off_hours_wire_count", "failed_count",
            "distinct_users", "alert_count",
            "high_value_rate", "risk_score", "risk_tier", "refreshed_at",
        )
    )
