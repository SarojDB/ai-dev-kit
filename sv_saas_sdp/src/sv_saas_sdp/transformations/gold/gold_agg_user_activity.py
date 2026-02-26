"""
Gold layer: agg_user_activity — daily user activity aggregate (Materialized View).

Typical SaaS metric: DAU/WAU/MAU tracking, feature adoption, error rates.
Grain: event_date × subscription_tier × product_type

Metrics:
  dau               - Daily Active Users (distinct user_id per day)
  total_events      - total event volume
  total_sessions    - unique session count (proxy for engagement depth)
  avg_latency_ms    - P50 latency (infrastructure health indicator)
  p95_latency_ms    - P95 latency (SLO monitoring)
  error_count       - error event count (quality metric)
  error_rate        - % of events that are errors (SLO compliance)
  feature_use_count - feature adoption events
  api_call_count    - API usage (integration depth)
  high_latency_pct  - % of events exceeding 1s (performance health)

Refreshed on each pipeline run from fact_event (gold SCD1).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

gold_schema = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.agg_user_activity",
    comment=(
        "Daily user activity metrics by subscription tier and product. "
        "Key SaaS KPIs: DAU, error rate, latency P95, feature adoption. "
        "Grain: event_date × subscription_tier × product_type."
    ),
    cluster_by=["event_date"],
    table_properties={"layer": "gold", "domain": "engagement", "grain": "daily"},
)
def agg_user_activity():
    return (
        spark.read.table(f"{gold_schema}.fact_event")
        .filter(F.col("event_date").isNotNull())
        .groupBy("event_date", "subscription_tier", "product_type")
        .agg(
            F.countDistinct("user_id").alias("dau"),
            F.count("event_id").alias("total_events"),
            F.countDistinct("session_id").alias("total_sessions"),
            F.avg("latency_ms").cast("decimal(10,2)").alias("avg_latency_ms"),
            F.percentile_approx("latency_ms", 0.95).alias("p95_latency_ms"),
            F.sum(F.when(F.col("is_error_event") == True, 1).otherwise(0))
             .alias("error_count"),
            F.sum(F.when(F.col("event_category") == "feature_use", 1).otherwise(0))
             .alias("feature_use_count"),
            F.sum(F.when(F.col("event_category") == "api_call", 1).otherwise(0))
             .alias("api_call_count"),
            F.sum(F.when(F.col("is_high_latency") == True, 1).otherwise(0))
             .alias("high_latency_events"),
            F.max("_processed_at").alias("last_updated"),
        )
        .withColumn(
            "error_rate",
            F.when(F.col("total_events") > 0,
                   (F.col("error_count") / F.col("total_events")).cast("decimal(6,4)"))
             .otherwise(F.lit(0.0))
        )
        .withColumn(
            "high_latency_pct",
            F.when(F.col("total_events") > 0,
                   (F.col("high_latency_events") / F.col("total_events")).cast("decimal(6,4)"))
             .otherwise(F.lit(0.0))
        )
    )
