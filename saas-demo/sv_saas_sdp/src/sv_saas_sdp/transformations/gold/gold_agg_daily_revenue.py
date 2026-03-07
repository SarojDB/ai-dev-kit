"""
Gold layer: agg_daily_revenue — daily revenue aggregate (Materialized View).

Typical SaaS metric: MRR/ARR tracking, cohort revenue, product mix analysis.
Grain: order_date × subscription_tier × product_type

Metrics:
  order_count        - total orders
  unique_customers   - distinct buyers (DAC: Daily Active Customers)
  total_revenue      - gross revenue
  avg_order_value    - AOV (key SaaS pricing health metric)
  completed_orders   - successful orders for net revenue calc
  refunded_orders    - refund count (churn signal)
  net_revenue        - total_revenue from completed orders only
  high_value_count   - premium deal count

Refreshed on each pipeline run from fact_order (gold SCD1).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

gold_schema = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.agg_daily_revenue",
    comment=(
        "Daily revenue metrics by subscription tier and product. "
        "Key SaaS KPIs: AOV, DAC, net revenue, refund rate. "
        "Grain: order_date × subscription_tier × product_type."
    ),
    cluster_by=["order_date"],
    table_properties={"layer": "gold", "domain": "revenue", "grain": "daily"},
)
def agg_daily_revenue():
    return (
        spark.read.table(f"{gold_schema}.fact_order")
        .filter(F.col("order_date").isNotNull())
        .groupBy("order_date", "subscription_tier", "product_type")
        .agg(
            F.count("order_id").alias("order_count"),
            F.countDistinct("user_id").alias("unique_customers"),
            F.sum("total_value").cast("decimal(18,2)").alias("total_revenue"),
            F.avg("total_value").cast("decimal(12,2)").alias("avg_order_value"),
            F.sum(F.when(F.col("status") == "completed", F.col("total_value")).otherwise(0))
             .cast("decimal(18,2)").alias("net_revenue"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0))
             .alias("completed_orders"),
            F.sum(F.when(F.col("status") == "refunded", 1).otherwise(0))
             .alias("refunded_orders"),
            F.sum(F.when(F.col("is_high_value") == True, 1).otherwise(0))
             .alias("high_value_count"),
            F.max("_processed_at").alias("last_updated"),
        )
        .withColumn(
            "refund_rate",
            F.when(F.col("order_count") > 0,
                   (F.col("refunded_orders") / F.col("order_count")).cast("decimal(6,4)"))
             .otherwise(F.lit(0.0))
        )
    )
