"""
Silver layer: clean and enrich order data.

Business rules applied:
- Drop records missing order_id or user_id (unprocessable)
- Filter negative or zero unit_price (data quality issue)
- Cast monetary values to DECIMAL for precision
- Derive order_month, order_quarter, is_high_value (>$500), order_category
- Flag potentially fraudulent orders (total_value > $900 AND quantity > 10)
- Standardize status values
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T

silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.silver_orders",
    comment=(
        "Cleaned and enriched order records. "
        "Monetary values cast to DECIMAL, business categorizations added. "
        "Source: bronze_orders streaming table."
    ),
    cluster_by=["order_id"],
    table_properties={"layer": "silver", "domain": "orders"},
)
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")
        # Hard filters
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("unit_price").isNotNull() & (F.col("unit_price") > 0))
        # Drop rescue column
        .drop("_rescued_data")
        # Cast timestamps
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Cast monetary values to DECIMAL(12,2)
        .withColumn("unit_price", F.col("unit_price").cast(T.DecimalType(12, 2)))
        .withColumn("total_value", F.col("total_value").cast(T.DecimalType(12, 2)))
        # Derived: date parts for partitioning and time-series analysis
        .withColumn("order_date", F.to_date("created_timestamp"))
        .withColumn("order_month", F.date_trunc("month", F.col("created_timestamp")))
        .withColumn(
            "order_quarter",
            F.concat(
                F.year("created_timestamp").cast("string"),
                F.lit("-Q"),
                F.ceil(F.month("created_timestamp") / 3).cast("string")
            )
        )
        # Derived: value segmentation for SaaS ARR analysis
        .withColumn(
            "order_category",
            F.when(F.col("total_value") >= 800, "premium")
             .when(F.col("total_value") >= 500, "high")
             .when(F.col("total_value") >= 200, "mid")
             .otherwise("low")
        )
        # Derived: high-value flag (above $500 — threshold for enterprise deals)
        .withColumn("is_high_value", F.col("total_value") >= 500)
        # Derived: anomaly flag for review (very high value + high quantity)
        .withColumn(
            "is_anomaly_flag",
            (F.col("total_value") > 900) & (F.col("quantity") > 10)
        )
        # Standardize status to lowercase
        .withColumn("status", F.lower(F.col("status")))
        .withColumn("_processed_at", F.current_timestamp())
    )
