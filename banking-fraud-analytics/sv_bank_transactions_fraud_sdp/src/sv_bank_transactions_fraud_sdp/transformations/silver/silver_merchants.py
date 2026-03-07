"""
Silver: Cleaned merchant reference data.

Transformations:
  - Cast timestamps to proper types
  - Derive merchant_risk_tier from category and is_high_risk flag
  - Filter null merchant_ids
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")

HIGH_RISK_CATEGORIES = ["Jewelry", "Forex/Remittance"]
MEDIUM_RISK_CATEGORIES = ["Travel", "Electronics", "Insurance"]


@dp.table(
    name=f"{silver_schema}.silver_merchants",
    comment=(
        "Cleaned merchant reference data with risk tier classification. "
        "merchant_risk_tier supports fraud scoring in downstream tables."
    ),
    cluster_by=["merchant_id"],
    table_properties={"layer": "silver", "domain": "merchants"},
)
def silver_merchants():
    return (
        spark.readStream.table("bronze_merchants")
        .filter(F.col("merchant_id").isNotNull())
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        .withColumn(
            "merchant_risk_tier",
            F.when(
                F.col("is_high_risk") | F.col("merchant_category").isin(HIGH_RISK_CATEGORIES),
                F.lit("HIGH"),
            )
            .when(
                F.col("merchant_category").isin(MEDIUM_RISK_CATEGORIES),
                F.lit("MEDIUM"),
            )
            .otherwise(F.lit("LOW")),
        )
        .drop("_rescued_data")
    )
