"""
Silver layer: clean and enrich 2-wheeler product catalog data.

Business rules applied:
- Drop records missing product_id
- Cast created_timestamp and updated_timestamp to TIMESTAMP
- Validate price fields are positive; null-out invalid prices
- Normalize product_type and fuel_type to uppercase
- Derive price_segment based on ex_showroom_price:
    BUDGET   : < INR 80,000   (entry-level commuters, basic scooters)
    MID      : < INR 1,50,000 (mid-range sports, premium scooters)
    PREMIUM  : >= INR 1,50,000 (Royal Enfield, adventure bikes, high-end EVs)
- Derive loan_to_value_ratio guidance:
    Max LTV 85% for budget, 80% for mid, 75% for premium
- Add is_high_value flag for premium segment risk scoring
- Drop _rescued_data
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.silver_products",
    comment=(
        "Cleaned 2-wheeler product catalog with typed fields, validated prices, "
        "and derived price segments. Covers Petrol, Electric, and mixed fuel types. "
        "Source: bronze_products streaming table."
    ),
    cluster_by=["product_id"],
    table_properties={"layer": "silver", "domain": "products"},
)
def silver_products():
    return (
        spark.readStream.table("bronze_products")
        .filter(F.col("product_id").isNotNull())
        # Cast timestamps
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Validate prices are positive; null-out invalid
        .withColumn(
            "ex_showroom_price",
            F.when(F.col("ex_showroom_price") > 0, F.col("ex_showroom_price"))
             .otherwise(F.lit(None).cast("bigint"))
        )
        .withColumn(
            "on_road_price_min",
            F.when(F.col("on_road_price_min") > 0, F.col("on_road_price_min"))
             .otherwise(F.lit(None).cast("bigint"))
        )
        .withColumn(
            "on_road_price_max",
            F.when(F.col("on_road_price_max") > 0, F.col("on_road_price_max"))
             .otherwise(F.lit(None).cast("bigint"))
        )
        # Normalize categorical values
        .withColumn("product_type", F.initcap(F.trim(F.col("product_type"))))
        .withColumn("fuel_type", F.initcap(F.trim(F.col("fuel_type"))))
        .withColumn("brand", F.trim(F.col("brand")))
        # Derive price segment for risk-based loan pricing
        .withColumn(
            "price_segment",
            F.when(F.col("ex_showroom_price") < 80000, F.lit("BUDGET"))
             .when(F.col("ex_showroom_price") < 150000, F.lit("MID"))
             .when(F.col("ex_showroom_price").isNotNull(), F.lit("PREMIUM"))
             .otherwise(F.lit("UNKNOWN"))
        )
        # Max LTV guideline per segment
        .withColumn(
            "max_ltv_pct",
            F.when(F.col("ex_showroom_price") < 80000, F.lit(85.0))
             .when(F.col("ex_showroom_price") < 150000, F.lit(80.0))
             .otherwise(F.lit(75.0))
        )
        # Price spread (on-road markup range)
        .withColumn(
            "on_road_markup_pct",
            F.round(
                (F.col("on_road_price_min").cast("double") - F.col("ex_showroom_price").cast("double"))
                / F.col("ex_showroom_price").cast("double") * 100.0,
                1
            )
        )
        .withColumn("is_high_value", F.col("ex_showroom_price") >= 150000)
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_rescued_data")
    )
