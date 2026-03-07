"""
Silver layer: clean and validate dealer data.

Business rules applied:
- Drop records missing dealer_id
- Cast created_timestamp and updated_timestamp to TIMESTAMP
- Validate region is one of valid Indian regions (North/South/East/West); flag others
- Normalize region and dealer_tier to uppercase
- Derive region_zone for higher-level geographic rollups:
    NORTH_CENTRAL : North
    SOUTH         : South
    WEST          : West
    EAST          : East
- Add dealer_risk_level: HIGH if flagged, MEDIUM if unauthorized, LOW otherwise
- Drop _rescued_data
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")

_VALID_REGIONS = ("NORTH", "SOUTH", "EAST", "WEST")


@dp.table(
    name=f"{silver_schema}.silver_dealers",
    comment=(
        "Cleaned dealer records with validated geography and risk classification. "
        "200 dealers across India; flagged dealers identified for fraud analysis. "
        "Source: bronze_dealers streaming table."
    ),
    cluster_by=["dealer_id"],
    table_properties={"layer": "silver", "domain": "dealers"},
)
def silver_dealers():
    return (
        spark.readStream.table("bronze_dealers")
        .filter(F.col("dealer_id").isNotNull())
        # Cast timestamps
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Normalize region and tier
        .withColumn("region", F.upper(F.trim(F.col("region"))))
        .withColumn("dealer_tier", F.upper(F.trim(F.col("dealer_tier"))))
        # Flag invalid regions
        .withColumn(
            "is_valid_region",
            F.col("region").isin(*_VALID_REGIONS)
        )
        # Dealer risk classification
        .withColumn(
            "dealer_risk_level",
            F.when(F.col("is_flagged") == True, F.lit("HIGH"))
             .when(F.col("is_authorized") == False, F.lit("MEDIUM"))
             .otherwise(F.lit("LOW"))
        )
        # Higher-level geographic grouping
        .withColumn(
            "region_zone",
            F.when(F.col("region") == "NORTH", F.lit("NORTH_CENTRAL"))
             .when(F.col("region") == "SOUTH", F.lit("SOUTH"))
             .when(F.col("region") == "WEST", F.lit("WEST"))
             .when(F.col("region") == "EAST", F.lit("EAST"))
             .otherwise(F.lit("UNKNOWN"))
        )
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_rescued_data")
    )
