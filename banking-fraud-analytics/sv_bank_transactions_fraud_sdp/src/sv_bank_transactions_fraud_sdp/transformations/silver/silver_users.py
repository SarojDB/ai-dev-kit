"""
Silver: Cleaned and enriched bank customer profiles.

Transformations:
  - Cast timestamps and dates to proper types
  - Derive credit_tier (PRIME / NEAR_PRIME / SUBPRIME / DEEP_SUBPRIME)
  - Derive balance_segment (HIGH / MID / LOW)
  - Validate email format
  - Filter null user_ids
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.silver_users",
    comment=(
        "Cleaned bank customer profiles with typed fields and derived risk tiers. "
        "credit_tier and balance_segment support downstream fraud scoring."
    ),
    cluster_by=["user_id"],
    table_properties={"layer": "silver", "domain": "users"},
)
def silver_users():
    return (
        spark.readStream.table("bronze_users")
        .filter(F.col("user_id").isNotNull())
        .withColumn("credit_score", F.col("credit_score").cast("int"))
        .withColumn("account_balance", F.col("account_balance").cast("decimal(18,2)"))
        .withColumn("registration_date", F.to_date("registration_date"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Derived credit risk tier
        .withColumn(
            "credit_tier",
            F.when(F.col("credit_score") >= 750, F.lit("PRIME"))
             .when(F.col("credit_score") >= 650, F.lit("NEAR_PRIME"))
             .when(F.col("credit_score") >= 550, F.lit("SUBPRIME"))
             .otherwise(F.lit("DEEP_SUBPRIME")),
        )
        # Derived balance segment
        .withColumn(
            "balance_segment",
            F.when(F.col("account_balance") > 500000,  F.lit("HIGH"))
             .when(F.col("account_balance") > 50000,   F.lit("MID"))
             .otherwise(F.lit("LOW")),
        )
        # Validate email format (basic regex)
        .withColumn(
            "is_valid_email",
            F.col("email").rlike(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"),
        )
        .drop("_rescued_data")
    )
