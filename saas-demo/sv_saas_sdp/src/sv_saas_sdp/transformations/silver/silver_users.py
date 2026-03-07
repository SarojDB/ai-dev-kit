"""
Silver layer: clean and enrich user data.

Business rules applied:
- Drop records with null user_id (unidentifiable)
- Validate email format; flag invalid emails rather than dropping
- Cast timestamps to proper TIMESTAMP types
- Derive email_domain, is_enterprise, account_age_days
- Standardize subscription_tier casing
- Remove Auto Loader rescue column (_rescued_data)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")
EMAIL_PATTERN = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"


@dp.table(
    name=f"{silver_schema}.silver_users",
    comment=(
        "Cleaned and enriched user records. "
        "Invalid emails flagged, timestamps cast, derived fields added. "
        "Source: bronze_users streaming table."
    ),
    cluster_by=["user_id"],
    table_properties={"layer": "silver", "domain": "users"},
)
def silver_users():
    return (
        spark.readStream.table("bronze_users")
        # Hard filter: user_id is mandatory
        .filter(F.col("user_id").isNotNull())
        # Drop Auto Loader rescue column if present
        .drop("_rescued_data")
        # Cast timestamps
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Data quality flag: is email valid
        .withColumn(
            "is_valid_email",
            F.when(F.col("email").isNull(), False)
             .when(F.col("email").rlike(EMAIL_PATTERN), True)
             .otherwise(False)
        )
        # Derived: email domain for analytics segmentation
        .withColumn(
            "email_domain",
            F.when(F.col("email").contains("@"), F.split("email", "@").getItem(1))
             .otherwise(F.lit(None))
        )
        # Derived: enterprise flag (Enterprise or Business tier)
        .withColumn(
            "is_enterprise",
            F.col("subscription_tier").isin("Enterprise", "Business")
        )
        # Derived: account age from created_timestamp to now
        .withColumn(
            "account_age_days",
            F.datediff(F.current_date(), F.to_date("created_timestamp"))
        )
        # Standardize subscription_tier (initcap in case source sends lowercase)
        .withColumn("subscription_tier", F.initcap(F.col("subscription_tier")))
        # Standardize country (uppercase ISO codes expected, trim whitespace)
        .withColumn("country", F.trim(F.upper(F.col("country"))))
        # Add processing timestamp
        .withColumn("_processed_at", F.current_timestamp())
    )
