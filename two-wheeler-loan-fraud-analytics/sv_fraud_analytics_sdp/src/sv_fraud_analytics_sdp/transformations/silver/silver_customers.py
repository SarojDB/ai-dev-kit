"""
Silver layer: clean and enrich customer data.

Business rules applied:
- Drop records missing customer_id or mobile_number (cannot be attributed)
- Cast date_of_birth to DATE; created_timestamp and updated_timestamp to TIMESTAMP
- Validate cibil_score is in valid range (300-900); null-out invalid scores
- Validate monthly_income > 0; null-out implausible incomes
- Normalize gender to uppercase (Male/Female/Other)
- Trim and uppercase region
- Derive loan_eligibility_tier based on CIBIL score:
    PRIME        : cibil_score >= 750  (lowest risk, best rates)
    NEAR_PRIME   : cibil_score >= 650  (moderate risk)
    SUBPRIME     : cibil_score >= 550  (elevated risk, higher rates)
    DEEP_SUBPRIME: cibil_score < 550   (high risk, may be declined)
- Derive income_segment:
    HIGH  : monthly_income > 100,000
    MID   : monthly_income > 40,000
    LOW   : monthly_income <= 40,000
- Derive age_band for cohort analysis
- Drop _rescued_data (internal Auto Loader artifact)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.silver_customers",
    comment=(
        "Cleaned and enriched customer records with typed fields, validated values, "
        "and derived risk tiers. Drops records without customer_id or mobile_number. "
        "Source: bronze_customers streaming table."
    ),
    cluster_by=["customer_id"],
    table_properties={"layer": "silver", "domain": "customers"},
)
def silver_customers():
    return (
        spark.readStream.table("bronze_customers")
        # Hard filters — records without these keys are unattributable
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("mobile_number").isNotNull())
        # Cast timestamps and dates
        .withColumn("date_of_birth", F.to_date("date_of_birth"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Validate and null-out out-of-range CIBIL scores
        .withColumn(
            "cibil_score",
            F.when(
                F.col("cibil_score").between(300, 900),
                F.col("cibil_score")
            ).otherwise(F.lit(None).cast("bigint"))
        )
        # Validate and null-out implausible incomes
        .withColumn(
            "monthly_income",
            F.when(F.col("monthly_income") > 0, F.col("monthly_income"))
             .otherwise(F.lit(None).cast("bigint"))
        )
        # Normalize gender
        .withColumn("gender", F.initcap(F.trim(F.col("gender"))))
        # Normalize region to uppercase
        .withColumn("region", F.upper(F.trim(F.col("region"))))
        # Derive loan eligibility tier based on CIBIL score
        .withColumn(
            "loan_eligibility_tier",
            F.when(F.col("cibil_score") >= 750, F.lit("PRIME"))
             .when(F.col("cibil_score") >= 650, F.lit("NEAR_PRIME"))
             .when(F.col("cibil_score") >= 550, F.lit("SUBPRIME"))
             .when(F.col("cibil_score").isNotNull(), F.lit("DEEP_SUBPRIME"))
             .otherwise(F.lit("UNKNOWN"))
        )
        # Derive income segment for analytics
        .withColumn(
            "income_segment",
            F.when(F.col("monthly_income") > 100000, F.lit("HIGH"))
             .when(F.col("monthly_income") > 40000, F.lit("MID"))
             .when(F.col("monthly_income").isNotNull(), F.lit("LOW"))
             .otherwise(F.lit("UNKNOWN"))
        )
        # Derive age band for cohort analysis
        .withColumn(
            "age_band",
            F.when(F.col("age") < 25, F.lit("18-24"))
             .when(F.col("age") < 35, F.lit("25-34"))
             .when(F.col("age") < 45, F.lit("35-44"))
             .when(F.col("age") < 55, F.lit("45-54"))
             .otherwise(F.lit("55+"))
        )
        # Add bureau risk indicator
        .withColumn(
            "bureau_risk_flag",
            F.col("bureau_enquiries_last_6m") > 4
        )
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_rescued_data")
    )
