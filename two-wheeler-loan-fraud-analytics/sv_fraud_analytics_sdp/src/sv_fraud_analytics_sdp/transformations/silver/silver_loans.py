"""
Silver layer: clean and enrich loan application data.

Business rules applied:
- Drop records missing loan_id or customer_id
- Cast date fields (application_date, disbursement_date, first_emi_date) to DATE
- Cast event_timestamp, created_timestamp, updated_timestamp to TIMESTAMP
- Cast interest_rate_pct to DOUBLE
- Validate loan_amount is between INR 50,000 and INR 2,00,000 per policy
- Validate loan_tenure_months is one of approved tenures (24, 36, 48)
- Normalize loan_status and application_channel to uppercase
- Derive ltv_ratio: loan_amount / on_road_price (loan-to-value ratio)
- Derive application_to_disbursement_days: turnaround time
- Derive loan_risk_band based on interest rate (proxy for assessed credit risk):
    STANDARD   : interest_rate_pct < 15%
    ELEVATED   : interest_rate_pct < 18%
    HIGH_RISK  : interest_rate_pct >= 18%
- Add loan_size_bucket for segmentation
- Flag suspicious patterns:
    is_overpriced    : loan_amount > on_road_price (loan exceeds vehicle value — fraud signal)
    is_quick_disburse: disbursed within 1 day of application (process risk)
- Drop _rescued_data
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")

_VALID_TENURES = (24, 36, 48)


@dp.table(
    name=f"{silver_schema}.silver_loans",
    comment=(
        "Cleaned and enriched loan application records with typed dates, validated amounts, "
        "derived risk metrics (LTV, turnaround time), and fraud signal flags. "
        "Source: bronze_loans streaming table."
    ),
    cluster_by=["loan_id"],
    table_properties={"layer": "silver", "domain": "loans"},
)
def silver_loans():
    return (
        spark.readStream.table("bronze_loans")
        # Hard filters
        .filter(F.col("loan_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        # Cast date fields
        .withColumn("application_date", F.to_date("application_date"))
        .withColumn("disbursement_date", F.to_date("disbursement_date"))
        .withColumn("first_emi_date", F.to_date("first_emi_date"))
        # Cast timestamps
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Cast numeric types
        .withColumn("interest_rate_pct", F.col("interest_rate_pct").cast("double"))
        # Validate loan amount is within policy bounds
        .withColumn(
            "loan_amount",
            F.when(
                F.col("loan_amount").between(50000, 200000),
                F.col("loan_amount")
            ).otherwise(F.lit(None).cast("bigint"))
        )
        # Normalize categoricals
        .withColumn("loan_status", F.upper(F.trim(F.col("loan_status"))))
        .withColumn("application_channel", F.upper(F.trim(F.col("application_channel"))))
        .withColumn("product_type", F.initcap(F.trim(F.col("product_type"))))
        # Validate tenure is an approved value
        .withColumn(
            "is_valid_tenure",
            F.col("loan_tenure_months").isin(*_VALID_TENURES)
        )
        # Derive LTV ratio (key underwriting metric)
        .withColumn(
            "ltv_ratio",
            F.round(
                F.col("loan_amount").cast("double") / F.col("on_road_price").cast("double"),
                4
            )
        )
        # Derive turnaround time in days
        .withColumn(
            "application_to_disbursement_days",
            F.datediff(F.col("disbursement_date"), F.col("application_date"))
        )
        # Loan risk band based on interest rate
        .withColumn(
            "loan_risk_band",
            F.when(F.col("interest_rate_pct") < 15.0, F.lit("STANDARD"))
             .when(F.col("interest_rate_pct") < 18.0, F.lit("ELEVATED"))
             .when(F.col("interest_rate_pct").isNotNull(), F.lit("HIGH_RISK"))
             .otherwise(F.lit("UNKNOWN"))
        )
        # Loan size bucket for portfolio segmentation
        .withColumn(
            "loan_size_bucket",
            F.when(F.col("loan_amount") < 75000, F.lit("SMALL"))
             .when(F.col("loan_amount") < 125000, F.lit("MEDIUM"))
             .when(F.col("loan_amount").isNotNull(), F.lit("LARGE"))
             .otherwise(F.lit("UNKNOWN"))
        )
        # Fraud signal: loan amount exceeds on-road price (asset inflation)
        .withColumn(
            "is_overpriced",
            F.col("loan_amount") > F.col("on_road_price")
        )
        # Process risk flag: same-day or next-day disbursement
        .withColumn(
            "is_quick_disburse",
            F.col("application_to_disbursement_days").isNotNull() &
            (F.col("application_to_disbursement_days") <= 1)
        )
        # Derive application month for time-series
        .withColumn("application_month", F.date_format(F.col("application_date"), "yyyy-MM"))
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_rescued_data")
    )
