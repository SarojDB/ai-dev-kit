"""
Silver layer: clean and enrich EMI payment event data.

Business rules applied:
- Drop records missing payment_id or loan_id
- Cast due_date and payment_date to DATE
- Cast event_timestamp, created_timestamp, updated_timestamp to TIMESTAMP
- Cast penalty_amount to DOUBLE
- Validate emi_amount and actual_amount_paid are non-negative
- Normalize payment_status to uppercase
- Derive payment behavior flags:
    is_paid       : payment_status == 'PAID'
    is_late       : payment_status == 'LATE'
    is_bounced    : payment_status == 'BOUNCED'
    is_defaulted  : payment_status == 'DEFAULTED'
- Derive DPD (Days Past Due) bucket for portfolio aging:
    CURRENT   : days_past_due == 0
    DPD_1_30  : 1-30 days past due (watch list)
    DPD_31_60 : 31-60 days (sub-standard)
    DPD_60_90 : 61-90 days (doubtful)
    DPD_90_PLUS: >90 days (loss)
- Derive payment_shortfall: difference between emi_amount and actual_amount_paid
- Drop _rescued_data
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.silver_emi_payments",
    comment=(
        "Cleaned EMI payment events with typed dates, validated amounts, "
        "payment behavior flags, and DPD aging buckets for portfolio risk analysis. "
        "Source: bronze_emi_payments streaming table."
    ),
    cluster_by=["payment_id"],
    table_properties={"layer": "silver", "domain": "payments"},
)
def silver_emi_payments():
    return (
        spark.readStream.table("bronze_emi_payments")
        # Hard filters
        .filter(F.col("payment_id").isNotNull())
        .filter(F.col("loan_id").isNotNull())
        # Cast date fields
        .withColumn("due_date", F.to_date("due_date"))
        .withColumn("payment_date", F.to_date("payment_date"))
        # Cast timestamps
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Cast penalty to double
        .withColumn("penalty_amount", F.col("penalty_amount").cast("double"))
        # Validate amounts are non-negative
        .withColumn(
            "emi_amount",
            F.when(F.col("emi_amount") >= 0, F.col("emi_amount"))
             .otherwise(F.lit(None).cast("bigint"))
        )
        .withColumn(
            "actual_amount_paid",
            F.when(F.col("actual_amount_paid") >= 0, F.col("actual_amount_paid"))
             .otherwise(F.lit(None).cast("bigint"))
        )
        # Normalize payment status
        .withColumn("payment_status", F.upper(F.trim(F.col("payment_status"))))
        .withColumn("payment_mode", F.upper(F.trim(F.col("payment_mode"))))
        # Payment behavior flags
        .withColumn("is_paid", F.col("payment_status") == "PAID")
        .withColumn("is_late", F.col("payment_status") == "LATE")
        .withColumn("is_bounced", F.col("payment_status") == "BOUNCED")
        .withColumn("is_defaulted", F.col("payment_status") == "DEFAULTED")
        # DPD bucket for portfolio aging classification
        .withColumn(
            "dpd_bucket",
            F.when(F.col("days_past_due") == 0, F.lit("CURRENT"))
             .when(F.col("days_past_due").between(1, 30), F.lit("DPD_1_30"))
             .when(F.col("days_past_due").between(31, 60), F.lit("DPD_31_60"))
             .when(F.col("days_past_due").between(61, 90), F.lit("DPD_60_90"))
             .when(F.col("days_past_due") > 90, F.lit("DPD_90_PLUS"))
             .otherwise(F.lit("UNKNOWN"))
        )
        # Payment shortfall (underpayment amount)
        .withColumn(
            "payment_shortfall",
            F.when(
                F.col("emi_amount").isNotNull() & F.col("actual_amount_paid").isNotNull(),
                (F.col("emi_amount") - F.col("actual_amount_paid")).cast("bigint")
            ).otherwise(F.lit(None).cast("bigint"))
        )
        # Derive payment month for time-series
        .withColumn("due_month", F.date_format(F.col("due_date"), "yyyy-MM"))
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_rescued_data")
    )
