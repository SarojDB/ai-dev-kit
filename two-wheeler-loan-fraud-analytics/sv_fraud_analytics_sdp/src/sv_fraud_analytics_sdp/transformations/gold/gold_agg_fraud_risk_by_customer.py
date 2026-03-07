"""
Gold layer: agg_fraud_risk_by_customer — Customer-level fraud risk aggregate.

Materialized view joining customers, loans, EMI payments, and KYC events
to produce a comprehensive fraud risk profile per customer.

Grain: one row per customer_id

Key metrics:
  total_loans           - total loan applications by customer
  fraud_loans           - confirmed fraud loans (ML label)
  defaulted_loans       - loans in Defaulted status
  npa_loans             - loans in NPA (Non-Performing Asset) status
  total_emi_payments    - total EMI events in payment history
  bounced_payments      - NACH/cheque bounce count (early default signal)
  defaulted_payments    - explicitly defaulted EMI installments
  max_days_past_due     - worst DPD observed across all loans
  total_kyc_attempts    - total KYC verification attempts
  failed_kyc_attempts   - count of failed KYC attempts
  has_suspicious_kyc    - TRUE if any attempt flagged suspicious
  has_document_fraud    - TRUE if any attempt had document fraud failure reason
  fraud_risk_score      - composite 0-100 score (higher = more risky)

Fraud risk score components (weights sum to 100):
  30% — default rate across loans
  25% — EMI bounce/default rate
  15% — KYC failure rate
  15% — has_suspicious_kyc flag (15 points if TRUE)
  15% — has_document_fraud flag (15 points if TRUE)

Refreshed on every pipeline run from silver tables.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.agg_fraud_risk_by_customer",
    comment=(
        "Customer-level fraud risk aggregation. Composite fraud_risk_score (0-100) "
        "derived from default behavior, EMI payment patterns, and KYC anomalies. "
        "Grain: customer_id. Refreshed on every pipeline run."
    ),
    cluster_by=["customer_id"],
    table_properties={"layer": "gold", "domain": "fraud_risk", "grain": "customer"},
)
def agg_fraud_risk_by_customer():
    customers = spark.read.table(f"{silver_schema}.silver_customers")

    loans = (
        spark.read.table(f"{silver_schema}.silver_loans")
        .groupBy("customer_id")
        .agg(
            F.count("loan_id").alias("total_loans"),
            F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_loans"),
            F.sum(F.when(F.col("loan_status") == "DEFAULTED", 1).otherwise(0)).alias("defaulted_loans"),
            F.sum(F.when(F.col("loan_status") == "NPA", 1).otherwise(0)).alias("npa_loans"),
            F.sum(F.when(F.col("loan_status") == "REJECTED", 1).otherwise(0)).alias("rejected_loans"),
            F.avg("ltv_ratio").alias("avg_ltv_ratio"),
            F.sum("loan_amount").alias("total_loan_amount"),
            F.sum(F.when(F.col("is_overpriced") == True, 1).otherwise(0)).alias("overpriced_loan_count"),
        )
    )

    payments = (
        spark.read.table(f"{silver_schema}.silver_emi_payments")
        .groupBy("customer_id")
        .agg(
            F.count("payment_id").alias("total_emi_payments"),
            F.sum(F.when(F.col("is_bounced") == True, 1).otherwise(0)).alias("bounced_payments"),
            F.sum(F.when(F.col("is_defaulted") == True, 1).otherwise(0)).alias("defaulted_payments"),
            F.sum(F.when(F.col("is_late") == True, 1).otherwise(0)).alias("late_payments"),
            F.max("days_past_due").alias("max_days_past_due"),
            F.sum("payment_shortfall").alias("total_payment_shortfall"),
        )
    )

    kyc = (
        spark.read.table(f"{silver_schema}.silver_kyc_events")
        .groupBy("customer_id")
        .agg(
            F.count("kyc_event_id").alias("total_kyc_attempts"),
            F.sum(F.when(F.col("is_failed") == True, 1).otherwise(0)).alias("failed_kyc_attempts"),
            F.max(F.col("is_suspicious").cast("int")).cast("boolean").alias("has_suspicious_kyc"),
            F.max(F.col("is_document_fraud").cast("int")).cast("boolean").alias("has_document_fraud"),
        )
    )

    combined = (
        customers
        .join(loans, "customer_id", "left")
        .join(payments, "customer_id", "left")
        .join(kyc, "customer_id", "left")
        # Fill nulls for customers with no loans/payments/kyc
        .fillna({
            "total_loans": 0, "fraud_loans": 0, "defaulted_loans": 0, "npa_loans": 0,
            "rejected_loans": 0, "total_loan_amount": 0, "overpriced_loan_count": 0,
            "total_emi_payments": 0, "bounced_payments": 0, "defaulted_payments": 0,
            "late_payments": 0, "max_days_past_due": 0, "total_payment_shortfall": 0,
            "total_kyc_attempts": 0, "failed_kyc_attempts": 0,
            "has_suspicious_kyc": False, "has_document_fraud": False,
        })
    )

    # Compute composite fraud risk score (0-100)
    return (
        combined
        .withColumn(
            "default_rate",
            F.when(F.col("total_loans") > 0,
                F.col("defaulted_loans").cast("double") / F.col("total_loans").cast("double")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "bounce_rate",
            F.when(F.col("total_emi_payments") > 0,
                (F.col("bounced_payments") + F.col("defaulted_payments")).cast("double")
                / F.col("total_emi_payments").cast("double")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "kyc_failure_rate",
            F.when(F.col("total_kyc_attempts") > 0,
                F.col("failed_kyc_attempts").cast("double") / F.col("total_kyc_attempts").cast("double")
            ).otherwise(F.lit(0.0))
        )
        # Composite score: 30% default, 25% bounce, 15% KYC fail, 15% suspicious KYC, 15% doc fraud
        .withColumn(
            "fraud_risk_score",
            F.round(
                (F.col("default_rate") * 30.0)
                + (F.col("bounce_rate") * 25.0)
                + (F.col("kyc_failure_rate") * 15.0)
                + F.when(F.col("has_suspicious_kyc") == True, F.lit(15.0)).otherwise(F.lit(0.0))
                + F.when(F.col("has_document_fraud") == True, F.lit(15.0)).otherwise(F.lit(0.0)),
                2
            )
        )
        .withColumn(
            "fraud_risk_tier",
            F.when(F.col("fraud_risk_score") >= 40, F.lit("HIGH"))
             .when(F.col("fraud_risk_score") >= 20, F.lit("MEDIUM"))
             .otherwise(F.lit("LOW"))
        )
        .select(
            "customer_id", "full_name", "region", "city",
            "cibil_score", "loan_eligibility_tier", "income_segment",
            "employment_type", "bureau_enquiries_last_6m", "bureau_risk_flag",
            "is_fraud_profile",
            # Loan metrics
            "total_loans", "fraud_loans", "defaulted_loans", "npa_loans", "rejected_loans",
            "total_loan_amount", "avg_ltv_ratio", "overpriced_loan_count",
            # Payment metrics
            "total_emi_payments", "bounced_payments", "defaulted_payments", "late_payments",
            "max_days_past_due", "total_payment_shortfall",
            # KYC metrics
            "total_kyc_attempts", "failed_kyc_attempts", "has_suspicious_kyc", "has_document_fraud",
            # Risk scores
            "default_rate", "bounce_rate", "kyc_failure_rate",
            "fraud_risk_score", "fraud_risk_tier",
        )
    )
