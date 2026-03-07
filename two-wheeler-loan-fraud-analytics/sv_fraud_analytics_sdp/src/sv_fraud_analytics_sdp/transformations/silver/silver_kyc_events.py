"""
Silver layer: clean and enrich KYC verification event data.

Business rules applied:
- Drop records missing kyc_event_id or customer_id
- Cast event_timestamp, created_timestamp, updated_timestamp to TIMESTAMP
- Normalize kyc_status, kyc_method, document_type to uppercase
- Derive is_failed flag for quick filtering
- Derive is_suspicious: attempt_number > 2 AND kyc_status in (FAILED, PENDING)
    — multiple failures suggest identity fraud or document manipulation
- Derive verification_method_tier:
    DIGITAL   : Aadhaar_OTP or DigiLocker (lowest friction, moderate assurance)
    VIDEO     : Video_KYC (high assurance, remote)
    PHYSICAL  : Physical_KYC (highest assurance, in-person)
- Flag high-risk failure reasons:
    is_document_fraud: failure_reason in (SUSPICIOUS_DOCUMENT, DUPLICATE_IDENTITY, FACE_MATCH_FAILED)
- Drop _rescued_data
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")

_DOCUMENT_FRAUD_REASONS = (
    "SUSPICIOUS_DOCUMENT",
    "DUPLICATE_IDENTITY",
    "FACE_MATCH_FAILED",
)


@dp.table(
    name=f"{silver_schema}.silver_kyc_events",
    comment=(
        "Cleaned KYC verification audit events with typed timestamps, normalized categoricals, "
        "and fraud-indicative flags. Multiple failures flagged as suspicious. "
        "Source: bronze_kyc_events streaming table."
    ),
    cluster_by=["kyc_event_id"],
    table_properties={"layer": "silver", "domain": "kyc"},
)
def silver_kyc_events():
    return (
        spark.readStream.table("bronze_kyc_events")
        # Hard filters
        .filter(F.col("kyc_event_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        # Cast timestamps
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Normalize categoricals to uppercase
        .withColumn("kyc_status", F.upper(F.trim(F.col("kyc_status"))))
        .withColumn("kyc_method", F.upper(F.trim(F.col("kyc_method"))))
        .withColumn("document_type", F.upper(F.trim(F.col("document_type"))))
        .withColumn("failure_reason", F.upper(F.trim(F.col("failure_reason"))))
        .withColumn("device_type", F.upper(F.trim(F.col("device_type"))))
        # Quick failure flag
        .withColumn("is_failed", F.col("kyc_status") == "FAILED")
        # Suspicious pattern: multiple failed attempts suggest identity fraud
        .withColumn(
            "is_suspicious",
            (F.col("attempt_number") > 2) &
            F.col("kyc_status").isin("FAILED", "PENDING")
        )
        # Verification method tier (assurance level)
        .withColumn(
            "verification_method_tier",
            F.when(F.col("kyc_method") == "PHYSICAL_KYC", F.lit("PHYSICAL"))
             .when(F.col("kyc_method") == "VIDEO_KYC", F.lit("VIDEO"))
             .when(
                 F.col("kyc_method").isin("AADHAAR_OTP", "DIGILOCKER"),
                 F.lit("DIGITAL")
             )
             .otherwise(F.lit("UNKNOWN"))
        )
        # High-risk failure reason flag
        .withColumn(
            "is_document_fraud",
            F.col("failure_reason").isin(*_DOCUMENT_FRAUD_REASONS)
        )
        # Derive event date for time-series
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_rescued_data")
    )
