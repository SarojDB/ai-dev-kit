"""
Bronze layer: raw KYC verification audit event data ingestion from JSON volume.

Streaming table using Auto Loader (cloudFiles) in JSON format.
~15,600 KYC events; fraud profiles show multiple failed attempts before passing.

Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/kyc_events/
Fields of note:
  kyc_event_id, customer_id, attempt_number, kyc_method, kyc_status
  (Passed/Failed/Pending/Expired), failure_reason, document_type,
  is_video_verified, agent_id, ip_address, device_type,
  event_timestamp, created_timestamp, updated_timestamp
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment=(
        "Raw KYC verification audit events ingested from JSON files. "
        "~15,600 events across Aadhaar OTP, Video KYC, Physical KYC, DigiLocker. "
        "Fraud profiles show repeated failures and suspicious document patterns. "
        "Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/kyc_events/"
    ),
    cluster_by=["kyc_event_id"],
    table_properties={"layer": "bronze", "source": "json_volume", "domain": "kyc"},
)
def bronze_kyc_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_kyc_events")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/kyc_events/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
