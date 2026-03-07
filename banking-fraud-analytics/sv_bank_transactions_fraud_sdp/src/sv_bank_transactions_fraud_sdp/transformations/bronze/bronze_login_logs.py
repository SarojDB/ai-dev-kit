"""
Bronze: Raw login log records — append-only, schema evolution enabled.

Source: /Volumes/financial_security/raw_data/mock_banking_data/login_logs/
Schema (inferred from data):
  online_login_id, user_id, device_id, ip_address,
  login_status, failed_attempt_num (long), mfa_change_flag (boolean),
  session_duration_sec (long), city, country_code,
  is_vpn (boolean), is_tor (boolean), is_new_device (boolean),
  is_suspicious (boolean),
  event_timestamp, created_timestamp, updated_timestamp
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment="Raw login log records with IP address, device, MFA, and session metadata. Append-only.",
    cluster_by=["online_login_id"],
    table_properties={
        "layer": "bronze",
        "source": "json_volume",
        "domain": "login_logs",
    },
)
def bronze_login_logs():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_login_logs")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/login_logs/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
