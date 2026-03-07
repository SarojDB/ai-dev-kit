"""
Bronze layer: raw customer data ingestion from JSON volume.

Streaming table using Auto Loader (cloudFiles) in JSON format.
Schema evolution enabled to accommodate future field additions without full refresh.
All raw fields preserved; metadata columns added for lineage.

Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/customers/
Fields of note (all ingested as-is):
  customer_id, full_name, gender, date_of_birth, age, pan_number, aadhaar_last4,
  mobile_number, email, cibil_score, monthly_income, employment_type, region,
  kyc_status, is_fraud_profile (ML training label), created_timestamp, updated_timestamp
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment=(
        "Raw customer records ingested from fraud analytics landing zone JSON files. "
        "Append-only. Schema evolution enabled via Auto Loader. "
        "Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/customers/"
    ),
    cluster_by=["customer_id"],
    table_properties={"layer": "bronze", "source": "json_volume", "domain": "customers"},
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_customers")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/customers/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
