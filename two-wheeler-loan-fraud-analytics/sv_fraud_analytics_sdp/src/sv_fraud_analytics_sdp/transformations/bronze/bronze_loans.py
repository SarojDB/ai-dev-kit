"""
Bronze layer: raw loan application data ingestion from JSON volume.

Streaming table using Auto Loader (cloudFiles) in JSON format.
5,000 loan applications over a 12-month period (Mar 2025 - Feb 2026).
Loan amounts between INR 50,000 and INR 2,00,000.

Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/loans/
Fields of note:
  loan_id, customer_id, product_id, dealer_id, brand, product_type,
  loan_amount, down_payment, on_road_price, loan_tenure_months,
  interest_rate_pct, emi_amount, application_channel, loan_status,
  application_date, disbursement_date, first_emi_date,
  is_fraud, fraud_type (ML training labels), event_timestamp,
  created_timestamp, updated_timestamp
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment=(
        "Raw loan application records ingested from JSON files. "
        "5,000 loans; 8% fraud rate across identity theft, income falsification, "
        "dealer collusion, duplicate applications, asset inflation. "
        "Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/loans/"
    ),
    cluster_by=["loan_id"],
    table_properties={"layer": "bronze", "source": "json_volume", "domain": "loans"},
)
def bronze_loans():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_loans")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/loans/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
