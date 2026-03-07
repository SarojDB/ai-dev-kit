"""
Bronze layer: raw EMI payment event data ingestion from JSON volume.

Streaming table using Auto Loader (cloudFiles) in JSON format.
~19,000 payment records across all active/closed loans.
Each record represents one EMI installment event.

Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/emi_payments/
Fields of note:
  payment_id, loan_id, customer_id, emi_number, due_date, payment_date,
  emi_amount, actual_amount_paid, penalty_amount, payment_status
  (Paid/Late/Bounced/Defaulted), payment_mode, days_past_due,
  event_timestamp, created_timestamp, updated_timestamp
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment=(
        "Raw EMI payment event records ingested from JSON files. "
        "~19,000 installment events; fraud loans show early delinquency (months 1-3). "
        "Payment modes: NACH, UPI, Net_Banking, Cash, Cheque. "
        "Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/emi_payments/"
    ),
    cluster_by=["payment_id"],
    table_properties={"layer": "bronze", "source": "json_volume", "domain": "payments"},
)
def bronze_emi_payments():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_emi_payments")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/emi_payments/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
