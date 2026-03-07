"""
Bronze: Raw transaction records — append-only, schema evolution enabled.

Source: /Volumes/financial_security/raw_data/mock_banking_data/transactions/
Schema (inferred from data):
  transaction_id, user_id, merchant_id, merchant_category,
  amount (double), currency, transaction_type, channel,
  transaction_status, txn_city, txn_state, txn_region,
  txn_lat (double), txn_lon (double), device_id, ip_address,
  is_international (boolean), is_fraud (boolean), fraud_type (string/null),
  event_timestamp, created_timestamp, updated_timestamp
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment="Raw transaction records ingested from mock banking JSON volume. Append-only. Schema evolution enabled.",
    cluster_by=["transaction_id"],
    table_properties={
        "layer": "bronze",
        "source": "json_volume",
        "domain": "transactions",
    },
)
def bronze_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_transactions")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/transactions/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
