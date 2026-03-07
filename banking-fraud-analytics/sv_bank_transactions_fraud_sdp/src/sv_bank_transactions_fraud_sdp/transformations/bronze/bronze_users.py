"""
Bronze: Raw user/customer profile records — append-only, schema evolution enabled.

Source: /Volumes/financial_security/raw_data/mock_banking_data/users/
Schema (inferred from data):
  user_id, full_name, email, mobile, city, state, region,
  account_type, account_balance (long), kyc_status,
  credit_score (long), is_new_account (boolean),
  is_fraud_linked (boolean), registration_date,
  created_timestamp, updated_timestamp
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment="Raw bank customer profile records. Append-only with schema evolution.",
    cluster_by=["user_id"],
    table_properties={
        "layer": "bronze",
        "source": "json_volume",
        "domain": "users",
    },
)
def bronze_users():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_users")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/users/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
