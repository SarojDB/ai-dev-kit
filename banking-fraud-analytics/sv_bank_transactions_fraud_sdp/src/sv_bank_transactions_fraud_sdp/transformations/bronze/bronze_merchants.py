"""
Bronze: Raw merchant reference data — append-only, schema evolution enabled.

Source: /Volumes/financial_security/raw_data/mock_banking_data/merchants/
Schema (inferred from data):
  merchant_id, merchant_name, merchant_category,
  city, state, region, is_high_risk (boolean),
  created_timestamp, updated_timestamp
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment="Raw merchant reference records. Append-only with schema evolution.",
    cluster_by=["merchant_id"],
    table_properties={
        "layer": "bronze",
        "source": "json_volume",
        "domain": "merchants",
    },
)
def bronze_merchants():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_merchants")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/merchants/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
