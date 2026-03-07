"""
Bronze layer: raw dealer data ingestion from JSON volume.

Streaming table using Auto Loader (cloudFiles) in JSON format.
Covers authorized and flagged dealers across North/South/East/West regions of India.

Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/dealers/
Fields of note:
  dealer_id, dealer_name, brand, city, state, region, pincode,
  is_authorized, dealer_tier (Tier1/Tier2/Tier3), is_flagged
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment=(
        "Raw dealer records ingested from JSON files. "
        "200 dealers across North, South, East, West regions of India. "
        "Includes is_flagged field for fraud-associated dealer detection. "
        "Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/dealers/"
    ),
    cluster_by=["dealer_id"],
    table_properties={"layer": "bronze", "source": "json_volume", "domain": "dealers"},
)
def bronze_dealers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_dealers")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/dealers/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
