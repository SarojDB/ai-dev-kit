"""
Bronze layer: raw 2-wheeler product catalog ingestion from JSON volume.

Streaming table using Auto Loader (cloudFiles) in JSON format.
Schema evolution enabled for future product attribute additions.

Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/products/
Fields of note:
  product_id, brand, model, product_type, fuel_type, engine_cc,
  ex_showroom_price, on_road_price_min, on_road_price_max,
  is_electric, insurance_type, insurance_premium_annual, insurance_partner
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment=(
        "Raw 2-wheeler product catalog ingested from JSON files. "
        "Covers Hero MotoCorp, Honda, TVS, Bajaj, Yamaha, Royal Enfield, Ather, Ola, Revolt. "
        "Source: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/products/"
    ),
    cluster_by=["product_id"],
    table_properties={"layer": "bronze", "source": "json_volume", "domain": "products"},
)
def bronze_products():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_products")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{raw_data_path}/products/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
