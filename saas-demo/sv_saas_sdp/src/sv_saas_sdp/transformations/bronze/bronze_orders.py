"""
Bronze layer: raw order data ingestion.

Streaming table using Auto Loader (cloudFiles) with schema evolution.
All raw fields preserved; metadata columns added for lineage.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment=(
        "Raw order records ingested from SaaS landing zone JSON files. "
        "Append-only. Schema evolution enabled via Auto Loader."
    ),
    cluster_by=["order_id"],
    table_properties={"layer": "bronze", "source": "json_files"},
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_orders")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("pathGlobFilter", "*.json")
        .load(f"{raw_data_path}/orders/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
