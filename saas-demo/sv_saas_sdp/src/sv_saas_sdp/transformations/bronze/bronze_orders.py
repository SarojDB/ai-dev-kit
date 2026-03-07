"""
Bronze layer: raw order data ingestion from Parquet volume.

Streaming table using Auto Loader (cloudFiles) in parquet format.
Schema is inferred from Parquet metadata; schema evolution enabled
to accommodate future field additions without full refresh.
All raw fields preserved; metadata columns added for lineage.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

schema_location_base = spark.conf.get("schema_location_base")
raw_data_path = spark.conf.get("raw_data_path")


@dp.table(
    comment=(
        "Raw order records ingested from SaaS landing zone Parquet files. "
        "Append-only. Schema evolution enabled via Auto Loader. "
        "Source: /Volumes/aibuilder_saas_demo/raw_data/saas_data/orders/"
    ),
    cluster_by=["order_id"],
    table_properties={"layer": "bronze", "source": "parquet_volume"},
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_orders")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/orders/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
