"""
Gold: dim_merchant — SCD Type 2 merchant dimension.

Tracks full history of changes to merchant risk classification,
category reassignments, and geographic coverage.

Source: silver_merchants (silver schema)
Pattern: dp.create_streaming_table + dp.create_auto_cdc_flow (SCD Type 2)

Current row query: WHERE __END_AT IS NULL
"""
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema   = spark.conf.get("gold_schema")

dp.create_streaming_table(
    f"{gold_schema}.dim_merchant",
    comment=(
        "SCD Type 2 merchant dimension. Full history of risk tier, category, "
        "and geographic changes. Query WHERE __END_AT IS NULL for current state."
    ),
    cluster_by=["merchant_id"],
    table_properties={
        "layer": "gold",
        "domain": "merchants",
        "scd_type": "2",
        "grain": "merchant_id",
    },
)

dp.create_auto_cdc_flow(
    target=f"{gold_schema}.dim_merchant",
    source=f"{silver_schema}.silver_merchants",
    keys=["merchant_id"],
    sequence_by="updated_timestamp",
    stored_as_scd_type=2,
    track_history_column_list=[
        "merchant_category",
        "merchant_risk_tier",
        "is_high_risk",
        "city",
        "state",
        "region",
    ],
)
