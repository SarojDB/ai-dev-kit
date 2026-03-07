"""
Gold layer: dim_product — SCD Type 2 product dimension.

Uses AUTO CDC to track 2-wheeler product catalog changes over time.
A new row is created when price, segment, or key attributes change, enabling:
- Historical pricing: "what was the on-road price of Splendor Plus in Oct 2025?"
- LTV recalculation: join with historical prices for accurate LTV analysis
- Insurance change tracking: monitor premium adjustments over time

History columns added by SDP:
  __START_AT  — when this version became effective
  __END_AT    — when this version expired (NULL = current/active record)

Query current catalog: WHERE __END_AT IS NULL
"""
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

dp.create_streaming_table(
    f"{gold_schema}.dim_product",
    comment=(
        "SCD Type 2 product dimension. Tracks price changes, insurance updates, "
        "and segment reclassifications for 2-wheeler products. "
        "Use WHERE __END_AT IS NULL to get current product catalog. "
        "Source: silver_products streaming table."
    ),
    cluster_by=["product_id"],
    table_properties={"layer": "gold", "domain": "products", "scd_type": "2"},
)

dp.create_auto_cdc_flow(
    target=f"{gold_schema}.dim_product",
    source=f"{silver_schema}.silver_products",
    keys=["product_id"],
    sequence_by="updated_timestamp",
    stored_as_scd_type=2,
    # Track history for pricing and classification changes
    track_history_column_list=[
        "ex_showroom_price",
        "on_road_price_min",
        "on_road_price_max",
        "price_segment",
        "max_ltv_pct",
        "on_road_markup_pct",
        "insurance_premium_annual",
        "insurance_type",
        "insurance_partner",
        "is_high_value",
    ],
)
