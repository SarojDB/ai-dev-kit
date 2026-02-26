"""
Gold layer: fact_order — SCD Type 1 order fact table.

Uses AUTO CDC with SCD Type 1 (in-place update) so that order status
changes (pending → completed → refunded) are reflected without duplicate rows.
One row per order_id, always reflecting the latest state.

Suitable for: revenue reporting, order fulfillment tracking,
cohort revenue analysis, subscription renewal analysis.
"""
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

dp.create_streaming_table(
    f"{gold_schema}.fact_order",
    comment=(
        "SCD Type 1 order fact table. One row per order_id at current state. "
        "Status updates (e.g., completed → refunded) are applied in-place."
    ),
    cluster_by=["order_id"],
    table_properties={"layer": "gold", "domain": "orders", "scd_type": "1"},
)

dp.create_auto_cdc_flow(
    target=f"{gold_schema}.fact_order",
    source=f"{silver_schema}.silver_orders",
    keys=["order_id"],
    sequence_by="updated_timestamp",
    stored_as_scd_type="1",  # string = SCD Type 1 (in-place upsert)
)
