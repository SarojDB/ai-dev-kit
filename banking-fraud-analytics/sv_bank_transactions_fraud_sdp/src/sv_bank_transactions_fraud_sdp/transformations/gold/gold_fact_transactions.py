"""
Gold: fact_transactions — SCD Type 1 transaction fact table.

Star schema fact table joining to dim_user and dim_merchant dimensions.
SCD Type 1: in-place updates, no history (transactions are corrected/reversed
in-place; history is in the silver layer).

Source: silver_transactions (silver schema)
Pattern: dp.create_streaming_table + dp.create_auto_cdc_flow (SCD Type 1)
"""
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema   = spark.conf.get("gold_schema")

dp.create_streaming_table(
    f"{gold_schema}.fact_transactions",
    comment=(
        "SCD Type 1 transaction fact table. Current state of each transaction "
        "with wire-transfer flag, fraud label, and enriched temporal features. "
        "Join to dim_user and dim_merchant on user_id / merchant_id."
    ),
    cluster_by=["transaction_id", "user_id"],
    table_properties={
        "layer": "gold",
        "domain": "transactions",
        "scd_type": "1",
        "grain": "transaction_id",
    },
)

dp.create_auto_cdc_flow(
    target=f"{gold_schema}.fact_transactions",
    source=f"{silver_schema}.silver_transactions",
    keys=["transaction_id"],
    sequence_by="updated_timestamp",
    stored_as_scd_type="1",
)
