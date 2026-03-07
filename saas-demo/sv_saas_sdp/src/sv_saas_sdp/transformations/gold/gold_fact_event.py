"""
Gold layer: fact_event — SCD Type 1 event fact table.

Uses AUTO CDC with SCD Type 1 for deduplication by event_id.
Events are immutable in SaaS systems, so this is effectively
event deduplication: if the same event_id arrives twice (replay),
the later record wins (based on event_timestamp).

Suitable for: funnel analysis, DAU/WAU/MAU metrics, feature adoption
tracking, error rate monitoring, session analysis.
"""
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

dp.create_streaming_table(
    f"{gold_schema}.fact_event",
    comment=(
        "SCD Type 1 event fact table. Deduplicated by event_id. "
        "Supports funnel analysis, DAU/WAU metrics, and error monitoring."
    ),
    cluster_by=["event_id"],
    table_properties={"layer": "gold", "domain": "events", "scd_type": "1"},
)

dp.create_auto_cdc_flow(
    target=f"{gold_schema}.fact_event",
    source=f"{silver_schema}.silver_events",
    keys=["event_id"],
    sequence_by="event_timestamp",
    stored_as_scd_type="1",  # string = SCD Type 1
)
