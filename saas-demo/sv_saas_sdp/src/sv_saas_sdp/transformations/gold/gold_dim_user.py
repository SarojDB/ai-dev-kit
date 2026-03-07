"""
Gold layer: dim_user — SCD Type 2 user dimension.

Uses AUTO CDC to track all changes to user attributes over time.
A new row is created whenever a tracked attribute changes, enabling
point-in-time queries (e.g., "what tier was user X in Q2?").

History columns added by SDP:
  __START_AT  - when this version became effective (based on updated_timestamp)
  __END_AT    - when this version expired (NULL = current record)

Query current users: WHERE __END_AT IS NULL
"""
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

# Create the target streaming table that AUTO CDC will populate
dp.create_streaming_table(
    f"{gold_schema}.dim_user",
    comment=(
        "SCD Type 2 user dimension. Full history of user attribute changes. "
        "Use WHERE __END_AT IS NULL to get current user state."
    ),
    cluster_by=["user_id"],
    table_properties={"layer": "gold", "domain": "users", "scd_type": "2"},
)

# Apply AUTO CDC: track all user attribute changes over time
dp.create_auto_cdc_flow(
    target=f"{gold_schema}.dim_user",
    source=f"{silver_schema}.silver_users",
    keys=["user_id"],
    sequence_by="updated_timestamp",
    stored_as_scd_type=2,  # integer = SCD Type 2 (history tracking)
    # Track history only for business-significant attributes
    # subscription_tier and product_type changes are analytically important
    track_history_column_list=[
        "subscription_tier",
        "product_type",
        "is_active",
        "is_enterprise",
        "is_valid_email",
        "email",
        "company",
        "country",
        "city",
    ],
)
