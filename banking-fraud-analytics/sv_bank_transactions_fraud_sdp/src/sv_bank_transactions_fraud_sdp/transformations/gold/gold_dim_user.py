"""
Gold: dim_user — SCD Type 2 user dimension.

Tracks full history of changes to credit profile, KYC status,
account balance segment, fraud link status, and account type.

Source: silver_users (silver schema)
Pattern: dp.create_streaming_table + dp.create_auto_cdc_flow (SCD Type 2)

Current row query: WHERE __END_AT IS NULL
Historical query:  WHERE __START_AT <= '<date>' AND (__END_AT IS NULL OR __END_AT > '<date>')
"""
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema   = spark.conf.get("gold_schema")

dp.create_streaming_table(
    f"{gold_schema}.dim_user",
    comment=(
        "SCD Type 2 user dimension. Full history of credit score, KYC status, "
        "account balance, account type, and fraud linkage changes. "
        "Query WHERE __END_AT IS NULL for current state."
    ),
    cluster_by=["user_id"],
    table_properties={
        "layer": "gold",
        "domain": "users",
        "scd_type": "2",
        "grain": "user_id",
    },
)

dp.create_auto_cdc_flow(
    target=f"{gold_schema}.dim_user",
    source=f"{silver_schema}.silver_users",
    keys=["user_id"],
    sequence_by="updated_timestamp",
    stored_as_scd_type=2,
    track_history_column_list=[
        "credit_score",
        "credit_tier",
        "kyc_status",
        "account_balance",
        "balance_segment",
        "account_type",
        "is_fraud_linked",
        "city",
        "state",
    ],
)
