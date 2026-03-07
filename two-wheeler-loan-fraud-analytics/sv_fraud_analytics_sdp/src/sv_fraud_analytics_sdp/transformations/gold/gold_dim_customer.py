"""
Gold layer: dim_customer — SCD Type 2 customer dimension.

Uses AUTO CDC to track all analytically significant attribute changes over time.
A new row is created whenever a tracked attribute changes, enabling:
- Point-in-time queries: "what was customer X's CIBIL tier when they applied for loan Y?"
- Change analysis: "how many customers moved from SUBPRIME to NEAR_PRIME?"
- Fraud investigation: "what was the customer's profile at time of application?"

History columns added by SDP:
  __START_AT  — when this version became effective (based on updated_timestamp)
  __END_AT    — when this version expired (NULL = current/active record)

Query current customers: WHERE __END_AT IS NULL
Track CIBIL changes: GROUP BY customer_id, loan_eligibility_tier, __START_AT
"""
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

dp.create_streaming_table(
    f"{gold_schema}.dim_customer",
    comment=(
        "SCD Type 2 customer dimension. Full history of credit profile, "
        "KYC status, and risk tier changes. "
        "Use WHERE __END_AT IS NULL to get current customer state. "
        "Source: silver_customers streaming table."
    ),
    cluster_by=["customer_id"],
    table_properties={"layer": "gold", "domain": "customers", "scd_type": "2"},
)

dp.create_auto_cdc_flow(
    target=f"{gold_schema}.dim_customer",
    source=f"{silver_schema}.silver_customers",
    keys=["customer_id"],
    sequence_by="updated_timestamp",
    stored_as_scd_type=2,
    # Track history for analytically important fields that change over time
    track_history_column_list=[
        "cibil_score",
        "loan_eligibility_tier",
        "income_segment",
        "monthly_income",
        "employment_type",
        "employer_sector",
        "kyc_status",
        "kyc_method",
        "existing_loan_count",
        "bureau_enquiries_last_6m",
        "bureau_risk_flag",
        "address_type",
        "city",
        "state",
        "region",
        "is_fraud_profile",
    ],
)
