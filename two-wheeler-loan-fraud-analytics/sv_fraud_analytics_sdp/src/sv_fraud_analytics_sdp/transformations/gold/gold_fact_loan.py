"""
Gold layer: fact_loan — SCD Type 1 loan fact table.

Uses AUTO CDC (SCD Type 1) to maintain a deduplicated, current-state loan record.
Each loan_id has exactly one row reflecting its latest known state.
Suitable for:
- Current portfolio reporting: active loans, delinquencies, NPA tracking
- Fraud investigation: current status of flagged loans
- EMI join: join with fact_emi_payments for payment history

SCD Type 1 behavior:
- No history preserved — overwrites on update
- Ideal for loan_status changes (Active → Defaulted → NPA)
- Use silver_loans for full streaming history if needed

Query all active loans: WHERE loan_status = 'ACTIVE'
Query fraud loans: WHERE is_fraud = TRUE
"""
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

dp.create_streaming_table(
    f"{gold_schema}.fact_loan",
    comment=(
        "SCD Type 1 loan fact table. One row per loan_id with current status. "
        "Tracks 5,000 loans over 12-month window; 8% fraud rate. "
        "Join to dim_customer and dim_product for full analytical model. "
        "Source: silver_loans streaming table."
    ),
    cluster_by=["loan_id"],
    table_properties={"layer": "gold", "domain": "loans", "scd_type": "1"},
)

dp.create_auto_cdc_flow(
    target=f"{gold_schema}.fact_loan",
    source=f"{silver_schema}.silver_loans",
    keys=["loan_id"],
    sequence_by="updated_timestamp",
    stored_as_scd_type="1",  # String "1" = SCD Type 1 (in-place updates, no history)
)
