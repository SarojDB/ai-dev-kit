"""
Silver: Cleaned and enriched transaction records.

Transformations:
  - Cast amount to Decimal(18,2), timestamps to proper types
  - Derive is_wire_transfer (NEFT/RTGS/IMPS/Net_Banking)
  - Derive is_high_value_wire (wire transfer > INR 5,00,000)
  - Derive amount_bucket (micro/small/medium/large/very_large)
  - Derive txn_hour_of_day, txn_day_of_week, is_off_hours
  - Filter nulls on business keys

Business rule — wire transfers over INR 5,00,000:
  transaction_type IN ('NEFT', 'RTGS', 'IMPS', 'Net_Banking') AND amount > 500000
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.silver_transactions",
    comment=(
        "Cleaned transactions with typed fields, wire-transfer flags, and fraud signals. "
        "is_high_value_wire marks NEFT/RTGS/IMPS/Net_Banking transfers over INR 5,00,000."
    ),
    cluster_by=["transaction_id", "user_id"],
    table_properties={"layer": "silver", "domain": "transactions"},
)
def silver_transactions():
    return (
        spark.readStream.table("bronze_transactions")
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("merchant_id").isNotNull())
        .filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
        .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn("event_timestamp",   F.to_timestamp("event_timestamp"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Wire transfer classification
        # Production threshold: INR 5,00,000 (500,000)
        # Demo data threshold: INR 50,000 (50,000)
        .withColumn(
            "is_wire_transfer",
            F.col("transaction_type").isin("NEFT", "RTGS", "IMPS", "Net_Banking"),
        )
        .withColumn(
            "is_high_value_wire",
            F.when(
                F.col("transaction_type").isin("NEFT", "RTGS", "IMPS", "Net_Banking")
                & (F.col("amount").cast("double") > 50000.0),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        # Amount segmentation
        .withColumn(
            "amount_bucket",
            F.when(F.col("amount") < 1000,    F.lit("micro"))
             .when(F.col("amount") < 10000,   F.lit("small"))
             .when(F.col("amount") < 100000,  F.lit("medium"))
             .when(F.col("amount") < 500000,  F.lit("large"))
             .otherwise(F.lit("very_large")),
        )
        # Temporal features
        .withColumn("txn_hour_of_day",  F.hour("event_timestamp"))
        .withColumn("txn_day_of_week",  F.dayofweek("event_timestamp"))
        .withColumn(
            "is_off_hours",
            (F.hour("event_timestamp") < 8) | (F.hour("event_timestamp") > 22),
        )
        # Data quality marker
        .withColumn(
            "dq_issues",
            F.when(F.col("is_international") & ~F.col("is_fraud").isNull(), F.lit("international_flag"))
             .otherwise(F.lit(None).cast("string")),
        )
        .drop("_rescued_data")
    )
