"""
Gold layer: agg_loan_performance_by_region — Regional loan portfolio performance.

Materialized view joining loans and dealers to produce regional portfolio metrics.
Designed for risk management, regional operations, and fraud hotspot analysis.

Grain: region × dealer_tier × brand × product_type × application_month

Key metrics:
  total_applications    - total loan applications received
  total_disbursed_count - applications resulting in disbursement (not Rejected)
  total_disbursed_amount- aggregate disbursed loan portfolio value (INR)
  avg_loan_amount       - average loan size (INR)
  avg_interest_rate_pct - average interest rate charged
  avg_ltv_ratio         - average loan-to-value ratio
  total_defaults        - count of Defaulted loans
  total_npa             - count of NPA (Non-Performing Asset) loans
  fraud_count           - confirmed fraud loan count (ML label)
  rejection_count       - rejected applications
  default_rate          - defaulted / total_disbursed_count
  npa_rate              - npa / total_disbursed_count
  fraud_rate            - fraud_count / total_disbursed_count
  rejection_rate        - rejection_count / total_applications
  avg_turnaround_days   - average application-to-disbursement days
  overpriced_count      - loans where loan_amount > on_road_price (fraud signal)

Refreshed on every pipeline run.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.agg_loan_performance_by_region",
    comment=(
        "Regional loan portfolio performance metrics. "
        "Grain: region × dealer_tier × brand × product_type × application_month. "
        "Key fraud detection views: fraud_rate, overpriced_count, npa_rate per region/dealer tier. "
        "Refreshed on every pipeline run."
    ),
    cluster_by=["region", "application_month"],
    table_properties={"layer": "gold", "domain": "loan_performance", "grain": "regional_monthly"},
)
def agg_loan_performance_by_region():
    loans = spark.read.table(f"{silver_schema}.silver_loans")
    dealers = spark.read.table(f"{silver_schema}.silver_dealers")

    enriched = loans.join(
        dealers.select("dealer_id", "region", "dealer_tier", "dealer_risk_level", "is_flagged", "is_authorized"),
        "dealer_id",
        "left"
    )

    return (
        enriched
        .groupBy("region", "dealer_tier", "dealer_risk_level", "brand", "product_type", "application_month")
        .agg(
            # Application volume
            F.count("loan_id").alias("total_applications"),
            F.sum(F.when(F.col("loan_status") != "REJECTED", 1).otherwise(0))
             .alias("total_disbursed_count"),
            F.sum(F.when(F.col("loan_status") == "REJECTED", 1).otherwise(0))
             .alias("rejection_count"),
            # Portfolio value
            F.sum(
                F.when(F.col("loan_status") != "REJECTED", F.col("loan_amount"))
                 .otherwise(F.lit(0))
            ).alias("total_disbursed_amount"),
            F.avg(
                F.when(F.col("loan_status") != "REJECTED", F.col("loan_amount"))
            ).alias("avg_loan_amount"),
            # Risk metrics
            F.avg("interest_rate_pct").alias("avg_interest_rate_pct"),
            F.avg("ltv_ratio").alias("avg_ltv_ratio"),
            F.avg(F.col("loan_tenure_months").cast("double")).alias("avg_tenure_months"),
            # Delinquency and loss metrics
            F.sum(F.when(F.col("loan_status") == "DEFAULTED", 1).otherwise(0))
             .alias("total_defaults"),
            F.sum(F.when(F.col("loan_status") == "NPA", 1).otherwise(0))
             .alias("total_npa"),
            F.sum(F.when(F.col("loan_status") == "ACTIVE", 1).otherwise(0))
             .alias("active_count"),
            # Fraud signals
            F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0))
             .alias("fraud_count"),
            F.sum(F.when(F.col("is_overpriced") == True, 1).otherwise(0))
             .alias("overpriced_count"),
            F.sum(F.when(F.col("is_quick_disburse") == True, 1).otherwise(0))
             .alias("quick_disburse_count"),
            # Operational metrics
            F.avg("application_to_disbursement_days").alias("avg_turnaround_days"),
            F.sum(F.when(F.col("application_channel") == "DEALER", 1).otherwise(0))
             .alias("dealer_sourced_count"),
            F.sum(F.when(F.col("application_channel") == "ONLINE", 1).otherwise(0))
             .alias("online_sourced_count"),
        )
        .withColumn(
            "default_rate",
            F.round(
                F.when(F.col("total_disbursed_count") > 0,
                    F.col("total_defaults").cast("double") / F.col("total_disbursed_count").cast("double")
                ).otherwise(F.lit(0.0)),
                4
            )
        )
        .withColumn(
            "npa_rate",
            F.round(
                F.when(F.col("total_disbursed_count") > 0,
                    F.col("total_npa").cast("double") / F.col("total_disbursed_count").cast("double")
                ).otherwise(F.lit(0.0)),
                4
            )
        )
        .withColumn(
            "fraud_rate",
            F.round(
                F.when(F.col("total_disbursed_count") > 0,
                    F.col("fraud_count").cast("double") / F.col("total_disbursed_count").cast("double")
                ).otherwise(F.lit(0.0)),
                4
            )
        )
        .withColumn(
            "rejection_rate",
            F.round(
                F.when(F.col("total_applications") > 0,
                    F.col("rejection_count").cast("double") / F.col("total_applications").cast("double")
                ).otherwise(F.lit(0.0)),
                4
            )
        )
        # Portfolio risk tier for the segment
        .withColumn(
            "portfolio_risk_tier",
            F.when(
                (F.col("fraud_rate") > 0.10) | (F.col("default_rate") > 0.15),
                F.lit("HIGH")
            ).when(
                (F.col("fraud_rate") > 0.05) | (F.col("default_rate") > 0.08),
                F.lit("MEDIUM")
            ).otherwise(F.lit("LOW"))
        )
    )
