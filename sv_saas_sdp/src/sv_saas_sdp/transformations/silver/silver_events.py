"""
Silver layer: clean and enrich event data.

Business rules applied:
- Drop records missing event_id or user_id
- Drop records with null event_timestamp (unorderable events)
- Cast timestamps to TIMESTAMP
- Classify events by category (navigation, api_call, feature_use, system_error)
- Flag error events and high-latency events (>1s)
- Derive event_date and hour_of_day for time-series partitioning
- Standardize event_type to lowercase
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")

# Event type classification map
NAV_EVENTS = ("page_view", "button_click", "form_submit", "search_query",
              "login", "logout")
API_EVENTS = ("api_call", "data_export", "webhook_trigger")
FEATURE_EVENTS = ("feature_used", "report_generated", "integration_sync",
                  "dashboard_created", "user_invited", "settings_changed",
                  "notification_sent")
ERROR_EVENTS = ("error_encountered",)


@dp.table(
    name=f"{silver_schema}.silver_events",
    comment=(
        "Cleaned and enriched event records. "
        "Events categorized, error/latency flags added, timestamps cast. "
        "Source: bronze_events streaming table."
    ),
    cluster_by=["event_id"],
    table_properties={"layer": "silver", "domain": "events"},
)
def silver_events():
    return (
        spark.readStream.table("bronze_events")
        # Hard filters
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        # Drop rescue column
        .drop("_rescued_data")
        # Cast timestamps
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        # Standardize event_type to lowercase
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        # Derived: event category for funnel analysis
        .withColumn(
            "event_category",
            F.when(F.col("event_type").isin(*NAV_EVENTS), "navigation")
             .when(F.col("event_type").isin(*API_EVENTS), "api_call")
             .when(F.col("event_type").isin(*FEATURE_EVENTS), "feature_use")
             .when(F.col("event_type").isin(*ERROR_EVENTS), "system_error")
             .otherwise("other")
        )
        # Derived: error flag (true for error events or non-2xx HTTP)
        .withColumn(
            "is_error_event",
            (F.col("event_type") == "error_encountered") |
            (F.col("http_status_code").isNotNull() & (F.col("http_status_code") >= 400))
        )
        # Derived: high latency flag (>1000ms = 1 second)
        .withColumn(
            "is_high_latency",
            F.col("latency_ms").isNotNull() & (F.col("latency_ms") > 1000)
        )
        # Derived: date parts for daily/hourly analysis
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        .withColumn("event_month", F.date_trunc("month", F.col("event_timestamp")))
        .withColumn("_processed_at", F.current_timestamp())
    )
