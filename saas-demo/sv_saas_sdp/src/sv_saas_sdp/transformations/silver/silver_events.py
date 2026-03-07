"""
Silver layer: clean and enrich event data.

Business rules applied:
- Drop records missing event_id or user_id (cannot be attributed)
- Drop records with null event_timestamp (cannot be ordered for CDC)
- Cast timestamps from string to TIMESTAMP
- Standardize event_type to lowercase/trimmed
- Recalculate is_error_event: event_type == 'error' OR http_status_code >= 400
- Recalculate is_high_latency: latency_ms > 1000 (1-second SLO threshold)
- Re-derive event_category aligned to actual SaaS event taxonomy
- Add performance_tier: 'fast'(<200ms), 'normal'(<1s), 'slow'(>=1s)
- Derive event_date and event_hour for time-partitioned aggregations

Event taxonomy (matches source data):
  navigation : page_view
  engagement : click, search, export, feature_use, upload, download,
               settings_change, share
  api        : api_call
  auth       : login, logout, session_start, session_end
  error      : error
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

silver_schema = spark.conf.get("silver_schema")

_NAV_TYPES    = ("page_view",)
_ENGAGE_TYPES = ("click", "search", "export", "feature_use", "upload",
                 "download", "settings_change", "share")
_API_TYPES    = ("api_call",)
_AUTH_TYPES   = ("login", "logout", "session_start", "session_end")
_ERROR_TYPES  = ("error",)


@dp.table(
    name=f"{silver_schema}.silver_events",
    comment=(
        "Cleaned and enriched event records. "
        "Error/latency flags recalculated from source signals. "
        "Event categories aligned to SaaS taxonomy. "
        "Source: bronze_events streaming table."
    ),
    cluster_by=["event_id"],
    table_properties={"layer": "silver", "domain": "events"},
)
def silver_events():
    return (
        spark.readStream.table("bronze_events")
        # Hard filters — records without these cannot be processed
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        # Cast timestamps
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("updated_timestamp", F.to_timestamp("updated_timestamp"))
        # Normalize event_type to lowercase, trimmed
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        # Recalculate event_category aligned to SaaS taxonomy
        .withColumn(
            "event_category",
            F.when(F.col("event_type").isin(*_NAV_TYPES),    F.lit("navigation"))
             .when(F.col("event_type").isin(*_ENGAGE_TYPES), F.lit("engagement"))
             .when(F.col("event_type").isin(*_API_TYPES),    F.lit("api"))
             .when(F.col("event_type").isin(*_AUTH_TYPES),   F.lit("auth"))
             .when(F.col("event_type").isin(*_ERROR_TYPES),  F.lit("error"))
             .otherwise(F.lit("other"))
        )
        # Recalculate is_error_event: error event_type OR 4xx/5xx HTTP status
        .withColumn(
            "is_error_event",
            F.col("event_type").isin(*_ERROR_TYPES) |
            (F.col("http_status_code").isNotNull() & (F.col("http_status_code") >= 400))
        )
        # Recalculate is_high_latency: above 1-second SLO threshold
        .withColumn(
            "is_high_latency",
            F.col("latency_ms").isNotNull() & (F.col("latency_ms") > 1000)
        )
        # Performance tier for SLO analysis
        .withColumn(
            "performance_tier",
            F.when(F.col("latency_ms") < 200,  F.lit("fast"))
             .when(F.col("latency_ms") < 1000, F.lit("normal"))
             .otherwise(F.lit("slow"))
        )
        # Derive date parts for time-series aggregations
        .withColumn("event_date",  F.to_date("event_timestamp"))
        .withColumn("event_hour",  F.hour("event_timestamp"))
        .withColumn("event_month", F.date_format(F.col("event_timestamp"), "yyyy-MM"))
        .withColumn("_processed_at", F.current_timestamp())
    )
