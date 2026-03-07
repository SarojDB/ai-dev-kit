"""
Impossible Travel Detector — Databricks Connect + Spark Structured Streaming

Monitors silver_transactions (as a simulated location-stamped event stream)
and flags any user whose geo-location jumps > 500 miles in < 10 minutes.

Detection approach — zero Python UDFs, pure Spark SQL Column expressions:

  SIMULATE mode (--simulate):
    ┌─────────────────────────────────────────────────────────────────┐
    │  silver_transactions (static)                                   │
    │     ↓  Window(partitionBy user_id, orderBy event_timestamp)    │
    │     ↓  lag(lat, lon, ts) → prev location per user             │
    │     ↓  Haversine in Spark Column API (no UDFs)                 │
    │     ↓  Filter: dist > 500 miles AND time_gap < 10 min         │
    │     ↓  Write alerts → Delta + Lakebase                         │
    └─────────────────────────────────────────────────────────────────┘

  STREAM mode (default):
    ┌─────────────────────────────────────────────────────────────────┐
    │  silver_transactions (Delta CDF stream)                         │
    │     ↓  foreachBatch                                             │
    │     ↓  Each batch joined with Delta state table                 │
    │         (user_last_location: user_id → last lat/lon/ts)        │
    │     ↓  Haversine via Spark SQL                                  │
    │     ↓  Flag + write alerts → Delta + Lakebase                  │
    │     ↓  MERGE state table with new latest locations             │
    └─────────────────────────────────────────────────────────────────┘

Run locally:
    python3 scripts/impossible_travel_detector.py --simulate   # one-shot
    python3 scripts/impossible_travel_detector.py              # live stream
"""

from __future__ import annotations

import os
import uuid
import json
import logging
import configparser
from datetime import datetime

import psycopg
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
CATALOG         = "financial_security"
SILVER_SCHEMA   = "sv_bank_transactions_fraud_silver"
SOURCE_TABLE    = f"{CATALOG}.{SILVER_SCHEMA}.silver_transactions"
ALERT_TABLE     = f"{CATALOG}.{SILVER_SCHEMA}.silver_impossible_travel_alerts"
STATE_TABLE     = f"{CATALOG}.{SILVER_SCHEMA}.user_last_location"     # streaming state
CHECKPOINT_PATH = "/Volumes/financial_security/raw_data/sv_bank_fraud_sdp_metadata/checkpoints/impossible_travel"

DISTANCE_THRESHOLD_MILES = 500
TIME_WINDOW_MINUTES      = 10
STATE_EXPIRY_HOURS       = 24    # purge user state older than this

LAKEBASE_INSTANCE = "sv-fraud-triage-db"
LAKEBASE_DB       = "fraud_ops"
LAKEBASE_SCHEMA   = "fraud_triage"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("impossible_travel")


# ─────────────────────────────────────────────────────────────────────────────
# Haversine distance — pure Spark Column API (no Python UDFs)
# Works identically in simulate and streaming mode via Databricks Connect.
# ─────────────────────────────────────────────────────────────────────────────
def haversine_miles(lat1, lon1, lat2, lon2) -> "Column":
    """
    Return a Spark Column expression for great-circle distance in miles.
    All inputs are Spark Column objects or column name strings.
    """
    R = 3_958.8
    φ1 = F.radians(lat1)
    φ2 = F.radians(lat2)
    Δφ = F.radians(lat2 - lat1)
    Δλ = F.radians(lon2 - lon1)
    a  = (F.pow(F.sin(Δφ / 2), 2)
          + F.cos(φ1) * F.cos(φ2) * F.pow(F.sin(Δλ / 2), 2))
    return F.lit(2 * R) * F.asin(F.sqrt(a))


# ─────────────────────────────────────────────────────────────────────────────
# Risk score — pure Column expression
# ─────────────────────────────────────────────────────────────────────────────
def risk_score_expr(dist_col, speed_col, amount_col) -> "Column":
    """
    60 pts base (threshold breach)
    + up to 30 pts for extreme implied speed
    + up to 10 pts for high-value transaction
    Capped at 100.
    """
    speed_pts  = F.least(F.lit(30.0), speed_col / F.lit(1000.0) * F.lit(10.0))
    amount_pts = F.least(F.lit(10.0),
                         F.coalesce(amount_col, F.lit(0.0)) / F.lit(100_000.0) * F.lit(10.0))
    return F.least(F.lit(100.0), F.lit(60.0) + speed_pts + amount_pts)


# ─────────────────────────────────────────────────────────────────────────────
# Shared enrichment: add distance / time / risk columns once flags are joined
# ─────────────────────────────────────────────────────────────────────────────
def add_travel_metrics(df: DataFrame,
                       prev_lat="prev_lat",  prev_lon="prev_lon",
                       curr_lat="txn_lat",   curr_lon="txn_lon",
                       prev_ts="prev_ts",    curr_ts="event_timestamp",
                       amount="amount") -> DataFrame:
    df = df.withColumn(
        "distance_miles",
        F.round(haversine_miles(F.col(prev_lat), F.col(prev_lon),
                                F.col(curr_lat), F.col(curr_lon)), 2)
    )
    df = df.withColumn(
        "time_gap_minutes",
        F.round(
            (F.unix_timestamp(curr_ts) - F.unix_timestamp(prev_ts)) / F.lit(60.0),
            3
        )
    )
    df = df.withColumn(
        "implied_speed_mph",
        F.round(
            F.col("distance_miles") / F.greatest(F.col("time_gap_minutes") / F.lit(60.0), F.lit(1e-6)),
            1
        )
    )
    df = df.withColumn(
        "risk_score",
        F.round(risk_score_expr(F.col("distance_miles"),
                                F.col("implied_speed_mph"),
                                F.col(amount)), 2)
    )
    df = df.withColumn(
        "risk_tier",
        F.when(F.col("risk_score") >= 80, "CRITICAL")
         .when(F.col("risk_score") >= 60, "HIGH")
         .when(F.col("risk_score") >= 40, "MEDIUM")
         .otherwise("LOW")
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Lakebase helpers
# ─────────────────────────────────────────────────────────────────────────────
def _lakebase_conn(w: WorkspaceClient) -> psycopg.Connection:
    inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[LAKEBASE_INSTANCE],
    )
    return psycopg.connect(
        f"host={inst.read_write_dns} dbname={LAKEBASE_DB} "
        f"user={w.current_user.me().user_name} password={cred.token} "
        f"sslmode=require connect_timeout=15"
    )


def write_to_lakebase(alerts_df: DataFrame, batch_id: int, w: WorkspaceClient):
    rows = alerts_df.select(
        "alert_id", "user_id", "curr_transaction_id", "amount",
        "transaction_type", "channel", "curr_city", "curr_state",
        "distance_miles", "time_gap_minutes", "implied_speed_mph",
        "risk_score", "risk_tier", "prev_event_timestamp",
        "curr_event_timestamp", "alert_generated_at",
    ).collect()

    if not rows:
        return

    sql = f"""
        INSERT INTO {LAKEBASE_SCHEMA}.real_time_fraud_triage (
            transaction_id, user_id, alert_id,
            risk_score, risk_tier, model_version, risk_factors,
            detected_fraud_type, transaction_amount, transaction_type,
            transaction_channel, transaction_city, transaction_state,
            minutes_after_login,
            automated_action, action_taken_at, action_executed_by,
            triage_status, source_system, pipeline_run_id, alert_generated_at
        ) VALUES (
            %s,%s,%s, %s,%s,%s, %s::jsonb,
            'GEO_ANOMALY',%s,%s, %s,%s,%s, %s,
            'FLAG_FOR_REVIEW', NOW(), 'impossible_travel_v1',
            'PENDING','impossible_travel_detector',%s,%s
        ) ON CONFLICT DO NOTHING
    """
    with _lakebase_conn(w) as conn:
        with conn.cursor() as cur:
            for r in rows:
                factors = json.dumps([
                    {"name": "impossible_travel",   "weight": 0.60, "value": r.distance_miles,
                     "description": f"Location jumped {r.distance_miles:.0f} mi in {r.time_gap_minutes:.1f} min"},
                    {"name": "implied_speed_mph",   "weight": 0.25, "value": r.implied_speed_mph,
                     "description": f"Implied speed {r.implied_speed_mph:.0f} mph (jet ~600 mph)"},
                    {"name": "transaction_amount",  "weight": 0.15,
                     "value": float(r.amount) if r.amount else 0,
                     "description": f"INR {r.amount:,.2f}" if r.amount else "N/A"},
                ])
                cur.execute(sql, (
                    r.curr_transaction_id, r.user_id, r.alert_id,
                    float(r.risk_score), r.risk_tier, "impossible_travel_v1", factors,
                    float(r.amount) if r.amount else None, r.transaction_type,
                    r.channel, r.curr_city, r.curr_state,
                    float(r.time_gap_minutes),
                    f"batch_{batch_id}", r.alert_generated_at,
                ))
        conn.commit()
    log.info(f"[batch {batch_id}] {len(rows)} alert(s) → Lakebase")


# ─────────────────────────────────────────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    return DatabricksSession.builder.remote(
        host=cfg["databricks_oneenv"]["host"],
        token=cfg["databricks_oneenv"]["token"],
        serverless=True,
    ).getOrCreate()


def build_w() -> WorkspaceClient:
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    return WorkspaceClient(host=cfg["databricks_oneenv"]["host"],
                           token=cfg["databricks_oneenv"]["token"])


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap: create necessary Delta tables
# ─────────────────────────────────────────────────────────────────────────────
def bootstrap_tables(spark: SparkSession):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ALERT_TABLE} (
            alert_id               STRING  NOT NULL,
            user_id                STRING  NOT NULL,
            prev_transaction_id    STRING,
            curr_transaction_id    STRING,
            prev_city              STRING,
            prev_state             STRING,
            curr_city              STRING,
            curr_state             STRING,
            prev_lat               DOUBLE,
            prev_lon               DOUBLE,
            curr_lat               DOUBLE,
            curr_lon               DOUBLE,
            distance_miles         DOUBLE,
            time_gap_minutes       DOUBLE,
            implied_speed_mph      DOUBLE,
            risk_score             DOUBLE,
            risk_tier              STRING,
            transaction_type       STRING,
            amount                 DOUBLE,
            channel                STRING,
            prev_event_timestamp   TIMESTAMP,
            curr_event_timestamp   TIMESTAMP,
            alert_generated_at     TIMESTAMP
        )
        USING DELTA
        COMMENT 'Impossible Travel alerts: geo-location jumped > {DISTANCE_THRESHOLD_MILES} miles in < {TIME_WINDOW_MINUTES} min.'
        CLUSTER BY (user_id)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
            user_id        STRING  NOT NULL,
            last_lat       DOUBLE,
            last_lon       DOUBLE,
            last_city      STRING,
            last_state     STRING,
            last_txn_id    STRING,
            last_ts        TIMESTAMP,
            updated_at     TIMESTAMP
        )
        USING DELTA
        COMMENT 'Streaming state: latest known location per user for impossible travel detection.'
        CLUSTER BY (user_id)
    """)
    log.info(f"Tables ready: {ALERT_TABLE} | {STATE_TABLE}")


# ─────────────────────────────────────────────────────────────────────────────
# MODE 1 — SIMULATE: window functions on historical static data
# ─────────────────────────────────────────────────────────────────────────────
def run_simulate(spark: SparkSession, w: WorkspaceClient):
    log.info(f"SIMULATE: scanning {SOURCE_TABLE} with window functions ...")

    df = (
        spark.read.table(SOURCE_TABLE)
        .select("transaction_id", "user_id",
                F.col("txn_lat").cast("double"),
                F.col("txn_lon").cast("double"),
                "txn_city", "txn_state",
                "transaction_type",
                F.col("amount").cast("double"),
                "channel",
                F.col("event_timestamp").cast("timestamp"))
        .filter(F.col("txn_lat").isNotNull() &
                F.col("txn_lon").isNotNull() &
                F.col("event_timestamp").isNotNull())
    )

    # Per-user ordered window to access the previous event
    w_spec = Window.partitionBy("user_id").orderBy("event_timestamp")

    df = (
        df
        .withColumn("prev_lat",          F.lag("txn_lat").over(w_spec))
        .withColumn("prev_lon",          F.lag("txn_lon").over(w_spec))
        .withColumn("prev_city",         F.lag("txn_city").over(w_spec))
        .withColumn("prev_state",        F.lag("txn_state").over(w_spec))
        .withColumn("prev_ts",           F.lag("event_timestamp").over(w_spec))
        .withColumn("prev_transaction_id", F.lag("transaction_id").over(w_spec))
        .filter(F.col("prev_lat").isNotNull())   # skip first event per user
    )

    df = add_travel_metrics(df, prev_ts="prev_ts", curr_ts="event_timestamp")

    alerts = (
        df
        .filter(
            (F.col("distance_miles") > DISTANCE_THRESHOLD_MILES) &
            (F.col("time_gap_minutes") > 0) &
            (F.col("time_gap_minutes") < TIME_WINDOW_MINUTES)
        )
        .withColumn("alert_id",           F.expr("uuid()"))
        .withColumn("alert_generated_at", F.current_timestamp())
        .withColumn("curr_transaction_id", F.col("transaction_id"))
        .withColumn("curr_city",           F.col("txn_city"))
        .withColumn("curr_state",          F.col("txn_state"))
        .withColumn("curr_lat",            F.col("txn_lat"))
        .withColumn("curr_lon",            F.col("txn_lon"))
        .withColumn("prev_event_timestamp", F.col("prev_ts"))
        .withColumn("curr_event_timestamp", F.col("event_timestamp"))
        .select(
            "alert_id", "user_id",
            "prev_transaction_id", "curr_transaction_id",
            "prev_city", "prev_state", "curr_city", "curr_state",
            "prev_lat", "prev_lon", "curr_lat", "curr_lon",
            "distance_miles", "time_gap_minutes", "implied_speed_mph",
            "risk_score", "risk_tier",
            "transaction_type", "amount", "channel",
            "prev_event_timestamp", "curr_event_timestamp",
            "alert_generated_at",
        )
    )

    n = alerts.count()
    log.info(f"SIMULATE complete: {n} impossible travel alert(s) found.")

    if n == 0:
        log.info("No impossible travel events in historical data.")
        return

    log.info("Top alerts (sorted by distance):")
    alerts.select(
        "user_id", "prev_city", "curr_city",
        "distance_miles", "time_gap_minutes", "implied_speed_mph",
        "risk_score", "risk_tier", "amount",
    ).orderBy(F.col("distance_miles").desc()).show(20, truncate=False)

    # Persist to Delta
    (alerts.write
     .format("delta")
     .mode("append")
     .option("mergeSchema", "true")
     .saveAsTable(ALERT_TABLE))
    log.info(f"Written {n} alert(s) → {ALERT_TABLE}")

    # Persist to Lakebase
    try:
        write_to_lakebase(alerts, batch_id=0, w=w)
    except Exception as e:
        log.error(f"Lakebase write failed: {e}  (alerts are safe in Delta)")


# ─────────────────────────────────────────────────────────────────────────────
# MODE 2 — STREAM: foreachBatch with Delta state table
# ─────────────────────────────────────────────────────────────────────────────
def run_stream(spark: SparkSession, w_client: WorkspaceClient,
               trigger: str = "30 seconds"):

    def process_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            log.info(f"[batch {batch_id}] empty.")
            return

        new_events = (
            batch_df
            .select("transaction_id", "user_id",
                    F.col("txn_lat").cast("double"),
                    F.col("txn_lon").cast("double"),
                    "txn_city", "txn_state",
                    "transaction_type",
                    F.col("amount").cast("double"),
                    "channel",
                    F.col("event_timestamp").cast("timestamp"))
            .filter(F.col("txn_lat").isNotNull() &
                    F.col("txn_lon").isNotNull() &
                    F.col("event_timestamp").isNotNull())
        )

        # Keep only the latest event per user within this micro-batch
        w_spec = Window.partitionBy("user_id").orderBy(F.col("event_timestamp").desc())
        latest = (
            new_events
            .withColumn("_rn", F.row_number().over(w_spec))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        # Join with state table (last known location per user)
        try:
            state = spark.read.table(STATE_TABLE).alias("state")
            has_state = True
        except Exception:
            has_state = False

        if has_state:
            joined = (
                latest.alias("curr")
                .join(state.select(
                    "user_id",
                    F.col("last_lat").alias("prev_lat"),
                    F.col("last_lon").alias("prev_lon"),
                    F.col("last_city").alias("prev_city"),
                    F.col("last_state").alias("prev_state"),
                    F.col("last_txn_id").alias("prev_transaction_id"),
                    F.col("last_ts").alias("prev_ts"),
                ), on="user_id", how="inner")
            )

            joined = add_travel_metrics(
                joined, prev_ts="prev_ts", curr_ts="event_timestamp"
            )

            alerts = (
                joined
                .filter(
                    (F.col("distance_miles") > DISTANCE_THRESHOLD_MILES) &
                    (F.col("time_gap_minutes") > 0) &
                    (F.col("time_gap_minutes") < TIME_WINDOW_MINUTES)
                )
                .withColumn("alert_id",            F.expr("uuid()"))
                .withColumn("alert_generated_at",  F.current_timestamp())
                .withColumn("curr_transaction_id", F.col("transaction_id"))
                .withColumn("curr_city",           F.col("txn_city"))
                .withColumn("curr_state",          F.col("txn_state"))
                .withColumn("curr_lat",            F.col("txn_lat"))
                .withColumn("curr_lon",            F.col("txn_lon"))
                .withColumn("prev_event_timestamp", F.col("prev_ts"))
                .withColumn("curr_event_timestamp", F.col("event_timestamp"))
                .select(
                    "alert_id", "user_id",
                    "prev_transaction_id", "curr_transaction_id",
                    "prev_city", "prev_state", "curr_city", "curr_state",
                    "prev_lat", "prev_lon", "curr_lat", "curr_lon",
                    "distance_miles", "time_gap_minutes", "implied_speed_mph",
                    "risk_score", "risk_tier",
                    "transaction_type", "amount", "channel",
                    "prev_event_timestamp", "curr_event_timestamp",
                    "alert_generated_at",
                )
            )

            n_alerts = alerts.count()
            if n_alerts > 0:
                log.info(f"[batch {batch_id}] ⚠  {n_alerts} IMPOSSIBLE TRAVEL alert(s)!")
                alerts.select(
                    "user_id", "prev_city", "curr_city",
                    "distance_miles", "time_gap_minutes", "implied_speed_mph",
                    "risk_score", "risk_tier",
                ).show(truncate=False)

                (alerts.write.format("delta").mode("append")
                 .option("mergeSchema", "true").saveAsTable(ALERT_TABLE))
                try:
                    write_to_lakebase(alerts, batch_id, w_client)
                except Exception as e:
                    log.error(f"Lakebase write failed: {e}")
            else:
                log.info(f"[batch {batch_id}] No impossible travel in {latest.count()} new event(s).")

        # Update state table: MERGE in latest location per user
        latest_for_state = (
            latest
            .withColumn("updated_at", F.current_timestamp())
            .select(
                "user_id",
                F.col("txn_lat").alias("last_lat"),
                F.col("txn_lon").alias("last_lon"),
                F.col("txn_city").alias("last_city"),
                F.col("txn_state").alias("last_state"),
                F.col("transaction_id").alias("last_txn_id"),
                F.col("event_timestamp").alias("last_ts"),
                "updated_at",
            )
        )
        latest_for_state.createOrReplaceTempView("_incoming_locations")
        spark.sql(f"""
            MERGE INTO {STATE_TABLE} AS tgt
            USING _incoming_locations AS src
            ON tgt.user_id = src.user_id
            WHEN MATCHED AND src.last_ts > tgt.last_ts THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        log.info(f"[batch {batch_id}] State table updated.")

    # ── Stream source ─────────────────────────────────────────────────────────
    log.info(f"Starting stream from {SOURCE_TABLE} ...")
    stream_df = (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "latest")
        .table(SOURCE_TABLE)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
    )

    query = (
        stream_df.writeStream
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=trigger)
        .foreachBatch(process_batch)
        .queryName("impossible_travel_detector")
        .start()
    )

    log.info(
        f"Stream running — watching for >{DISTANCE_THRESHOLD_MILES} mi "
        f"in <{TIME_WINDOW_MINUTES} min. Press Ctrl+C to stop."
    )
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopping stream ...")
        query.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def monitor_impossible_travel(simulate: bool = False, trigger: str = "30 seconds"):
    spark = build_spark()
    w     = build_w()
    bootstrap_tables(spark)
    if simulate:
        run_simulate(spark, w)
    else:
        run_stream(spark, w, trigger=trigger)


if __name__ == "__main__":
    import sys
    monitor_impossible_travel(simulate="--simulate" in sys.argv or "-s" in sys.argv)
