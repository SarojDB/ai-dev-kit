"""
Generate synthetic SaaS company internal data.
Datasets: users (10k), orders (30k), events (50k)
Storage: sv_ai_builder_workspace_catalog.aibuilder_saas_demo.saas_demo (NDJSON)
"""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession
import json, os

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG     = "sv_ai_builder_workspace_catalog"
SCHEMA      = "aibuilder_saas_demo"
VOLUME      = "saas_demo"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

N_USERS  = 10_000
N_ORDERS = 30_000
N_EVENTS = 50_000

# Orders span last 3 months
NOW             = datetime.utcnow()
ORDERS_END      = NOW
ORDERS_START    = NOW - timedelta(days=90)

# Events span a single 6-hour window (today)
EVENTS_END      = NOW
EVENTS_START    = NOW - timedelta(hours=6)

SEED = 42
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

spark = SparkSession.builder.getOrCreate()

# =============================================================================
# CREATE INFRASTRUCTURE
# =============================================================================
print("Creating catalog / schema / volume if needed...")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.{VOLUME}")
print(f"  Volume path: {VOLUME_PATH}")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def random_ts(start: datetime, end: datetime) -> str:
    delta = (end - start).total_seconds()
    return (start + timedelta(seconds=float(np.random.uniform(0, delta)))).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"

def updated_ts(created: str, max_days: int = 30) -> str:
    dt = datetime.strptime(created, "%Y-%m-%dT%H:%M:%S.%fZ")
    offset = timedelta(seconds=float(np.random.exponential(scale=max_days * 86400 * 0.3)))
    updated = min(dt + offset, NOW)
    return updated.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def write_ndjson(records: list, path: str):
    """Write list of dicts to newline-delimited JSON via Spark."""
    df = spark.createDataFrame(pd.DataFrame(records))
    df.coalesce(1).write.mode("overwrite").json(path)
    print(f"  Saved {len(records):,} records → {path}")

# =============================================================================
# REFERENCE LOOKUPS
# =============================================================================
PRODUCT_TYPES = [
    "Analytics Platform", "Data Pipeline", "ML Workbench",
    "BI Dashboard", "API Gateway", "Storage Connector",
    "Security Suite", "Collaboration Hub",
]
PRODUCT_WEIGHTS = [0.20, 0.18, 0.15, 0.14, 0.12, 0.09, 0.07, 0.05]

SUBSCRIPTION_TIERS = ["Free", "Starter", "Professional", "Business", "Enterprise"]
TIER_WEIGHTS       = [0.30, 0.25, 0.22, 0.15, 0.08]

TIER_PRICE_PARAMS = {
    "Free":         (None, None),
    "Starter":      (3.2, 0.4),   # lognormal → ~$25 median
    "Professional": (4.3, 0.5),   # → ~$74 median
    "Business":     (5.5, 0.5),   # → ~$245 median
    "Enterprise":   (6.5, 0.5),   # → ~$665 median
}

OS_TYPES    = ["Windows", "macOS", "Linux", "iOS", "Android", "ChromeOS"]
OS_WEIGHTS  = [0.30, 0.25, 0.18, 0.12, 0.10, 0.05]
BROWSERS    = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
BR_WEIGHTS  = [0.50, 0.18, 0.20, 0.09, 0.03]
COUNTRIES   = ["US", "GB", "IN", "DE", "CA", "AU", "FR", "SG", "BR", "JP"]
CTRY_WEIGHTS= [0.35, 0.10, 0.12, 0.08, 0.07, 0.06, 0.06, 0.05, 0.06, 0.05]

EVENT_TYPES = [
    "page_view", "button_click", "search", "login", "logout",
    "feature_activated", "feature_deactivated", "api_call",
    "file_upload", "file_download", "dashboard_opened",
    "report_generated", "error_encountered", "session_start",
    "session_end", "settings_changed", "invite_sent",
    "export_triggered", "notification_dismissed", "onboarding_step_completed",
]
EVENT_WEIGHTS = [
    0.18, 0.14, 0.10, 0.06, 0.04,
    0.06, 0.02, 0.08,
    0.03, 0.04, 0.06,
    0.03, 0.03, 0.05,
    0.04, 0.02, 0.02,
    0.03, 0.02, 0.02,
]
EVENT_WEIGHTS = [w / sum(EVENT_WEIGHTS) for w in EVENT_WEIGHTS]  # normalize to 1.0

# Resources: one resource_id per product_type
RESOURCE_IDS = {pt: f"RES-{i:04d}" for i, pt in enumerate(PRODUCT_TYPES)}

# =============================================================================
# 1. USERS  (master table)
# =============================================================================
print(f"\nGenerating {N_USERS:,} users...")

user_records = []
for i in range(N_USERS):
    uid = f"USR-{i:06d}"
    tier = np.random.choice(SUBSCRIPTION_TIERS, p=TIER_WEIGHTS)
    country = np.random.choice(COUNTRIES, p=CTRY_WEIGHTS)
    created = random_ts(ORDERS_START - timedelta(days=180), ORDERS_START)

    user_records.append({
        "user_id":            uid,
        "email":              fake.email(),
        "full_name":          fake.name(),
        "company":            fake.company(),
        "country":            country,
        "city":               fake.city(),
        "subscription_tier":  tier,
        "product_type":       np.random.choice(PRODUCT_TYPES, p=PRODUCT_WEIGHTS),
        "os":                 np.random.choice(OS_TYPES,    p=OS_WEIGHTS),
        "browser":            np.random.choice(BROWSERS,    p=BR_WEIGHTS),
        "device_type":        np.random.choice(["desktop", "mobile", "tablet"], p=[0.65, 0.25, 0.10]),
        "app_version":        f"{np.random.randint(3,6)}.{np.random.randint(0,12)}.{np.random.randint(0,9)}",
        "is_active":          bool(np.random.choice([True, False], p=[0.88, 0.12])),
        "mfa_enabled":        bool(np.random.choice([True, False], p=[0.55, 0.45])),
        "role":               np.random.choice(["admin","editor","viewer","analyst"], p=[0.10,0.25,0.40,0.25]),
        "created_timestamp":  created,
        "updated_timestamp":  updated_ts(created, max_days=90),
    })

users_df = pd.DataFrame(user_records)
print(f"  Tier distribution:\n{users_df['subscription_tier'].value_counts().to_string()}")

# Build lookup maps
user_ids         = users_df["user_id"].tolist()
user_tier_map    = dict(zip(users_df["user_id"], users_df["subscription_tier"]))
user_product_map = dict(zip(users_df["user_id"], users_df["product_type"]))

# Weighted sampling — paid tiers generate more activity
activity_weights = users_df["subscription_tier"].map(
    {"Free": 0.5, "Starter": 1.0, "Professional": 2.0, "Business": 3.5, "Enterprise": 6.0}
)
user_activity_weights = (activity_weights / activity_weights.sum()).tolist()

write_ndjson(user_records, f"{VOLUME_PATH}/users")

# =============================================================================
# 2. ORDERS  (references users; 3-month window, $20-$1000)
# =============================================================================
print(f"\nGenerating {N_ORDERS:,} orders...")

order_records = []
for i in range(N_ORDERS):
    uid   = np.random.choice(user_ids, p=user_activity_weights)
    tier  = user_tier_map[uid]
    pt    = user_product_map[uid]
    rid   = RESOURCE_IDS[pt]

    # Lognormal price clipped to $20–$1000
    mu, sigma = TIER_PRICE_PARAMS.get(tier, (4.3, 0.5))
    if mu is None:
        amount = round(float(np.random.uniform(20, 49)), 2)
    else:
        raw = np.random.lognormal(mu, sigma)
        amount = round(float(np.clip(raw, 20, 1000)), 2)

    created = random_ts(ORDERS_START, ORDERS_END)
    qty     = int(np.random.choice([1, 2, 3, 5, 10], p=[0.60, 0.20, 0.10, 0.07, 0.03]))

    order_records.append({
        "order_id":           f"ORD-{i:07d}",
        "user_id":            uid,
        "resource_id":        rid,
        "product_type":       pt,
        "subscription_tier":  tier,
        "quantity":           qty,
        "unit_price":         amount,
        "total_value":        round(amount * qty, 2),
        "currency":           "USD",
        "status":             np.random.choice(
                                  ["completed","pending","cancelled","refunded"],
                                  p=[0.82, 0.10, 0.05, 0.03]),
        "payment_method":     np.random.choice(
                                  ["credit_card","invoice","bank_transfer","paypal"],
                                  p=[0.55, 0.25, 0.12, 0.08]),
        "billing_country":    np.random.choice(COUNTRIES, p=CTRY_WEIGHTS),
        "promo_code_used":    bool(np.random.choice([True, False], p=[0.15, 0.85])),
        "created_timestamp":  created,
        "updated_timestamp":  updated_ts(created, max_days=14),
    })

orders_df = pd.DataFrame(order_records)
print(f"  Value stats: min=${orders_df['unit_price'].min():.2f}  "
      f"median=${orders_df['unit_price'].median():.2f}  "
      f"max=${orders_df['unit_price'].max():.2f}")

write_ndjson(order_records, f"{VOLUME_PATH}/orders")

# =============================================================================
# 3. EVENTS  (references users; 6-hour window)
# =============================================================================
print(f"\nGenerating {N_EVENTS:,} events...")

# Session map: each user gets 1-5 sessions during the window
session_map = {}
for uid in user_ids:
    n_sessions = int(np.random.choice([0, 1, 2, 3, 5], p=[0.45, 0.30, 0.15, 0.07, 0.03]))
    session_map[uid] = [f"SES-{uid}-{s:02d}" for s in range(n_sessions)]

# Filter to users who have at least one session
active_users     = [u for u, s in session_map.items() if s]
active_weights_raw = np.array([activity_weights[users_df.index[users_df["user_id"]==u][0]]
                                for u in active_users])
active_weights   = (active_weights_raw / active_weights_raw.sum()).tolist()

event_records = []
for i in range(N_EVENTS):
    uid      = np.random.choice(active_users, p=active_weights)
    tier     = user_tier_map[uid]
    pt       = user_product_map[uid]
    rid      = RESOURCE_IDS[pt]
    sessions = session_map[uid]
    session  = np.random.choice(sessions)

    evt_type = np.random.choice(EVENT_TYPES, p=EVENT_WEIGHTS)
    evt_ts   = random_ts(EVENTS_START, EVENTS_END)

    # Latency: error and api_call have realistic latency
    if evt_type == "error_encountered":
        latency_ms = int(np.random.lognormal(7.5, 0.8))   # higher latency at errors
    elif evt_type == "api_call":
        latency_ms = int(np.random.lognormal(5.5, 0.9))
    else:
        latency_ms = int(np.random.lognormal(4.5, 0.7))

    # HTTP status code where applicable
    if evt_type in ("api_call", "error_encountered"):
        if evt_type == "error_encountered":
            http_status = int(np.random.choice([400,401,403,404,429,500,502,503],
                                               p=[0.15,0.10,0.08,0.20,0.10,0.20,0.10,0.07]))
        else:
            http_status = int(np.random.choice([200,201,204,400,429,500],
                                               p=[0.82,0.06,0.04,0.04,0.02,0.02]))
    else:
        http_status = None

    # Feature name for feature events
    if evt_type in ("feature_activated","feature_deactivated"):
        feature_name = np.random.choice([
            "dark_mode","two_factor_auth","advanced_filters","bulk_export",
            "scheduled_reports","webhooks","sso","custom_dashboards"
        ])
    else:
        feature_name = None

    created = evt_ts  # for events, created == event time
    record = {
        "event_id":          f"EVT-{i:08d}",
        "user_id":           uid,
        "session_id":        session,
        "resource_id":       rid,
        "product_type":      pt,
        "subscription_tier": tier,
        "event_type":        evt_type,
        "event_timestamp":   evt_ts,
        "page_url":          f"/{pt.lower().replace(' ','_')}/{fake.uri_path()}",
        "referrer":          np.random.choice(
                                 ["direct","google","email_campaign","in_app","slack"],
                                 p=[0.35, 0.25, 0.18, 0.14, 0.08]),
        "latency_ms":        latency_ms,
        "http_status_code":  http_status,
        "feature_name":      feature_name,
        "browser":           users_df.loc[users_df["user_id"]==uid, "browser"].values[0],
        "os":                users_df.loc[users_df["user_id"]==uid, "os"].values[0],
        "device_type":       users_df.loc[users_df["user_id"]==uid, "device_type"].values[0],
        "country":           users_df.loc[users_df["user_id"]==uid, "country"].values[0],
        "created_timestamp": created,
        "updated_timestamp": updated_ts(created, max_days=1),
    }
    event_records.append(record)

events_df = pd.DataFrame(event_records)
print(f"  Event type distribution (top 5):\n"
      f"{events_df['event_type'].value_counts().head(5).to_string()}")
print(f"  Time window: {EVENTS_START.strftime('%H:%M')} UTC → {EVENTS_END.strftime('%H:%M')} UTC")

write_ndjson(event_records, f"{VOLUME_PATH}/events")

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "="*60)
print("DATA GENERATION COMPLETE")
print("="*60)
print(f"  Users  → {VOLUME_PATH}/users   ({N_USERS:,} records)")
print(f"  Orders → {VOLUME_PATH}/orders  ({N_ORDERS:,} records)")
print(f"  Events → {VOLUME_PATH}/events  ({N_EVENTS:,} records)")
print(f"\nKey join columns: user_id, resource_id, product_type, subscription_tier")
print(f"Timestamp columns: created_timestamp, updated_timestamp (all)")
print(f"                   event_timestamp   (events only)")
