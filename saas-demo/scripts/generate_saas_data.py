"""Generate synthetic SaaS data: users, orders, events -> Unity Catalog Volume."""
# Install required libraries first
import subprocess
subprocess.run(["pip", "install", "faker", "holidays", "-q"], check=True)

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
import holidays
import uuid

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG     = "aibuilder_saas_demo"
SCHEMA      = "raw_data"
VOLUME      = "saas_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

N_USERS  = 10_000
N_ORDERS = 30_000
N_EVENTS = 50_000

# Orders span 3 months; events span the last 6 hours from now
NOW        = datetime.now()
ORDER_END  = NOW.replace(hour=0, minute=0, second=0, microsecond=0)
ORDER_START = ORDER_END - timedelta(days=90)
EVENT_END  = NOW
EVENT_START = NOW - timedelta(hours=6)

US_HOLIDAYS = holidays.US(years=[ORDER_START.year, ORDER_END.year])
SEED = 42

# =============================================================================
# SETUP
# =============================================================================
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# =============================================================================
# INFRASTRUCTURE
# =============================================================================
print("Creating catalog / schema / volume if needed...")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"  Volume ready: {VOLUME_PATH}")

# =============================================================================
# SHARED LOOKUPS
# =============================================================================
SUBSCRIPTION_TIERS = ["Free", "Starter", "Professional", "Enterprise"]
TIER_WEIGHTS       = [0.45, 0.30, 0.18, 0.07]
PRODUCT_TYPES      = ["API", "Dashboard", "Analytics", "Automation"]
PRODUCT_WEIGHTS    = [0.35, 0.25, 0.22, 0.18]
COUNTRIES          = ["US", "UK", "DE", "FR", "CA", "AU", "IN", "BR", "SG", "JP"]
COUNTRY_WEIGHTS    = [0.38, 0.12, 0.09, 0.07, 0.07, 0.06, 0.06, 0.05, 0.05, 0.05]
DEVICES            = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS     = [0.62, 0.30, 0.08]
BROWSERS           = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
BROWSER_WEIGHTS    = [0.65, 0.10, 0.13, 0.09, 0.03]
OS_LIST            = ["Windows", "macOS", "Linux", "iOS", "Android"]
OS_WEIGHTS         = [0.45, 0.22, 0.08, 0.13, 0.12]
ROLES              = ["admin", "developer", "analyst", "viewer", "manager"]
ROLE_WEIGHTS       = [0.08, 0.30, 0.25, 0.27, 0.10]
APP_VERSIONS       = ["2.4.1", "2.4.0", "2.3.5", "2.3.2", "2.2.9"]
APP_VERSION_WEIGHTS= [0.45, 0.25, 0.15, 0.10, 0.05]

# =============================================================================
# 1. USERS  (10,000 records)
# =============================================================================
print(f"\nGenerating {N_USERS:,} users...")

user_ids     = [f"USR-{i:05d}" for i in range(N_USERS)]
tiers        = np.random.choice(SUBSCRIPTION_TIERS, N_USERS, p=TIER_WEIGHTS)
products     = np.random.choice(PRODUCT_TYPES, N_USERS, p=PRODUCT_WEIGHTS)
countries    = np.random.choice(COUNTRIES, N_USERS, p=COUNTRY_WEIGHTS)
devices      = np.random.choice(DEVICES, N_USERS, p=DEVICE_WEIGHTS)
browsers     = np.random.choice(BROWSERS, N_USERS, p=BROWSER_WEIGHTS)
os_arr       = np.random.choice(OS_LIST, N_USERS, p=OS_WEIGHTS)
roles        = np.random.choice(ROLES, N_USERS, p=ROLE_WEIGHTS)
app_versions = np.random.choice(APP_VERSIONS, N_USERS, p=APP_VERSION_WEIGHTS)

# Account age: Enterprise users tend to be older accounts
account_age_days = np.where(
    tiers == "Enterprise", np.random.randint(365, 1461, N_USERS),
    np.where(tiers == "Professional", np.random.randint(180, 730, N_USERS),
    np.where(tiers == "Starter", np.random.randint(30, 365, N_USERS),
             np.random.randint(1, 180, N_USERS)))
)

created_ts = [
    NOW - timedelta(days=int(age), hours=np.random.randint(0, 24))
    for age in account_age_days
]

users_pdf = pd.DataFrame({
    "user_id":          user_ids,
    "full_name":        [fake.name() for _ in range(N_USERS)],
    "email":            [fake.email() for _ in range(N_USERS)],
    "email_domain":     [fake.domain_name() for _ in range(N_USERS)],
    "company":          [fake.company() for _ in range(N_USERS)],
    "role":             roles,
    "subscription_tier": tiers,
    "product_type":     products,
    "country":          countries,
    "city":             [fake.city() for _ in range(N_USERS)],
    "device_type":      devices,
    "browser":          browsers,
    "os":               os_arr,
    "app_version":      app_versions,
    "account_age_days": account_age_days,
    "is_active":        np.random.choice([True, False], N_USERS, p=[0.88, 0.12]),
    "is_enterprise":    tiers == "Enterprise",
    "mfa_enabled":      np.random.choice([True, False], N_USERS, p=[0.55, 0.45]),
    "is_valid_email":   np.random.choice([True, False], N_USERS, p=[0.97, 0.03]),
    "created_timestamp":  created_ts,
    "updated_timestamp":  [
        ct + timedelta(days=np.random.randint(0, max(1, int(age))))
        for ct, age in zip(created_ts, account_age_days)
    ],
})

print(f"  Tier distribution:\n{users_pdf['subscription_tier'].value_counts().to_string()}")

# Lookups for downstream tables
user_ids_list   = users_pdf["user_id"].tolist()
user_tier_map   = dict(zip(users_pdf["user_id"], users_pdf["subscription_tier"]))
user_product_map= dict(zip(users_pdf["user_id"], users_pdf["product_type"]))
user_country_map= dict(zip(users_pdf["user_id"], users_pdf["country"]))
user_device_map = dict(zip(users_pdf["user_id"], users_pdf["device_type"]))
user_browser_map= dict(zip(users_pdf["user_id"], users_pdf["browser"]))

# Weighted sampling — Enterprise/Professional users generate more activity
tier_activity_w = users_pdf["subscription_tier"].map(
    {"Enterprise": 6.0, "Professional": 3.0, "Starter": 1.5, "Free": 1.0}
)
user_weights = (tier_activity_w / tier_activity_w.sum()).values

# =============================================================================
# 2. ORDERS  (30,000 records over 90 days)
# =============================================================================
print(f"\nGenerating {N_ORDERS:,} orders...")

PAYMENT_METHODS = ["credit_card", "paypal", "bank_transfer", "invoice", "crypto"]
PAYMENT_WEIGHTS = [0.55, 0.20, 0.15, 0.08, 0.02]
ORDER_STATUSES  = ["completed", "pending", "cancelled", "refunded"]
ORDER_STATUS_W  = [0.82, 0.08, 0.06, 0.04]
ORDER_CATEGORIES= ["new_subscription", "renewal", "upgrade", "addon", "one_time"]
ORDER_CAT_W     = [0.25, 0.40, 0.15, 0.12, 0.08]

# Price ranges by tier ($20 to $1000)
def sample_price(tier):
    if tier == "Enterprise":
        val = np.random.lognormal(6.2, 0.5)   # ~$500 median
    elif tier == "Professional":
        val = np.random.lognormal(5.2, 0.5)   # ~$180 median
    elif tier == "Starter":
        val = np.random.lognormal(4.2, 0.5)   # ~$67 median
    else:
        val = np.random.lognormal(3.4, 0.4)   # ~$30 median
    return round(float(np.clip(val, 20, 1000)), 2)

# Date distribution with weekday pattern + slight growth trend
total_days   = (ORDER_END - ORDER_START).days
date_range   = pd.date_range(ORDER_START, ORDER_END - timedelta(days=1), freq="D")

def order_day_weight(d):
    w = 1.0
    if d.weekday() >= 5: w *= 0.55          # weekend dip
    if d in US_HOLIDAYS: w *= 0.3           # holiday dip
    growth = 1 + 0.4 * ((d - ORDER_START).days / total_days)  # 40% growth over 3 months
    return max(0.05, w * growth * np.random.normal(1, 0.08))

day_weights = np.array([order_day_weight(d) for d in date_range])
day_weights /= day_weights.sum()

# Sample an order date for each order
order_days_idx  = np.random.choice(len(date_range), N_ORDERS, p=day_weights)
order_dates     = [date_range[i].date() for i in order_days_idx]

sampled_user_ids = np.random.choice(user_ids_list, N_ORDERS, p=user_weights)

orders_data = []
for i in range(N_ORDERS):
    uid   = sampled_user_ids[i]
    tier  = user_tier_map[uid]
    prod  = user_product_map[uid]
    odate = order_dates[i]
    qty   = int(np.random.choice([1, 2, 3, 5, 10], p=[0.60, 0.20, 0.10, 0.06, 0.04]))
    unit  = sample_price(tier)
    total = round(unit * qty, 2)
    cat   = np.random.choice(ORDER_CATEGORIES, p=ORDER_CAT_W)
    status= np.random.choice(ORDER_STATUSES, p=ORDER_STATUS_W)
    created = datetime.combine(odate, datetime.min.time()) + timedelta(
        hours=np.random.randint(6, 22), minutes=np.random.randint(0, 60))
    updated = created + timedelta(hours=np.random.randint(0, 48))

    orders_data.append({
        "order_id":         f"ORD-{i:06d}",
        "user_id":          uid,
        "resource_id":      f"RES-{np.random.randint(1, 2001):05d}",
        "product_type":     prod,
        "subscription_tier": tier,
        "order_category":   cat,
        "status":           status,
        "unit_price":       unit,
        "quantity":         qty,
        "total_value":      total,
        "currency":         "USD",
        "payment_method":   np.random.choice(PAYMENT_METHODS, p=PAYMENT_WEIGHTS),
        "promo_code_used":  np.random.choice([True, False], p=[0.18, 0.82]),
        "order_date":       odate,
        "order_month":      odate.strftime("%Y-%m"),
        "order_quarter":    f"Q{(odate.month - 1) // 3 + 1}-{odate.year}",
        "billing_country":  user_country_map[uid],
        "is_high_value":    total > 500,
        "is_anomaly_flag":  np.random.choice([True, False], p=[0.02, 0.98]),
        "created_timestamp": created,
        "updated_timestamp": updated,
    })

orders_pdf = pd.DataFrame(orders_data)
print(f"  Status distribution:\n{orders_pdf['status'].value_counts().to_string()}")
print(f"  Revenue range: ${orders_pdf['total_value'].min():.2f} - ${orders_pdf['total_value'].max():.2f}")
print(f"  Avg order value: ${orders_pdf['total_value'].mean():.2f}")

# =============================================================================
# 3. EVENTS  (50,000 events over 6-hour window)
# =============================================================================
print(f"\nGenerating {N_EVENTS:,} events over 6-hour window ({EVENT_START.strftime('%H:%M')} - {EVENT_END.strftime('%H:%M')})...")

EVENT_TYPES = [
    "page_view", "click", "api_call", "login", "logout",
    "search", "export", "feature_use", "error", "session_start",
    "session_end", "upload", "download", "settings_change", "share",
]
EVENT_TYPE_WEIGHTS = [0.22, 0.18, 0.15, 0.06, 0.04, 0.08, 0.05, 0.09, 0.04, 0.03, 0.02, 0.01, 0.01, 0.01, 0.01]
EVENT_CATEGORIES = {
    "page_view": "navigation", "click": "engagement", "api_call": "api",
    "login": "auth", "logout": "auth", "search": "engagement",
    "export": "engagement", "feature_use": "engagement", "error": "error",
    "session_start": "auth", "session_end": "auth", "upload": "engagement",
    "download": "engagement", "settings_change": "engagement", "share": "engagement",
}
FEATURE_NAMES = [
    "dashboard_builder", "report_export", "api_explorer", "user_management",
    "data_connector", "scheduler", "alert_manager", "audit_log",
    "ml_pipeline", "query_editor", "visualization", "collaboration",
]
PAGE_URLS = [
    "/dashboard", "/reports", "/api", "/settings", "/users",
    "/analytics", "/integrations", "/billing", "/help", "/onboarding",
]
HTTP_SUCCESS = [200, 201, 204]
HTTP_ERRORS  = [400, 401, 403, 404, 429, 500, 502, 503]

# 6-hour window in seconds
window_secs = int((EVENT_END - EVENT_START).total_seconds())

# Session assignments — each session has multiple events
N_SESSIONS   = N_USERS // 3   # ~3,333 sessions
session_ids  = [f"SES-{uuid.uuid4().hex[:8].upper()}" for _ in range(N_SESSIONS)]

# Distribute sessions across the 6-hour window (more activity mid-window)
sess_time_offsets = np.random.beta(2, 2, N_SESSIONS) * window_secs  # bell-shaped
sess_user_sample  = np.random.choice(user_ids_list, N_SESSIONS, p=user_weights)
sess_start_times  = [EVENT_START + timedelta(seconds=int(s)) for s in sess_time_offsets]

# Map sessions -> users/start-time
sess_user_map  = dict(zip(session_ids, sess_user_sample))
sess_start_map = dict(zip(session_ids, sess_start_times))

# Assign events to sessions (weighted by tier activity)
sess_tier_weights = np.array([
    tier_activity_w[user_ids_list.index(sess_user_map[s])] for s in session_ids
])
sess_tier_weights /= sess_tier_weights.sum()

event_session_ids = np.random.choice(session_ids, N_EVENTS, p=sess_tier_weights)

events_data = []
for i in range(N_EVENTS):
    sid   = event_session_ids[i]
    uid   = sess_user_map[sid]
    tier  = user_tier_map[uid]
    prod  = user_product_map[uid]
    sess_start = sess_start_map[sid]

    etype   = np.random.choice(EVENT_TYPES, p=EVENT_TYPE_WEIGHTS)
    ecat    = EVENT_CATEGORIES[etype]
    is_err  = etype == "error" or (
        etype == "api_call" and np.random.random() < (0.03 if tier == "Enterprise" else 0.06))

    # Latency: API calls and errors are slower
    if etype == "api_call":
        latency = int(np.random.lognormal(5.5, 0.8))    # ~245ms median
    elif is_err:
        latency = int(np.random.lognormal(6.5, 0.6))    # ~665ms median
    else:
        latency = int(np.random.lognormal(4.8, 0.7))    # ~121ms median
    latency = int(np.clip(latency, 10, 15000))
    is_high_latency = latency > 1000

    http_code = (np.random.choice(HTTP_ERRORS) if is_err
                 else np.random.choice(HTTP_SUCCESS, p=[0.88, 0.09, 0.03]))

    # Event timestamp: offset from session start (events within ~30 min window)
    evt_offset_secs = np.random.exponential(120)   # most events within 2 min of each other
    evt_ts = sess_start + timedelta(seconds=int(evt_offset_secs))
    evt_ts = min(evt_ts, EVENT_END)                # cap at window end

    created_ts = evt_ts
    updated_ts = evt_ts + timedelta(seconds=np.random.randint(0, 5))

    events_data.append({
        "event_id":         f"EVT-{i:06d}",
        "user_id":          uid,
        "session_id":       sid,
        "resource_id":      f"RES-{np.random.randint(1, 2001):05d}",
        "product_type":     prod,
        "subscription_tier": tier,
        "event_type":       etype,
        "event_category":   ecat,
        "event_timestamp":  evt_ts,
        "event_date":       evt_ts.date(),
        "event_hour":       evt_ts.hour,
        "event_month":      evt_ts.strftime("%Y-%m"),
        "feature_name":     np.random.choice(FEATURE_NAMES),
        "page_url":         np.random.choice(PAGE_URLS),
        "referrer":         np.random.choice(PAGE_URLS + ["external", None], p=[0.05]*len(PAGE_URLS) + [0.15, 0.35]),
        "http_status_code": int(http_code),
        "latency_ms":       latency,
        "is_error_event":   bool(is_err),
        "is_high_latency":  bool(is_high_latency),
        "device_type":      user_device_map[uid],
        "browser":          user_browser_map[uid],
        "os":               os_arr[user_ids_list.index(uid)] if uid in user_ids_list else "Unknown",
        "country":          user_country_map[uid],
        "created_timestamp": created_ts,
        "updated_timestamp": updated_ts,
    })

events_pdf = pd.DataFrame(events_data)
print(f"  Event type distribution (top 5):\n{events_pdf['event_type'].value_counts().head().to_string()}")
print(f"  Error events: {events_pdf['is_error_event'].sum():,} ({events_pdf['is_error_event'].mean()*100:.1f}%)")
print(f"  High-latency events: {events_pdf['is_high_latency'].sum():,} ({events_pdf['is_high_latency'].mean()*100:.1f}%)")

# =============================================================================
# 4. SAVE TO VOLUME
# =============================================================================
print(f"\nSaving to {VOLUME_PATH}...")

spark.createDataFrame(users_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/users")
print(f"  Saved users: {len(users_pdf):,} rows")

spark.createDataFrame(orders_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/orders")
print(f"  Saved orders: {len(orders_pdf):,} rows")

spark.createDataFrame(events_pdf).write.mode("overwrite").parquet(f"{VOLUME_PATH}/events")
print(f"  Saved events: {len(events_pdf):,} rows")

# =============================================================================
# 5. VALIDATION
# =============================================================================
print("\n=== VALIDATION ===")
for name in ["users", "orders", "events"]:
    df = spark.read.parquet(f"{VOLUME_PATH}/{name}")
    print(f"  {name}: {df.count():,} rows | {len(df.columns)} columns")

# Revenue summary
print(f"\n  Order revenue by tier:")
orders_pdf.groupby("subscription_tier")["total_value"].agg(["count","sum","mean"]).round(2).apply(
    lambda r: print(f"    {r.name}: {int(r['count']):,} orders | ${r['sum']:,.0f} total | ${r['mean']:.2f} avg")
, axis=1)

print(f"\n  Event window: {events_pdf['event_timestamp'].min()} -> {events_pdf['event_timestamp'].max()}")
print(f"  Order window: {orders_pdf['order_date'].min()} -> {orders_pdf['order_date'].max()}")
print("\n✅ Data generation complete!")
print(f"   Volume: {VOLUME_PATH}")
print(f"   Users:  {VOLUME_PATH}/users")
print(f"   Orders: {VOLUME_PATH}/orders")
print(f"   Events: {VOLUME_PATH}/events")
