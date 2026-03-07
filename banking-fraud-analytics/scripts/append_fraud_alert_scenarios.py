"""
Appends ~65 explicit wire-fraud-after-IP-change alert scenarios to the
existing mock banking datasets so that silver_wire_transfer_ip_alerts
surfaces ~50 confirmed alerts.

Each scenario:
  1. A suspicious login (MFA change, VPN, TOR, or foreign country)
  2. A wire transfer (NEFT/RTGS/IMPS/Net_Banking) > INR 50,000
     occurring 3–55 minutes after that login for the same user_id.

Appended to:
  /Volumes/financial_security/raw_data/mock_banking_data/transactions/
  /Volumes/financial_security/raw_data/mock_banking_data/login_logs/
"""
import json
import random
import hashlib
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

CATALOG     = "financial_security"
SCHEMA      = "raw_data"
VOLUME_NAME = "mock_banking_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

SEED = 99
random.seed(SEED)
np.random.seed(SEED)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# 1. Read existing data to get valid user_ids and current max IDs
# ---------------------------------------------------------------------------
print("Reading existing data...")
existing_txns   = spark.read.json(f"{VOLUME_PATH}/transactions/").toPandas()
existing_logins = spark.read.json(f"{VOLUME_PATH}/login_logs/").toPandas()
users_df        = spark.read.json(f"{VOLUME_PATH}/users/").toPandas()
merchants_df    = spark.read.json(f"{VOLUME_PATH}/merchants/").toPandas()

user_ids     = users_df["user_id"].tolist()
user_map     = users_df.set_index("user_id").to_dict("index")

# Wire-capable merchants (any category is fine; NEFT/RTGS are bank-to-bank)
merchant_ids = merchants_df["merchant_id"].tolist()

# Determine next IDs to avoid collisions
max_txn_num   = existing_txns["transaction_id"].str.extract(r'TXN-(\d+)')[0].astype(int).max()
max_login_num = existing_logins["online_login_id"].str.extract(r'LOG-(\d+)')[0].astype(int).max()
next_txn   = max_txn_num + 1
next_login = max_login_num + 1

print(f"  Existing transactions: {len(existing_txns):,}  (next TXN id: {next_txn})")
print(f"  Existing login logs:   {len(existing_logins):,}  (next LOG id: {next_login})")

# ---------------------------------------------------------------------------
# 2. Define fraud alert scenario parameters
# ---------------------------------------------------------------------------
N_SCENARIOS = 65     # target 65 → expect ~50+ pass the 60-min window filter

# Feb 2026 date range
START = datetime(2026, 2, 2, 0, 0, 0)
END   = datetime(2026, 2, 27, 22, 0, 0)

WIRE_TYPES    = ["NEFT", "RTGS", "IMPS", "Net_Banking"]
RISK_PROFILES = [
    # (mfa_change, is_vpn, is_tor, country_code, login_risk_level)
    (True,  False, False, "IN",  "MEDIUM"),   # MFA change
    (False, True,  False, "IN",  "MEDIUM"),   # VPN
    (False, True,  False, "CN",  "HIGH"),     # VPN + China
    (False, False, True,  "IN",  "HIGH"),     # TOR
    (True,  False, False, "NG",  "HIGH"),     # MFA + Nigeria
    (False, True,  False, "RU",  "HIGH"),     # VPN + Russia
    (False, False, True,  "UA",  "HIGH"),     # TOR + Ukraine
    (True,  True,  False, "IN",  "HIGH"),     # MFA + VPN
    (False, False, False, "US",  "MEDIUM"),   # Foreign country
    (False, False, False, "CN",  "HIGH"),     # China login
]

CITIES = [
    ("Mumbai",    "Maharashtra",    19.076, 72.877),
    ("Delhi",     "Delhi",          28.704, 77.102),
    ("Bengaluru", "Karnataka",      12.972, 77.594),
    ("Hyderabad", "Telangana",      17.385, 78.487),
    ("Chennai",   "Tamil Nadu",     13.083, 80.270),
    ("Pune",      "Maharashtra",    18.520, 73.856),
    ("Ahmedabad", "Gujarat",        23.023, 72.572),
    ("Kolkata",   "West Bengal",    22.572, 88.364),
    ("Jaipur",    "Rajasthan",      26.912, 75.787),
    ("Nagpur",    "Maharashtra",    21.146, 79.088),
]

SUSPICIOUS_IPS = [
    "185.220.101.45",  # TOR exit
    "45.141.152.18",   # known VPN range
    "194.165.16.11",   # suspicious range
    "185.107.47.215",  # VPN
    "23.129.64.218",   # TOR
    "198.98.51.189",   # proxy
]

def rand_ip_suspicious():
    return random.choice(SUSPICIOUS_IPS)

def fmt(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def random_ts_in_range():
    delta = int((END - START).total_seconds())
    return START + timedelta(seconds=random.randint(3600, delta - 7200))

# ---------------------------------------------------------------------------
# 3. Generate scenarios
# ---------------------------------------------------------------------------
print(f"\nGenerating {N_SCENARIOS} fraud alert scenarios...")

new_logins = []
new_txns   = []

# Use a diverse but reproducible set of users
scenario_users = random.sample(user_ids, min(N_SCENARIOS, len(user_ids)))

for i in range(N_SCENARIOS):
    uid          = scenario_users[i]
    user         = user_map[uid]
    risk_profile = RISK_PROFILES[i % len(RISK_PROFILES)]
    mfa_change, is_vpn, is_tor, country_code, risk_level = risk_profile

    city_info  = random.choice(CITIES)
    city, state, lat, lon = city_info

    # Login timestamp
    login_ts = random_ts_in_range()
    # Transaction: 3–55 minutes after login (all within the 60-min alert window)
    delay_mins = random.randint(3, 55)
    txn_ts = login_ts + timedelta(minutes=delay_mins)

    ip_login = rand_ip_suspicious()
    ip_txn   = ip_login if random.random() < 0.4 else f"117.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"

    device_id = "DEV-" + hashlib.md5(f"alert_{i}_{uid}".encode()).hexdigest()[:12].upper()
    merch_id  = random.choice(merchant_ids)
    wire_type = random.choice(WIRE_TYPES)

    # Amount: 55K–2.9L (all above 50K threshold)
    amount = round(random.uniform(55000, 290000), 2)

    # Fraud label on the transaction (75% of alert scenarios are marked as actual fraud)
    is_fraud  = random.random() < 0.75
    fraud_lbl = "Account_Takeover" if is_fraud else None

    login_id = f"LOG-{next_login + i:08d}"
    txn_id   = f"TXN-{next_txn + i:08d}"

    # Login record
    new_logins.append({
        "online_login_id":      login_id,
        "user_id":              uid,
        "device_id":            device_id,
        "ip_address":           ip_login,
        "login_status":         "Success",
        "failed_attempt_num":   0,
        "mfa_change_flag":      mfa_change,
        "session_duration_sec": random.randint(300, 3600),
        "city":                 user["city"],
        "country_code":         country_code,
        "is_vpn":               is_vpn,
        "is_tor":               is_tor,
        "is_new_device":        True,
        "is_suspicious":        True,
        "event_timestamp":      fmt(login_ts),
        "created_timestamp":    fmt(login_ts),
        "updated_timestamp":    fmt(login_ts),
    })

    # Transaction record
    new_txns.append({
        "transaction_id":     txn_id,
        "user_id":            uid,
        "merchant_id":        merch_id,
        "merchant_category":  "Net_Banking" if wire_type == "Net_Banking" else "NEFT/RTGS/IMPS",
        "amount":             amount,
        "currency":           "INR",
        "transaction_type":   wire_type,
        "channel":            random.choice(["Mobile_App", "Internet_Banking"]),
        "transaction_status": "Success",
        "txn_city":           city,
        "txn_state":          state,
        "txn_region":         "West" if state in ("Maharashtra", "Gujarat") else
                              "North" if state in ("Delhi", "Rajasthan") else
                              "South" if state in ("Karnataka", "Tamil Nadu", "Telangana") else "East",
        "txn_lat":            round(lat + np.random.normal(0, 0.05), 5),
        "txn_lon":            round(lon + np.random.normal(0, 0.05), 5),
        "device_id":          device_id,
        "ip_address":         ip_txn,
        "is_international":   False,
        "is_fraud":           is_fraud,
        "fraud_type":         fraud_lbl,
        "event_timestamp":    fmt(txn_ts),
        "created_timestamp":  fmt(txn_ts),
        "updated_timestamp":  fmt(txn_ts),
    })

print(f"  Generated {len(new_logins)} login records and {len(new_txns)} transaction records")

# ---------------------------------------------------------------------------
# 4. Union with existing data and re-save
# ---------------------------------------------------------------------------
print("\nMerging with existing data and re-saving...")

new_logins_pdf = pd.DataFrame(new_logins)
new_txns_pdf   = pd.DataFrame(new_txns)

combined_logins = pd.concat([existing_logins, new_logins_pdf], ignore_index=True)
combined_txns   = pd.concat([existing_txns,   new_txns_pdf],   ignore_index=True)

def save_ndjson(pdf, path):
    spark.createDataFrame(pdf).coalesce(1).write.mode("overwrite").json(path)
    print(f"  Saved {len(pdf):,} records → {path}")

save_ndjson(spark.createDataFrame(combined_logins).toPandas(), f"{VOLUME_PATH}/login_logs")
save_ndjson(spark.createDataFrame(combined_txns).toPandas(),   f"{VOLUME_PATH}/transactions")

# ---------------------------------------------------------------------------
# 5. Summary
# ---------------------------------------------------------------------------
print("\n=== APPEND SUMMARY ===")
print(f"login_logs:   {len(existing_logins):,} + {len(new_logins_pdf)} = {len(combined_logins):,}")
print(f"transactions: {len(existing_txns):,} + {len(new_txns_pdf)} = {len(combined_txns):,}")
print(f"\nScenarios added:")
mfa_cnt = sum(1 for r in new_logins if r["mfa_change_flag"])
vpn_cnt = sum(1 for r in new_logins if r["is_vpn"])
tor_cnt = sum(1 for r in new_logins if r["is_tor"])
fgn_cnt = sum(1 for r in new_logins if r["country_code"] != "IN")
fraud_txn_cnt = sum(1 for r in new_txns if r["is_fraud"])
print(f"  MFA change logins:   {mfa_cnt}")
print(f"  VPN logins:          {vpn_cnt}")
print(f"  TOR logins:          {tor_cnt}")
print(f"  Foreign country:     {fgn_cnt}")
print(f"  is_fraud=true txns:  {fraud_txn_cnt}")
print(f"\nAll scenarios have wire transfer > INR 50,000 within 3–55 min of IP risk login.")
print(f"Re-run the SDP pipeline with --refresh-all to see updated alert count.")
