"""
Generate synthetic Axis Bank-style online banking fraud analytics data.

Datasets:
  users         — 10,000 bank customers
  merchants     — 500 merchants across India
  devices       — device fingerprint registry
  transactions  — 100,000 transactions over Feb 2026
  login_logs    — ~45,000 login events aligned with transaction activity

Fraud patterns embedded:
  1. Account takeover: MFA change followed by large transfer < 30 min
  2. High-velocity burst: 10+ transactions in < 60 min (card testing / fraud)
  3. Geographic anomaly: transaction city differs >500km from user's home city
  4. New device + large amount: first use of device with >INR 50,000 transaction
  5. Suspicious login: VPN/TOR IP followed by transaction same session
  6. Synthetic identity: account opened < 7 days ago with large transaction
  7. Repeated small amounts: card-not-present probing pattern

Storage: /Volumes/financial_security/raw_data/mock_banking_data/
"""
import json
import random
import hashlib
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG       = "financial_security"
SCHEMA        = "raw_data"
VOLUME_NAME   = "mock_banking_data"
VOLUME_PATH   = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

N_USERS        = 10_000
N_MERCHANTS    = 500
N_TRANSACTIONS = 100_000

# Feb 2026 — complete one-month window
START_DATE = datetime(2026, 2, 1, 0, 0, 0)
END_DATE   = datetime(2026, 2, 28, 23, 59, 59)
MONTH_SECS = int((END_DATE - START_DATE).total_seconds())

SEED = 42
np.random.seed(SEED)
random.seed(SEED)
Faker.seed(SEED)
fake = Faker("en_IN")

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# =============================================================================
# GEOGRAPHY
# =============================================================================
CITIES = [
    # (city, state, region, lat, lon, tier)
    ("Mumbai",       "Maharashtra",    "West",  19.076, 72.877,  1),
    ("Delhi",        "Delhi",          "North", 28.704, 77.102,  1),
    ("Bengaluru",    "Karnataka",      "South", 12.972, 77.594,  1),
    ("Hyderabad",    "Telangana",      "South", 17.385, 78.487,  1),
    ("Chennai",      "Tamil Nadu",     "South", 13.083, 80.270,  1),
    ("Kolkata",      "West Bengal",    "East",  22.572, 88.364,  1),
    ("Pune",         "Maharashtra",    "West",  18.520, 73.856,  1),
    ("Ahmedabad",    "Gujarat",        "West",  23.023, 72.572,  1),
    ("Jaipur",       "Rajasthan",      "North", 26.912, 75.787,  2),
    ("Lucknow",      "Uttar Pradesh",  "North", 26.850, 80.949,  2),
    ("Chandigarh",   "Punjab",         "North", 30.733, 76.779,  2),
    ("Surat",        "Gujarat",        "West",  21.170, 72.831,  2),
    ("Nagpur",       "Maharashtra",    "West",  21.146, 79.088,  2),
    ("Indore",       "Madhya Pradesh", "West",  22.719, 75.858,  2),
    ("Bhopal",       "Madhya Pradesh", "West",  23.259, 77.413,  2),
    ("Patna",        "Bihar",          "East",  25.614, 85.144,  2),
    ("Bhubaneswar",  "Odisha",         "East",  20.296, 85.824,  2),
    ("Kochi",        "Kerala",         "South", 9.931,  76.267,  2),
    ("Coimbatore",   "Tamil Nadu",     "South", 11.017, 76.955,  2),
    ("Visakhapatnam","Andhra Pradesh", "South", 17.686, 83.218,  2),
    ("Ranchi",       "Jharkhand",      "East",  23.344, 85.310,  3),
    ("Guwahati",     "Assam",          "East",  26.144, 91.736,  3),
    ("Vadodara",     "Gujarat",        "West",  22.307, 73.181,  2),
    ("Agra",         "Uttar Pradesh",  "North", 27.176, 78.008,  2),
    ("Varanasi",     "Uttar Pradesh",  "North", 25.317, 83.005,  2),
]
city_names   = [c[0] for c in CITIES]
city_weights = np.array([6 if c[5] == 1 else 3 if c[5] == 2 else 1 for c in CITIES], dtype=float)
city_weights /= city_weights.sum()
city_info    = {c[0]: {"state": c[1], "region": c[2], "lat": c[3], "lon": c[4], "tier": c[5]} for c in CITIES}

def pick_city(rng=None):
    return np.random.choice(city_names, p=city_weights)

# =============================================================================
# MERCHANT CATEGORIES
# =============================================================================
MERCHANT_CATS = [
    ("Grocery",           0.20, (100,   5000)),
    ("E-commerce",        0.18, (200,  20000)),
    ("Restaurant",        0.12, (150,   3000)),
    ("Petrol/Fuel",       0.10, (500,   5000)),
    ("Healthcare",        0.08, (200,  15000)),
    ("Utilities",         0.07, (500,   8000)),
    ("Travel",            0.06, (500,  50000)),
    ("Education",         0.05, (500,  25000)),
    ("Electronics",       0.04, (2000, 80000)),
    ("Entertainment",     0.03, (100,   2000)),
    ("Jewelry",           0.02, (5000,200000)),   # high-risk
    ("Forex/Remittance",  0.02, (5000,100000)),   # high-risk
    ("Clothing",          0.02, (500,  15000)),
    ("Insurance",         0.01, (1000, 50000)),
]
cat_names   = [c[0] for c in MERCHANT_CATS]
cat_weights = np.array([c[1] for c in MERCHANT_CATS])
cat_weights /= cat_weights.sum()

# =============================================================================
# TRANSACTION TYPES & CHANNELS
# =============================================================================
TXN_TYPES   = ["UPI", "IMPS", "NEFT", "RTGS", "Debit_Card", "Credit_Card", "Net_Banking"]
TXN_WEIGHTS = [0.38, 0.20, 0.12, 0.05, 0.12, 0.10, 0.03]

CHANNELS     = ["Mobile_App", "Internet_Banking", "ATM", "POS", "WhatsApp_Pay"]
CHAN_WEIGHTS  = [0.45,         0.20,               0.12, 0.18,  0.05]

# =============================================================================
# HELPERS
# =============================================================================
def random_ts(start=START_DATE, end=END_DATE):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def fmt_ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def rand_ip(country="IN", suspicious=False):
    if suspicious and random.random() < 0.5:
        # Known suspicious ranges (fictional)
        return f"185.{random.randint(100,200)}.{random.randint(0,255)}.{random.randint(1,254)}"
    # Indian ISP-like ranges
    prefixes = ["49.", "103.", "117.", "122.", "125.", "182.", "223.", "27."]
    p = random.choice(prefixes)
    parts = p.split(".")
    while len(parts) < 4:
        parts.append(str(random.randint(0, 255)))
    return ".".join(parts[:4])

def rand_device_id():
    return "DEV-" + hashlib.md5(str(random.random()).encode()).hexdigest()[:12].upper()

# =============================================================================
# DATASET 1: USERS
# =============================================================================
print("Generating users...")

FIRST_NAMES = ["Rahul","Priya","Amit","Sunita","Vikram","Anjali","Rohan","Pooja",
                "Arjun","Divya","Suresh","Meena","Nitin","Rekha","Kiran","Kavita",
                "Rajesh","Swati","Anil","Neha","Deepak","Anita","Vijay","Smita",
                "Manish","Ritu","Sanjay","Geeta","Ashok","Shruti","Ramesh","Pallavi"]
LAST_NAMES  = ["Sharma","Patel","Singh","Kumar","Gupta","Joshi","Mishra","Rao",
                "Nair","Reddy","Iyer","Shah","Mehta","Desai","Verma","Yadav",
                "Tiwari","Pandey","Banerjee","Chatterjee","Das","Sen","Pillai","Menon"]
ACCT_TYPES  = ["Savings", "Current", "Salary", "NRE"]
ACCT_WGHTS  = [0.65,      0.15,      0.15,     0.05]

users_data = []
# Pre-mark ~1.5% as fraud-linked accounts
n_fraud_users = int(N_USERS * 0.015)
fraud_user_idx = set(random.sample(range(N_USERS), n_fraud_users))

for i in range(N_USERS):
    city    = pick_city()
    ci      = city_info[city]
    fname   = random.choice(FIRST_NAMES)
    lname   = random.choice(LAST_NAMES)
    name    = f"{fname} {lname}"
    is_new  = random.random() < 0.03   # 3% new accounts (<7 days old)
    reg_ts  = random_ts(START_DATE - timedelta(days=365), START_DATE - timedelta(days=7 if not is_new else 0))
    if is_new:
        reg_ts = random_ts(START_DATE - timedelta(days=6), START_DATE + timedelta(days=5))
    balance = int(np.random.lognormal(np.log(50000), 0.9))
    balance = max(1000, min(10_000_000, balance))

    users_data.append({
        "user_id":           f"USR-{i:06d}",
        "full_name":         name,
        "email":             f"{fname.lower()}.{lname.lower()}{random.randint(1,999)}@{'gmail' if random.random()<0.6 else 'yahoo'}.com",
        "mobile":            f"{'6789'[random.randint(0,3)]}{random.randint(100000000,999999999)}",
        "city":              city,
        "state":             ci["state"],
        "region":            ci["region"],
        "account_type":      np.random.choice(ACCT_TYPES, p=ACCT_WGHTS),
        "account_balance":   balance,
        "kyc_status":        "Verified" if random.random() < 0.95 else "Pending",
        "credit_score":      int(np.random.beta(5, 2) * 600 + 300),
        "is_new_account":    is_new,
        "is_fraud_linked":   i in fraud_user_idx,
        "registration_date": reg_ts.strftime("%Y-%m-%d"),
        "created_timestamp": fmt_ts(reg_ts),
        "updated_timestamp": fmt_ts(reg_ts),
    })

users_pdf = pd.DataFrame(users_data)
user_ids  = users_pdf["user_id"].tolist()
user_map  = users_pdf.set_index("user_id").to_dict("index")
fraud_user_ids = set(users_pdf[users_pdf["is_fraud_linked"]]["user_id"])
new_user_ids   = set(users_pdf[users_pdf["is_new_account"]]["user_id"])
print(f"  Created {len(users_pdf):,} users ({len(fraud_user_ids)} fraud-linked, {len(new_user_ids)} new accounts)")

# =============================================================================
# DATASET 2: MERCHANTS
# =============================================================================
print("Generating merchants...")

MERCHANT_BRANDS = {
    "Grocery":          ["BigBazaar","DMart","Reliance_Fresh","Spencer","More_Supermart"],
    "E-commerce":       ["Flipkart","Amazon_IN","Myntra","Meesho","Snapdeal","Nykaa"],
    "Restaurant":       ["Zomato","Swiggy","McDonald's","Domino's","KFC","Burger_King"],
    "Petrol/Fuel":      ["HPCL","BPCL","IndianOil","Reliance_Petrol","Shell"],
    "Healthcare":       ["Apollo","Fortis","Practo","PharmEasy","1mg","Netmeds"],
    "Utilities":        ["BESCOM","MSEDCL","BSES","Tata_Power","Airtel","Jio","BSNL"],
    "Travel":           ["IRCTC","MakeMyTrip","Goibibo","Indigo","Air_India","OLA","Uber"],
    "Education":        ["BYJU's","Unacademy","Coursera","Vedantu","Allen"],
    "Electronics":      ["Croma","Vijay_Sales","Samsung_IN","Apple_Authorised","OnePlus"],
    "Entertainment":    ["BookMyShow","Netflix","Hotstar","Amazon_Prime","SonyLIV"],
    "Jewelry":          ["Tanishq","Malabar_Gold","Kalyan_Jewellers","PC_Jeweller"],
    "Forex/Remittance": ["Western_Union","MoneyGram","BookMyForex","Wise","Thomas_Cook"],
    "Clothing":         ["H&M","Zara","Pantaloons","Max_Fashion","FBB"],
    "Insurance":        ["LIC","HDFC_Life","SBI_Life","ICICI_Prudential","Bajaj_Allianz"],
}

merchants_data = []
for i in range(N_MERCHANTS):
    cat     = np.random.choice(cat_names, p=cat_weights)
    brands  = MERCHANT_BRANDS.get(cat, [f"{cat}_Merchant"])
    city    = pick_city()
    ci      = city_info[city]
    ts      = fmt_ts(START_DATE - timedelta(days=random.randint(30, 1000)))
    merchants_data.append({
        "merchant_id":       f"MER-{i:05d}",
        "merchant_name":     random.choice(brands) + f"_{city[:3].upper()}_{i:03d}",
        "merchant_category": cat,
        "city":              city,
        "state":             ci["state"],
        "region":            ci["region"],
        "is_high_risk":      cat in ("Jewelry", "Forex/Remittance"),
        "created_timestamp": ts,
        "updated_timestamp": ts,
    })

merchants_pdf = pd.DataFrame(merchants_data)
merchant_ids  = merchants_pdf["merchant_id"].tolist()
merch_map     = merchants_pdf.set_index("merchant_id").to_dict("index")
hv_merchant_ids = set(merchants_pdf[merchants_pdf["is_high_risk"]]["merchant_id"])
print(f"  Created {len(merchants_pdf):,} merchants ({len(hv_merchant_ids)} high-risk)")

# =============================================================================
# DATASET 3: DEVICES
# =============================================================================
print("Generating device registry...")

# 2-4 devices per user on average
device_data   = []
user_devices  = {}   # user_id → [device_id, ...]

for uid in user_ids:
    n_dev = np.random.choice([1, 2, 3, 4], p=[0.40, 0.35, 0.18, 0.07])
    devs  = []
    for j in range(n_dev):
        did = rand_device_id()
        os  = np.random.choice(["Android", "iOS", "Windows", "macOS"], p=[0.55, 0.30, 0.10, 0.05])
        ts  = fmt_ts(random_ts(START_DATE - timedelta(days=180), START_DATE))
        device_data.append({
            "device_id":       did,
            "user_id":         uid,
            "device_type":     np.random.choice(["Mobile", "Tablet", "Desktop"], p=[0.72, 0.08, 0.20]),
            "os":              os,
            "app_version":     f"6.{random.randint(0,5)}.{random.randint(0,12)}",
            "is_trusted":      j == 0,      # first device is trusted
            "registered_at":   ts,
            "created_timestamp": ts,
            "updated_timestamp": ts,
        })
        devs.append(did)
    user_devices[uid] = devs

devices_pdf = pd.DataFrame(device_data)
print(f"  Created {len(devices_pdf):,} device records")

# =============================================================================
# DATASET 4: TRANSACTIONS  (100,000)
# =============================================================================
print("Generating transactions (100,000)...")

# --- Fraud scenario assignments ---
# S1: high-velocity bursts (~2% of users, 15-40 txns in <90 min)
hv_users   = set(random.sample(user_ids, int(N_USERS * 0.02)))
# S2: geo-anomaly users (~1% of users)
geo_users  = set(random.sample(user_ids, int(N_USERS * 0.01)))
# S3: card-not-present probing (~1.5% — repeated small e-com txns)
cnp_users  = set(random.sample(user_ids, int(N_USERS * 0.015)))

# Build distribution of transactions per user (Pareto: power users dominate)
pareto_weights = (np.random.pareto(1.5, N_USERS) + 1)
pareto_weights /= pareto_weights.sum()

# Assign each transaction to a user
txn_user_ids = np.random.choice(user_ids, size=N_TRANSACTIONS, p=pareto_weights)

# Base timestamps: distributed across Feb 2026
# Add realistic intra-day pattern (peak 10-12, 18-21; low 2-6)
def sample_txn_time():
    day  = random.randint(0, 27)
    hour_probs = np.array([0.005,0.003,0.002,0.002,0.003,0.005,
                            0.015,0.025,0.035,0.045,0.060,0.065,
                            0.055,0.045,0.040,0.035,0.045,0.065,
                            0.075,0.080,0.070,0.055,0.030,0.015])
    hour_probs /= hour_probs.sum()
    hour = np.random.choice(24, p=hour_probs)
    mins = random.randint(0, 59)
    secs = random.randint(0, 59)
    return START_DATE + timedelta(days=day, hours=hour, minutes=mins, seconds=secs)

# Pre-generate burst windows for hv_users
burst_windows = {}
for uid in hv_users:
    anchor = sample_txn_time()
    n_burst = random.randint(12, 35)
    burst_windows[uid] = (anchor, n_burst)

txns_data    = []
burst_counts = {}   # uid → burst txns assigned

n_fraud = int(N_TRANSACTIONS * 0.04)  # 4% fraud rate
fraud_indices = set(random.sample(range(N_TRANSACTIONS), n_fraud))

for idx in range(N_TRANSACTIONS):
    uid  = txn_user_ids[idx]
    user = user_map[uid]
    home_city = user["city"]
    is_fraud  = idx in fraud_indices

    # --- Timestamp ---
    if uid in hv_users and burst_counts.get(uid, 0) < burst_windows[uid][1]:
        anchor, _ = burst_windows[uid]
        txn_ts = anchor + timedelta(seconds=random.randint(10, 5400))
        burst_counts[uid] = burst_counts.get(uid, 0) + 1
    else:
        txn_ts = sample_txn_time()

    # --- Merchant ---
    if uid in cnp_users and is_fraud:
        # card probing: E-commerce, small amounts
        merch_id = random.choice([m for m in merchant_ids if merch_map[m]["merchant_category"] == "E-commerce"])
    elif is_fraud and random.random() < 0.4:
        merch_id = random.choice(list(hv_merchant_ids)) if hv_merchant_ids else random.choice(merchant_ids)
    else:
        merch_id = np.random.choice(merchant_ids)

    cat        = merch_map[merch_id]["merchant_category"]
    amt_min, amt_max = next(c[2] for c in MERCHANT_CATS if c[0] == cat)

    # Amount: fraud txns skew toward higher values (account takeover) or very small (probing)
    if is_fraud and uid in cnp_users:
        amount = round(random.uniform(1, 499), 2)   # sub-500 probing
    elif is_fraud:
        amount = round(np.random.lognormal(np.log(max(amt_min, 10000)), 0.5), 2)
        amount = min(amount, 500000)
    else:
        amount = round(np.random.lognormal(np.log((amt_min + amt_max) / 2), 0.7), 2)
        amount = max(amt_min, min(amt_max * 1.5, amount))

    # --- Transaction city ---
    if uid in geo_users and random.random() < 0.6:
        # geo-anomaly: pick a city far from home
        far_cities = [c for c in city_names if c != home_city]
        txn_city   = random.choice(far_cities)
    else:
        # mostly home city with occasional nearby
        if random.random() < 0.80:
            txn_city = home_city
        else:
            txn_city = pick_city()

    txn_ci = city_info[txn_city]
    device_id  = random.choice(user_devices.get(uid, ["DEV-UNKNOWN"]))
    txn_type   = np.random.choice(TXN_TYPES, p=TXN_WEIGHTS)
    channel    = np.random.choice(CHANNELS, p=CHAN_WEIGHTS)
    ip_addr    = rand_ip(suspicious=(is_fraud and random.random() < 0.3))
    status     = "Failed" if (is_fraud and random.random() < 0.15) else \
                 "Reversed" if random.random() < 0.02 else "Success"

    # Fraud type label
    if is_fraud:
        if uid in hv_users:
            fraud_label = "High_Velocity"
        elif uid in geo_users:
            fraud_label = "Geographic_Anomaly"
        elif uid in cnp_users:
            fraud_label = "Card_Not_Present_Probe"
        elif uid in new_user_ids:
            fraud_label = "New_Account_Fraud"
        elif uid in fraud_user_ids:
            fraud_label = "Account_Takeover"
        else:
            fraud_label = np.random.choice(["Merchant_Fraud","Identity_Theft","SIM_Swap"])
    else:
        fraud_label = None

    txns_data.append({
        "transaction_id":     f"TXN-{idx:08d}",
        "user_id":            uid,
        "merchant_id":        merch_id,
        "merchant_category":  cat,
        "amount":             round(amount, 2),
        "currency":           "INR",
        "transaction_type":   txn_type,
        "channel":            channel,
        "transaction_status": status,
        "txn_city":           txn_city,
        "txn_state":          txn_ci["state"],
        "txn_region":         txn_ci["region"],
        "txn_lat":            round(txn_ci["lat"] + np.random.normal(0, 0.05), 5),
        "txn_lon":            round(txn_ci["lon"] + np.random.normal(0, 0.05), 5),
        "device_id":          device_id,
        "ip_address":         ip_addr,
        "is_international":   False,
        "is_fraud":           is_fraud,
        "fraud_type":         fraud_label,
        "event_timestamp":    fmt_ts(txn_ts),
        "created_timestamp":  fmt_ts(txn_ts),
        "updated_timestamp":  fmt_ts(txn_ts),
    })

txns_pdf = pd.DataFrame(txns_data)
print(f"  Created {len(txns_pdf):,} transactions ({txns_pdf['is_fraud'].sum():,} fraud, {(txns_pdf['transaction_status']=='Failed').sum():,} failed)")

# =============================================================================
# DATASET 5: LOGIN LOGS  (~40,000)
# =============================================================================
print("Generating login logs...")

# Add temporary columns for grouping
txns_pdf["event_date"] = pd.to_datetime(txns_pdf["event_timestamp"]).dt.date
txns_pdf["event_hour"] = pd.to_datetime(txns_pdf["event_timestamp"]).dt.hour

# One login per unique (user_id, event_date) — take the earliest txn of each day as the anchor
txns_sorted = txns_pdf.sort_values("event_timestamp")
first_per_day = (
    txns_sorted
    .groupby(["user_id", "event_date"], sort=False)
    .first()
    .reset_index()[["user_id", "event_date", "event_timestamp", "device_id", "ip_address"]]
)
n_logins = len(first_per_day)
print(f"  Building {n_logins:,} successful login records (1 per user-day)...")

# Vectorized suspicious flags
is_susp_arr = first_per_day["user_id"].isin(fraud_user_ids | hv_users).values
susp_roll   = np.random.random(n_logins)        # draw once
mfa_roll    = np.random.random(n_logins)
vpn_roll    = np.random.random(n_logins)
tor_roll    = np.random.random(n_logins)
cc_indices  = np.random.choice(6, size=n_logins)
cc_options  = ["IN","US","CN","NG","RU","UA"]
cc_probs    = [0.40, 0.20, 0.15, 0.10, 0.10, 0.05]
cc_cum      = np.cumsum(cc_probs)

def pick_country(roll):
    for i, threshold in enumerate(cc_cum):
        if roll < threshold:
            return cc_options[i]
    return "IN"

login_rows = []
for i in range(n_logins):
    row        = first_per_day.iloc[i]
    uid        = row["user_id"]
    anchor_str = row["event_timestamp"]
    device_id  = row["device_id"]
    ip_addr    = row["ip_address"]
    user       = user_map[uid]

    # Parse anchor timestamp
    try:
        anchor_ts = datetime.strptime(anchor_str, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        anchor_ts = START_DATE

    login_ts   = anchor_ts - timedelta(minutes=random.randint(2, 15))
    if login_ts < START_DATE:
        login_ts = anchor_ts

    is_susp    = bool(is_susp_arr[i])
    mfa_change = False
    is_vpn     = False
    is_tor     = False
    country    = "IN"
    susp_flag  = False

    if is_susp and susp_roll[i] < 0.20:
        ip_addr    = rand_ip(suspicious=True)
        mfa_change = mfa_roll[i] < 0.35
        is_vpn     = vpn_roll[i] < 0.40
        is_tor     = tor_roll[i] < 0.10
        country    = pick_country(np.random.random())
        susp_flag  = is_vpn or is_tor or mfa_change or country != "IN"

    session_sec = random.randint(120, 3600)

    login_rows.append({
        "online_login_id":      f"LOG-{i:08d}",
        "user_id":              uid,
        "device_id":            device_id,
        "ip_address":           ip_addr,
        "login_status":         "Success",
        "failed_attempt_num":   0,
        "mfa_change_flag":      mfa_change,
        "session_duration_sec": session_sec,
        "city":                 user["city"],
        "country_code":         country,
        "is_vpn":               is_vpn,
        "is_tor":               is_tor,
        "is_new_device":        random.random() < 0.04,
        "is_suspicious":        susp_flag,
        "event_timestamp":      fmt_ts(login_ts),
        "created_timestamp":    fmt_ts(login_ts),
        "updated_timestamp":    fmt_ts(login_ts),
    })

# Generate failed login attempts — ~8% of unique user-days get 1-4 failed attempts before success
n_failed_sessions = int(n_logins * 0.08)
failed_session_indices = random.sample(range(n_logins), n_failed_sessions)
fail_counter = n_logins

for idx in failed_session_indices:
    row    = first_per_day.iloc[idx]
    uid    = row["user_id"]
    user   = user_map[uid]
    is_s   = bool(is_susp_arr[idx])

    try:
        anchor_ts = datetime.strptime(row["event_timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        anchor_ts = START_DATE

    n_fails = random.randint(1, 4)
    for fa in range(n_fails):
        fail_ts = anchor_ts - timedelta(minutes=random.randint(1, 30) * (n_fails - fa))
        if fail_ts < START_DATE:
            fail_ts = anchor_ts
        login_rows.append({
            "online_login_id":      f"LOG-{fail_counter:08d}",
            "user_id":              uid,
            "device_id":            row["device_id"],
            "ip_address":           rand_ip(suspicious=is_s),
            "login_status":         "Failed",
            "failed_attempt_num":   fa + 1,
            "mfa_change_flag":      False,
            "session_duration_sec": 0,
            "city":                 user["city"],
            "country_code":         "IN",
            "is_vpn":               False,
            "is_tor":               False,
            "is_new_device":        False,
            "is_suspicious":        is_s,
            "event_timestamp":      fmt_ts(fail_ts),
            "created_timestamp":    fmt_ts(fail_ts),
            "updated_timestamp":    fmt_ts(fail_ts),
        })
        fail_counter += 1

login_pdf = pd.DataFrame(login_rows)
print(f"  Created {len(login_pdf):,} login log records")
print(f"  Suspicious logins: {login_pdf['is_suspicious'].sum():,}")
print(f"  MFA changes:       {login_pdf['mfa_change_flag'].sum():,}")
print(f"  Failed logins:     {(login_pdf['login_status']=='Failed').sum():,}")

# Clean up temp columns before saving
txns_pdf_out = txns_pdf.drop(columns=["event_date", "event_hour"])

# =============================================================================
# SAVE TO VOLUME AS NDJSON
# =============================================================================
print(f"\nSaving all datasets to {VOLUME_PATH}...")

def save_ndjson(pdf, path):
    spark.createDataFrame(pdf).coalesce(1).write.mode("overwrite").json(path)
    print(f"  Saved {len(pdf):,} records → {path}")

save_ndjson(users_pdf,      f"{VOLUME_PATH}/users")
save_ndjson(merchants_pdf,  f"{VOLUME_PATH}/merchants")
save_ndjson(devices_pdf,    f"{VOLUME_PATH}/devices")
save_ndjson(txns_pdf_out,   f"{VOLUME_PATH}/transactions")
save_ndjson(login_pdf,      f"{VOLUME_PATH}/login_logs")

# =============================================================================
# VALIDATION SUMMARY
# =============================================================================
print("\n=== DATA GENERATION SUMMARY ===")
print(f"{'Dataset':<20} {'Records':>10}")
print("-" * 32)
for name, df in [("users", users_pdf), ("merchants", merchants_pdf),
                  ("devices", devices_pdf), ("transactions", txns_pdf_out),
                  ("login_logs", login_pdf)]:
    print(f"{name:<20} {len(df):>10,}")

print(f"\n--- Transaction Stats ---")
print(f"Fraud txns:          {txns_pdf_out['is_fraud'].sum():,} / {len(txns_pdf_out):,} ({txns_pdf_out['is_fraud'].mean()*100:.1f}%)")
print(f"Fraud types:         {txns_pdf_out[txns_pdf_out['is_fraud']]['fraud_type'].value_counts().to_dict()}")
print(f"Avg amount:          INR {txns_pdf_out['amount'].mean():,.0f}")
print(f"Total volume:        INR {txns_pdf_out['amount'].sum():,.0f}")
print(f"\n--- By Channel ---")
print(txns_pdf_out["channel"].value_counts().to_string())
print(f"\n--- By Txn Type ---")
print(txns_pdf_out["transaction_type"].value_counts().to_string())
print(f"\n--- City Distribution (top 5) ---")
print(txns_pdf_out["txn_city"].value_counts().head(5).to_string())
print(f"\nAll data saved to: {VOLUME_PATH}")
