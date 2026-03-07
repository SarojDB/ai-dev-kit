"""
Generate synthetic 2-wheeler loan fraud analytics data for Hero FinCorp-style demo.
Datasets: products, dealers, customers, loans, emi_payments, kyc_events
Storage: /Volumes/aibuilder_fraud_analytics/raw_data/fraud_analytics_raw_data/
"""
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
import random
import hashlib

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "aibuilder_fraud_analytics"
SCHEMA = "raw_data"
VOLUME_NAME = "fraud_analytics_raw_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

N_CUSTOMERS = 10000
N_LOANS = 5000
N_PRODUCTS = 50
N_DEALERS = 200
FRAUD_RATE = 0.08  # 8% fraud rate on loans

# 12-month window ending today
END_DATE = datetime(2026, 3, 1)
START_DATE = END_DATE - timedelta(days=365)

# Festival season peaks (Oct-Nov)
FESTIVAL_START = datetime(2025, 10, 1)
FESTIVAL_END = datetime(2025, 11, 30)

SEED = 42
np.random.seed(SEED)
random.seed(SEED)
Faker.seed(SEED)
fake = Faker("en_IN")

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# =============================================================================
# INFRASTRUCTURE
# =============================================================================
print("Creating schema and volume if needed...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
print(f"Volume path: {VOLUME_PATH}")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def random_pan():
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return (
        random.choice(letters) + random.choice(letters) + random.choice(letters)
        + random.choice("ABCFGHLJPT")  # 4th char: entity type
        + random.choice(letters)
        + str(random.randint(1000, 9999))
        + random.choice(letters)
    )

def random_aadhaar_last4():
    return str(random.randint(1000, 9999))

def random_mobile():
    prefixes = ["6", "7", "8", "9"]
    return random.choice(prefixes) + str(random.randint(100000000, 999999999))

def synthetic_email(name):
    parts = name.lower().split()
    domains = ["gmail.com", "yahoo.co.in", "rediffmail.com", "outlook.com", "hotmail.com"]
    username = ".".join(parts[:2]) if len(parts) > 1 else parts[0]
    username = username.replace("'", "").replace(",", "") + str(random.randint(1, 999))
    return f"{username}@{random.choice(domains)}"

def random_timestamp(start, end):
    delta = end - start
    secs = random.randint(0, int(delta.total_seconds()))
    return (start + timedelta(seconds=secs)).strftime("%Y-%m-%dT%H:%M:%SZ")

def random_date(start, end):
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

# =============================================================================
# INDIAN GEOGRAPHY
# =============================================================================
REGIONS = {
    "North": {
        "states": ["Uttar Pradesh", "Delhi", "Haryana", "Punjab", "Rajasthan", "Himachal Pradesh", "Uttarakhand", "Jammu & Kashmir"],
        "cities": [
            ("Delhi", "110001"), ("Lucknow", "226001"), ("Jaipur", "302001"),
            ("Chandigarh", "160001"), ("Agra", "282001"), ("Meerut", "250001"),
            ("Varanasi", "221001"), ("Allahabad", "211001"), ("Ghaziabad", "201001"),
            ("Noida", "201301"), ("Gurugram", "122001"), ("Faridabad", "121001"),
            ("Amritsar", "143001"), ("Ludhiana", "141001"), ("Dehradun", "248001"),
        ],
        "weight": 0.35
    },
    "South": {
        "states": ["Karnataka", "Tamil Nadu", "Andhra Pradesh", "Telangana", "Kerala"],
        "cities": [
            ("Bengaluru", "560001"), ("Chennai", "600001"), ("Hyderabad", "500001"),
            ("Kochi", "682001"), ("Mysuru", "570001"), ("Coimbatore", "641001"),
            ("Madurai", "625001"), ("Visakhapatnam", "530001"), ("Vijayawada", "520001"),
            ("Thiruvananthapuram", "695001"), ("Mangaluru", "575001"), ("Tiruppur", "641601"),
        ],
        "weight": 0.28
    },
    "West": {
        "states": ["Maharashtra", "Gujarat", "Goa", "Madhya Pradesh"],
        "cities": [
            ("Mumbai", "400001"), ("Pune", "411001"), ("Ahmedabad", "380001"),
            ("Surat", "395001"), ("Nashik", "422001"), ("Nagpur", "440001"),
            ("Vadodara", "390001"), ("Rajkot", "360001"), ("Thane", "400601"),
            ("Indore", "452001"), ("Bhopal", "462001"), ("Aurangabad", "431001"),
        ],
        "weight": 0.22
    },
    "East": {
        "states": ["West Bengal", "Bihar", "Odisha", "Jharkhand", "Assam"],
        "cities": [
            ("Kolkata", "700001"), ("Patna", "800001"), ("Bhubaneswar", "751001"),
            ("Ranchi", "834001"), ("Guwahati", "781001"), ("Jamshedpur", "831001"),
            ("Dhanbad", "826001"), ("Cuttack", "753001"), ("Siliguri", "734001"),
        ],
        "weight": 0.15
    }
}

def pick_location(region=None):
    if region is None:
        region = np.random.choice(
            list(REGIONS.keys()),
            p=[v["weight"] for v in REGIONS.values()]
        )
    r = REGIONS[region]
    state = random.choice(r["states"])
    city, pincode = random.choice(r["cities"])
    return region, state, city, pincode

# =============================================================================
# DATASET 1: PRODUCTS
# =============================================================================
print("\nGenerating products...")

BRANDS = [
    ("Hero MotoCorp", ["Splendor Plus", "HF Deluxe", "Passion Pro", "Glamour", "Xtreme 160R", "Xpulse 200"], "Petrol"),
    ("Honda Motorcycle", ["Shine", "Unicorn", "SP 125", "Hornet 2.0", "CB300R", "Activa 6G"], "Petrol"),
    ("TVS Motor", ["Apache RTR 160", "Star City+", "Jupiter", "Ntorq 125", "Raider 125", "iQube Electric"], "Mixed"),
    ("Bajaj Auto", ["Pulsar 125", "Pulsar 150", "Pulsar NS200", "Avenger Street 160", "Dominar 400", "Platina 110"], "Petrol"),
    ("Yamaha", ["FZ-S V3", "MT-15 V2", "R15 V4", "Fascino 125", "Ray-ZR 125", "FZX"], "Petrol"),
    ("Royal Enfield", ["Classic 350", "Meteor 350", "Bullet 350", "Hunter 350", "Thunderbird X350", "Himalayan"], "Petrol"),
    ("Suzuki", ["Gixxer 150", "Gixxer SF 250", "Access 125", "Burgman Street", "V-Strom SX"], "Petrol"),
    ("Ather Energy", ["450X", "450 Plus", "450S"], "Electric"),
    ("Ola Electric", ["S1 Pro", "S1 Air", "S1 X"], "Electric"),
    ("Revolt", ["RV400", "RV300"], "Electric"),
]

PRODUCT_TYPES = {
    "Commuter": (55000, 90000),
    "Sports": (90000, 200000),
    "Scooter": (65000, 130000),
    "Electric": (80000, 150000),
    "Cruiser": (150000, 350000),
    "Adventure": (130000, 250000),
}

INSURANCE_PARTNERS = ["Bajaj Allianz", "ICICI Lombard", "HDFC ERGO", "New India Assurance", "Tata AIG", "SBI General"]

products_data = []
prod_idx = 1
for brand, models, fuel in BRANDS:
    for model in models:
        if prod_idx > N_PRODUCTS:
            break
        if fuel == "Electric":
            ptype = "Electric"
        elif "Cruiser" in model or "Avenger" in model or "Thunderbird" in model:
            ptype = "Cruiser"
        elif "Himalayan" in model or "V-Strom" in model or "Xpulse" in model or "Dominar" in model:
            ptype = "Adventure"
        elif any(x in model for x in ["Activa", "Jupiter", "Ntorq", "Fascino", "Ray-ZR", "Access", "Burgman"]):
            ptype = "Scooter"
        elif any(x in model for x in ["Apache", "Pulsar NS", "Gixxer SF", "R15", "MT-15", "Hornet", "CB300"]):
            ptype = "Sports"
        else:
            ptype = "Commuter"

        price_min, price_max = PRODUCT_TYPES[ptype]
        ex_showroom = int(np.random.uniform(price_min, price_max) // 1000) * 1000
        on_road_min = int(ex_showroom * 1.10)
        on_road_max = int(ex_showroom * 1.18)
        insurance_premium = int(np.random.uniform(0.045, 0.065) * ex_showroom)

        ts = "2024-01-01T00:00:00Z"
        products_data.append({
            "product_id": f"PROD-{prod_idx:03d}",
            "brand": brand,
            "model": model,
            "product_type": ptype,
            "fuel_type": fuel if fuel != "Mixed" else random.choice(["Petrol", "Electric"]),
            "engine_cc": 0 if fuel == "Electric" else random.choice([100, 110, 125, 150, 160, 200, 250, 350, 400]),
            "ex_showroom_price": ex_showroom,
            "on_road_price_min": on_road_min,
            "on_road_price_max": on_road_max,
            "is_electric": fuel == "Electric",
            "insurance_type": random.choice(["Comprehensive", "Third-Party"]),
            "insurance_premium_annual": insurance_premium,
            "insurance_partner": random.choice(INSURANCE_PARTNERS),
            "created_timestamp": ts,
            "updated_timestamp": ts,
        })
        prod_idx += 1

products_pdf = pd.DataFrame(products_data)
print(f"  Created {len(products_pdf):,} products")

# =============================================================================
# DATASET 2: DEALERS
# =============================================================================
print("Generating dealers...")

DEALER_SUFFIXES = ["Auto Works", "Motors", "Automobiles", "Bikes", "Two Wheelers", "Moto Hub", "Auto Plaza", "Vehicle World"]
CITY_TIERS = {
    "Tier1": ["Delhi", "Mumbai", "Bengaluru", "Chennai", "Hyderabad", "Kolkata", "Ahmedabad", "Pune"],
    "Tier2": ["Lucknow", "Jaipur", "Chandigarh", "Kochi", "Coimbatore", "Nagpur", "Indore", "Bhopal", "Patna", "Bhubaneswar"],
}

dealers_data = []
dealer_brand_pool = [b for b, _, _ in BRANDS for _ in range(3)]  # weighted toward all brands
for i in range(1, N_DEALERS + 1):
    region = np.random.choice(list(REGIONS.keys()), p=[v["weight"] for v in REGIONS.values()])
    _, state, city, pincode = pick_location(region)
    brand = random.choice(dealer_brand_pool)

    if city in CITY_TIERS["Tier1"]:
        tier = "Tier1"
    elif city in CITY_TIERS["Tier2"]:
        tier = "Tier2"
    else:
        tier = "Tier3"

    # Some rogue dealers (used in fraud signals)
    is_flagged = random.random() < 0.05  # 5% flagged dealers

    ts = "2024-01-01T00:00:00Z"
    dealers_data.append({
        "dealer_id": f"DLR-{i:04d}",
        "dealer_name": f"{fake.last_name()} {random.choice(DEALER_SUFFIXES)}",
        "brand": brand,
        "city": city,
        "state": state,
        "region": region,
        "pincode": pincode,
        "is_authorized": not is_flagged,
        "dealer_tier": tier,
        "is_flagged": is_flagged,
        "created_timestamp": ts,
        "updated_timestamp": ts,
    })

dealers_pdf = pd.DataFrame(dealers_data)
print(f"  Created {len(dealers_pdf):,} dealers")

# =============================================================================
# DATASET 3: CUSTOMERS
# =============================================================================
print("Generating customers...")

INDIAN_FIRST_NAMES_M = [
    "Rajesh", "Amit", "Suresh", "Vikram", "Anil", "Rohit", "Sandeep", "Pradeep",
    "Mahesh", "Rakesh", "Vijay", "Deepak", "Sanjay", "Naveen", "Kiran", "Mohan",
    "Ramesh", "Ganesh", "Sunil", "Ashok", "Pawan", "Sachin", "Nitin", "Ajay",
    "Ravi", "Vinod", "Manoj", "Yogesh", "Harish", "Girish", "Dinesh", "Umesh",
]
INDIAN_FIRST_NAMES_F = [
    "Priya", "Sunita", "Rekha", "Kavitha", "Anita", "Meena", "Pooja", "Neha",
    "Divya", "Anjali", "Swati", "Sapna", "Aarti", "Shweta", "Nisha", "Ritu",
    "Sneha", "Deepa", "Manisha", "Komal", "Sonal", "Preeti", "Geeta", "Sushma",
]
INDIAN_LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Singh", "Kumar", "Gupta", "Joshi", "Mishra",
    "Yadav", "Tiwari", "Pandey", "Chauhan", "Rao", "Nair", "Reddy", "Pillai",
    "Iyer", "Menon", "Das", "Chatterjee", "Banerjee", "Mukherjee", "Bose", "Sen",
    "Shah", "Mehta", "Desai", "Modi", "Jain", "Agarwal", "Goel", "Kapoor",
]

EMPLOYMENT_TYPES = ["Salaried", "Self-Employed", "Business Owner"]
EMPLOYER_SECTORS = {
    "Salaried": ["IT/Software", "Manufacturing", "Banking", "Government", "Healthcare", "Education", "Retail", "Logistics"],
    "Self-Employed": ["Retail Shop", "Transport", "Agriculture", "Construction", "Food & Beverage", "Auto Repair"],
    "Business Owner": ["Wholesale", "Real Estate", "Manufacturing", "Import-Export", "Franchise"],
}
KYC_METHODS = ["Aadhaar_OTP", "Video_KYC", "Physical_KYC", "DigiLocker"]

customers_data = []
# Pre-select ~3% customers who are fraud accomplices (will be used in loans)
n_fraud_customers = int(N_CUSTOMERS * 0.03)
fraud_customer_indices = set(random.sample(range(N_CUSTOMERS), n_fraud_customers))

for i in range(N_CUSTOMERS):
    gender = random.choice(["Male", "Male", "Male", "Female"])  # 75% male (realistic for 2W)
    first_name = random.choice(INDIAN_FIRST_NAMES_M if gender == "Male" else INDIAN_FIRST_NAMES_F)
    last_name = random.choice(INDIAN_LAST_NAMES)
    full_name = f"{first_name} {last_name}"

    region, state, city, pincode = pick_location()
    employment = np.random.choice(EMPLOYMENT_TYPES, p=[0.55, 0.30, 0.15])

    # Monthly income: log-normal, realistic INR range
    if employment == "Business Owner":
        monthly_income = int(np.random.lognormal(np.log(80000), 0.6))
    elif employment == "Self-Employed":
        monthly_income = int(np.random.lognormal(np.log(35000), 0.7))
    else:
        monthly_income = int(np.random.lognormal(np.log(45000), 0.5))
    monthly_income = max(12000, min(500000, monthly_income))

    # CIBIL score: beta distribution 300-900
    raw_cibil = np.random.beta(5, 2) * 600 + 300
    cibil_score = int(min(900, max(300, raw_cibil)))

    is_fraud_profile = i in fraud_customer_indices
    # Fraud customers have slightly worse credit profiles
    if is_fraud_profile:
        cibil_score = max(300, cibil_score - random.randint(50, 150))
        bureau_enquiries = random.randint(4, 12)
        existing_loan_count = random.randint(2, 6)
    else:
        bureau_enquiries = int(np.random.exponential(1.5))
        bureau_enquiries = min(bureau_enquiries, 8)
        existing_loan_count = int(np.random.exponential(0.8))
        existing_loan_count = min(existing_loan_count, 5)

    dob = fake.date_of_birth(minimum_age=21, maximum_age=58)
    created_ts = random_timestamp(START_DATE - timedelta(days=730), START_DATE)

    customers_data.append({
        "customer_id": f"CUST-{i:05d}",
        "full_name": full_name,
        "gender": gender,
        "date_of_birth": dob.strftime("%Y-%m-%d"),
        "age": (datetime.now().date() - dob).days // 365,
        "pan_number": random_pan(),
        "aadhaar_last4": random_aadhaar_last4(),
        "mobile_number": random_mobile(),
        "email": synthetic_email(full_name),
        "marital_status": random.choice(["Married", "Married", "Single", "Single", "Divorced"]),
        "city": city,
        "state": state,
        "region": region,
        "pincode": pincode,
        "address_type": np.random.choice(["Owned", "Rented", "Parental"], p=[0.35, 0.45, 0.20]),
        "years_at_current_address": random.randint(0, 20),
        "employment_type": employment,
        "employer_sector": random.choice(EMPLOYER_SECTORS[employment]),
        "monthly_income": monthly_income,
        "cibil_score": cibil_score,
        "existing_loan_count": existing_loan_count,
        "bureau_enquiries_last_6m": bureau_enquiries,
        "kyc_status": "Verified" if not is_fraud_profile else np.random.choice(["Verified", "Pending", "Failed"], p=[0.6, 0.25, 0.15]),
        "kyc_method": random.choice(KYC_METHODS),
        "is_politically_exposed": random.random() < 0.005,
        "is_fraud_profile": is_fraud_profile,
        "created_timestamp": created_ts,
        "updated_timestamp": created_ts,
    })

customers_pdf = pd.DataFrame(customers_data)
customer_ids = customers_pdf["customer_id"].tolist()
customer_map = customers_pdf.set_index("customer_id").to_dict("index")
fraud_customer_ids = set(customers_pdf[customers_pdf["is_fraud_profile"]]["customer_id"].tolist())
print(f"  Created {len(customers_pdf):,} customers ({len(fraud_customer_ids)} fraud profiles)")

# =============================================================================
# DATASET 4: LOANS
# =============================================================================
print("Generating loans...")

product_ids = products_pdf["product_id"].tolist()
product_map = products_pdf.set_index("product_id").to_dict("index")
dealer_ids = dealers_pdf["dealer_id"].tolist()
dealer_map = dealers_pdf.set_index("dealer_id").to_dict("index")
flagged_dealer_ids = set(dealers_pdf[dealers_pdf["is_flagged"]]["dealer_id"].tolist())

CHANNELS = ["Branch", "Dealer", "Online", "DSA"]
CHANNEL_WEIGHTS = [0.40, 0.35, 0.20, 0.05]
TENURE_OPTIONS = [24, 36, 48]
TENURE_WEIGHTS = [0.40, 0.45, 0.15]
LOAN_STATUSES = ["Active", "Closed", "Defaulted", "Rejected", "NPA"]

n_fraud_loans = int(N_LOANS * FRAUD_RATE)
fraud_loan_indices = set(random.sample(range(N_LOANS), n_fraud_loans))

loans_data = []
for i in range(N_LOANS):
    is_fraud = i in fraud_loan_indices

    # Fraud loans: higher chance of being from fraud customer profile
    if is_fraud and random.random() < 0.6:
        cid = random.choice(list(fraud_customer_ids))
    else:
        cid = random.choice(customer_ids)

    cust = customer_map[cid]

    # Pick product (electric more expensive → higher loan amount)
    product_id = random.choice(product_ids)
    prod = product_map[product_id]

    # Loan amount between INR 50,000 and 2,00,000 — capped at on_road price
    loan_max = min(200000, prod["on_road_price_max"])
    loan_min = max(50000, int(prod["on_road_price_min"] * 0.70))
    loan_amount = int(np.random.uniform(loan_min, loan_max) // 1000) * 1000
    loan_amount = max(50000, min(200000, loan_amount))

    down_payment = int((prod["on_road_price_min"] - loan_amount) if prod["on_road_price_min"] > loan_amount else 0)

    tenure = np.random.choice(TENURE_OPTIONS, p=TENURE_WEIGHTS)

    # Interest rate: higher for fraud customers (risk-based pricing fails in fraud)
    base_rate = 14.0
    if cust["cibil_score"] < 600:
        base_rate += 3.5
    elif cust["cibil_score"] < 680:
        base_rate += 1.5
    interest_rate = round(base_rate + np.random.uniform(-0.5, 1.5), 2)

    # Monthly EMI: P * r * (1+r)^n / ((1+r)^n - 1)
    r = interest_rate / 100 / 12
    emi = int(loan_amount * r * (1 + r) ** tenure / ((1 + r) ** tenure - 1))

    # Application date with festival season spike
    if random.random() < 0.25:
        app_date = fake.date_between(start_date=FESTIVAL_START, end_date=FESTIVAL_END)
    else:
        app_date = fake.date_between(start_date=START_DATE, end_date=END_DATE)

    # Dealer: fraud loans prefer flagged dealers
    if is_fraud and random.random() < 0.4 and flagged_dealer_ids:
        dealer_id = random.choice(list(flagged_dealer_ids))
    else:
        dealer_id = random.choice(dealer_ids)

    channel = np.random.choice(CHANNELS, p=CHANNEL_WEIGHTS)

    # Fraud loan statuses: more defaults/NPA
    if is_fraud:
        loan_status = np.random.choice(LOAN_STATUSES, p=[0.20, 0.05, 0.45, 0.20, 0.10])
        fraud_type = np.random.choice([
            "Identity_Theft", "Income_Falsification", "Dealer_Collusion",
            "Duplicate_Application", "Asset_Inflation"
        ])
    else:
        loan_status = np.random.choice(LOAN_STATUSES, p=[0.55, 0.30, 0.08, 0.05, 0.02])
        fraud_type = None

    disbursement_date = (app_date + timedelta(days=random.randint(2, 7))).strftime("%Y-%m-%d") \
        if loan_status not in ["Rejected"] else None
    first_emi_date = (datetime.strptime(disbursement_date, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d") \
        if disbursement_date else None

    event_ts = app_date.strftime("%Y-%m-%dT") + f"{random.randint(8, 18):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}Z"

    loans_data.append({
        "loan_id": f"LN-{i:06d}",
        "customer_id": cid,
        "product_id": product_id,
        "dealer_id": dealer_id,
        "brand": prod["brand"],
        "product_type": prod["product_type"],
        "loan_amount": loan_amount,
        "down_payment": down_payment,
        "on_road_price": prod["on_road_price_min"],
        "loan_tenure_months": tenure,
        "interest_rate_pct": interest_rate,
        "emi_amount": emi,
        "application_channel": channel,
        "loan_status": loan_status,
        "application_date": app_date.strftime("%Y-%m-%d"),
        "disbursement_date": disbursement_date,
        "first_emi_date": first_emi_date,
        "is_fraud": is_fraud,
        "fraud_type": fraud_type,
        "event_timestamp": event_ts,
        "created_timestamp": event_ts,
        "updated_timestamp": event_ts,
    })

loans_pdf = pd.DataFrame(loans_data)
loan_ids = loans_pdf["loan_id"].tolist()
loan_map = loans_pdf.set_index("loan_id").to_dict("index")
print(f"  Created {len(loans_pdf):,} loans ({loans_pdf['is_fraud'].sum()} fraud)")

# =============================================================================
# DATASET 5: EMI PAYMENTS
# =============================================================================
print("Generating EMI payments...")

# Only for disbursed (non-rejected) loans
disbursed_loans = loans_pdf[loans_pdf["disbursement_date"].notna() & (loans_pdf["loan_status"] != "Rejected")]

emi_data = []
emi_idx = 0
for _, loan_row in disbursed_loans.iterrows():
    lid = loan_row["loan_id"]
    cid = loan_row["customer_id"]
    tenure = loan_row["loan_tenure_months"]
    emi_amount = loan_row["emi_amount"]
    is_fraud = loan_row["is_fraud"]
    loan_status = loan_row["loan_status"]
    first_emi_str = loan_row["first_emi_date"]
    if pd.isna(first_emi_str) or first_emi_str is None:
        continue
    first_emi_dt = datetime.strptime(first_emi_str, "%Y-%m-%d")

    # How many EMIs have been generated so far (within our 12-month window)
    months_elapsed = min(tenure, max(1, (END_DATE - first_emi_dt).days // 30))

    for month in range(1, months_elapsed + 1):
        due_date = first_emi_dt + timedelta(days=30 * (month - 1))
        if due_date > END_DATE:
            break

        # Payment behavior based on fraud and loan status
        if is_fraud and month <= 3:
            # Fraud: early delinquency
            status = np.random.choice(["Paid", "Late", "Bounced", "Defaulted"], p=[0.30, 0.25, 0.30, 0.15])
        elif loan_status == "Defaulted" and month > 3:
            status = np.random.choice(["Defaulted", "Bounced"], p=[0.7, 0.3])
        elif loan_status == "NPA":
            status = "Defaulted"
        elif loan_status == "Active":
            status = np.random.choice(["Paid", "Late", "Bounced"], p=[0.88, 0.08, 0.04])
        else:  # Closed
            status = "Paid"

        payment_date = due_date + timedelta(days=random.randint(0, 5)) if status == "Paid" \
            else due_date + timedelta(days=random.randint(6, 30)) if status == "Late" \
            else None

        actual_amount = emi_amount if status in ["Paid", "Late"] else 0
        penalty = round(emi_amount * 0.02 * random.uniform(1, 3), 2) if status in ["Late", "Bounced"] else 0.0

        event_ts = (payment_date or due_date).strftime("%Y-%m-%dT") + \
            f"{random.randint(8, 20):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}Z"

        emi_data.append({
            "payment_id": f"PAY-{emi_idx:07d}",
            "loan_id": lid,
            "customer_id": cid,
            "emi_number": month,
            "due_date": due_date.strftime("%Y-%m-%d"),
            "payment_date": payment_date.strftime("%Y-%m-%d") if payment_date else None,
            "emi_amount": int(emi_amount),
            "actual_amount_paid": int(actual_amount),
            "penalty_amount": penalty,
            "payment_status": status,
            "payment_mode": random.choice(["NACH", "UPI", "Net_Banking", "Cash", "Cheque"]) if status in ["Paid", "Late"] else None,
            "days_past_due": max(0, (due_date - (payment_date or END_DATE)).days * -1) if status != "Paid" else 0,
            "event_timestamp": event_ts,
            "created_timestamp": event_ts,
            "updated_timestamp": event_ts,
        })
        emi_idx += 1

emi_pdf = pd.DataFrame(emi_data)
print(f"  Created {len(emi_pdf):,} EMI payment records")

# =============================================================================
# DATASET 6: KYC EVENTS
# =============================================================================
print("Generating KYC events...")

KYC_STATUSES = ["Passed", "Failed", "Pending", "Expired"]
KYC_FAILURE_REASONS = [
    "Document_Mismatch", "Face_Match_Failed", "Address_Mismatch",
    "Invalid_PAN", "Aadhaar_OTP_Failed", "Suspicious_Document", "Duplicate_Identity"
]

kyc_data = []
kyc_idx = 0
for cust_row in customers_data:
    cid = cust_row["customer_id"]
    is_fraud_profile = cust_row["is_fraud_profile"]

    # Fraud profiles get more KYC attempts (multiple failures before passing)
    n_attempts = random.randint(2, 5) if is_fraud_profile else random.randint(1, 2)

    for attempt in range(1, n_attempts + 1):
        is_last = attempt == n_attempts
        if is_last:
            status = "Passed" if cust_row["kyc_status"] == "Verified" else \
                     "Failed" if cust_row["kyc_status"] == "Failed" else "Pending"
        else:
            status = "Failed" if is_fraud_profile else np.random.choice(["Passed", "Failed"], p=[0.85, 0.15])

        failure_reason = random.choice(KYC_FAILURE_REASONS) if status == "Failed" else None
        method = random.choice(KYC_METHODS)

        created_ts = cust_row["created_timestamp"]
        event_dt = datetime.strptime(created_ts, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=attempt * random.randint(1, 48))
        event_ts = event_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        kyc_data.append({
            "kyc_event_id": f"KYC-{kyc_idx:07d}",
            "customer_id": cid,
            "attempt_number": attempt,
            "kyc_method": method,
            "kyc_status": status,
            "failure_reason": failure_reason,
            "document_type": random.choice(["Aadhaar", "PAN", "Voter_ID", "Driving_License", "Passport"]),
            "is_video_verified": method == "Video_KYC",
            "agent_id": f"AGT-{random.randint(1, 200):04d}" if method == "Physical_KYC" else None,
            "ip_address": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
            "device_type": random.choice(["Mobile", "Desktop", "Tablet"]),
            "event_timestamp": event_ts,
            "created_timestamp": event_ts,
            "updated_timestamp": event_ts,
        })
        kyc_idx += 1

kyc_pdf = pd.DataFrame(kyc_data)
print(f"  Created {len(kyc_pdf):,} KYC event records")

# =============================================================================
# SAVE ALL DATASETS TO VOLUME AS NDJSON
# =============================================================================
print(f"\nSaving all datasets to {VOLUME_PATH}...")

def save_as_ndjson(pdf, path):
    """Save pandas DataFrame as newline-delimited JSON to Databricks Volume."""
    spark.createDataFrame(pdf).coalesce(1).write.mode("overwrite").json(path)
    print(f"  Saved {len(pdf):,} records → {path}")

save_as_ndjson(products_pdf, f"{VOLUME_PATH}/products")
save_as_ndjson(dealers_pdf, f"{VOLUME_PATH}/dealers")
save_as_ndjson(customers_pdf, f"{VOLUME_PATH}/customers")
save_as_ndjson(loans_pdf, f"{VOLUME_PATH}/loans")
save_as_ndjson(emi_pdf, f"{VOLUME_PATH}/emi_payments")
save_as_ndjson(kyc_pdf, f"{VOLUME_PATH}/kyc_events")

# =============================================================================
# VALIDATION SUMMARY
# =============================================================================
print("\n=== DATA GENERATION SUMMARY ===")
print(f"{'Dataset':<20} {'Records':>10}")
print("-" * 32)
for name, df in [("products", products_pdf), ("dealers", dealers_pdf), ("customers", customers_pdf),
                  ("loans", loans_pdf), ("emi_payments", emi_pdf), ("kyc_events", kyc_pdf)]:
    print(f"{name:<20} {len(df):>10,}")

print(f"\n--- Fraud Stats ---")
print(f"Fraud loans:       {loans_pdf['is_fraud'].sum():,} / {len(loans_pdf):,} ({loans_pdf['is_fraud'].mean()*100:.1f}%)")
print(f"Fraud customers:   {customers_pdf['is_fraud_profile'].sum():,} / {len(customers_pdf):,}")
print(f"Flagged dealers:   {dealers_pdf['is_flagged'].sum():,} / {len(dealers_pdf):,}")
print(f"\n--- Loan Amount Stats ---")
print(f"Min: ₹{loans_pdf['loan_amount'].min():,.0f}  |  Max: ₹{loans_pdf['loan_amount'].max():,.0f}  |  Mean: ₹{loans_pdf['loan_amount'].mean():,.0f}")
print(f"\n--- Loan Status Distribution ---")
print(loans_pdf["loan_status"].value_counts().to_string())
print(f"\n--- Region Distribution (Customers) ---")
print(customers_pdf["region"].value_counts().to_string())
print(f"\nAll data saved to: {VOLUME_PATH}")
