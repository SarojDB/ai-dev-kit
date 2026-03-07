# Banking Financial Transactions Fraud Analytics

End-to-end implementation of a real-time fraud detection and investigation platform built on Databricks, covering synthetic data generation, a streaming medallion pipeline, a Genie AI analytics space, an OLTP fraud triage database, an impossible travel detector, and a live analyst application.

---

## Architecture Overview

```
Raw JSON (Unity Catalog Volume)
        │
        ▼
┌───────────────────────────────────────────────────────────────┐
│               Lakeflow Spark Declarative Pipeline             │
│                  sv_bank_transactions_fraud_sdp               │
│                                                               │
│  Bronze (Auto Loader)  →  Silver (Transform + Alert)  →  Gold│
└───────────────────────────────────────────────────────────────┘
        │                         │                      │
        │                         ▼                      ▼
        │            silver_wire_transfer_ip_alerts   dim_user (SCD2)
        │            silver_impossible_travel_alerts  dim_merchant (SCD2)
        │                                             fact_transactions
        │                                             agg_fraud_velocity_by_user
        │                                             agg_high_value_wire_risk
        ▼
┌─────────────────────┐   ┌────────────────────────┐   ┌──────────────────────┐
│  Genie Space        │   │  Lakebase (PostgreSQL)  │   │  Impossible Travel   │
│  Banking Fraud      │   │  fraud_ops.fraud_triage │   │  Detector            │
│  Investigation Hub  │   │  real_time_fraud_triage │   │  (Databricks Connect)│
└─────────────────────┘   └────────────────────────┘   └──────────────────────┘
                                      │
                                      ▼
                         ┌────────────────────────┐
                         │   Live Fraud Queue App  │
                         │   (Databricks App/Dash) │
                         │   Fraud Queue  │  Genie │
                         └────────────────────────┘
```

---

## Step 1 — Synthetic Data Generation

**Script:** `scripts/generate_banking_data.py`

Generates five interlinked datasets mimicking Axis Bank's internal transaction systems, stored as newline-delimited JSON (`.jsonl`) in Unity Catalog.

| Dataset | Volume Path | Records |
|---------|------------|---------|
| Users | `.../mock_banking_data/users/` | 10,000 |
| Merchants | `.../mock_banking_data/merchants/` | 2,000 |
| Devices | `.../mock_banking_data/devices/` | 15,000 |
| Transactions | `.../mock_banking_data/transactions/` | 100,000 |
| Login Logs | `.../mock_banking_data/login_logs/` | ~80,000 |

**Key design decisions:**
- Consistent foreign keys: `user_id`, `merchant_id`, `device_id`, `transaction_id`, `login_id`
- Transactions geographically distributed across India with realistic lat/lon coordinates
- Login logs include `ip_address`, `mfa_change_flag`, `ip_risk_score`, `is_vpn`, `is_tor`, `login_country_code`
- Transactions include `transaction_type` (NEFT, RTGS, IMPS, WIRE), `amount`, `city`, `state`
- Vectorized Pandas generation for performance (avoids row-by-row slowness)

**Fraud scenario injection:** `scripts/append_fraud_alert_scenarios.py`

Appends ~65 explicit wire-fraud-after-IP-change scenarios ensuring a sufficient alert count for demonstration. Each scenario injects a matching pair: a high-risk login (MFA change + IP change) followed by a high-value wire transfer within 60 minutes from the same user.

---

## Step 2 — Medallion Pipeline (Lakeflow Spark Declarative Pipelines)

**Bundle:** `sv_bank_transactions_fraud_sdp/`  
**Catalog:** `financial_security`  
**Deployed via:** Databricks Asset Bundles (DABs)

### Bronze Layer — Raw Ingestion

Schema: `financial_security.sv_bank_transactions_fraud_bronze`

All four tables use **Auto Loader** (`cloudFiles`) with `cloudFiles.schemaEvolution = "addNewColumns"` for resilient schema evolution.

| Table | Source Path | Notes |
|-------|------------|-------|
| `bronze_transactions` | `.../transactions/` | Streaming table |
| `bronze_login_logs` | `.../login_logs/` | Streaming table |
| `bronze_users` | `.../users/` | Streaming table |
| `bronze_merchants` | `.../merchants/` | Streaming table |

Schema location stored in a UC volume for checkpoint and schema inference persistence.

### Silver Layer — Cleanse, Enrich, Alert

Schema: `financial_security.sv_bank_transactions_fraud_silver`

| Table / View | Type | Key Logic |
|---|---|---|
| `silver_transactions` | Streaming Table | Casts types; derives `is_wire_transfer`, `is_high_value_wire` (amount > ₹50,000); normalises city/state; drops nulls on critical fields |
| `silver_login_logs` | Streaming Table | Parses `event_timestamp`; scores IP risk; flags `mfa_change_flag`, `is_vpn`, `is_tor` |
| `silver_users` | Streaming Table | Cleans PII fields; derives `credit_tier`, `account_age_days` |
| `silver_merchants` | Streaming Table | Normalises merchant category; derives `is_high_risk_category` |
| `silver_wire_transfer_ip_alerts` | Materialized View | **Core fraud signal:** joins wire transfers > ₹50,000 with login logs showing IP change or MFA change within the preceding 60 minutes |

**`silver_wire_transfer_ip_alerts` logic:**
```sql
-- Flag: high-value wire transfer within 60 minutes of an IP/MFA risk login event
WHERE t.is_high_value_wire = true
  AND l.is_ip_change = true OR l.mfa_change_flag = true
  AND t.event_timestamp BETWEEN l.event_timestamp AND l.event_timestamp + INTERVAL 60 MINUTES
```

### Gold Layer — Star Schema

Schema: `financial_security.sv_bank_transactions_fraud_gold`

| Table | Type | Pattern |
|-------|------|---------|
| `dim_user` | Streaming Table | SCD Type 2 via `dp.create_auto_cdc_flow` — tracks `credit_tier`, `kyc_status`, `address_city` changes |
| `dim_merchant` | Streaming Table | SCD Type 2 — tracks `merchant_category`, `risk_score` changes |
| `fact_transactions` | Streaming Table | SCD Type 1 — latest transaction enriched with fraud signal join |
| `agg_fraud_velocity_by_user` | Materialized View | Transaction counts, fraud counts, avg amount, and velocity score per user per day |
| `agg_high_value_wire_risk` | Materialized View | Wire transfer risk aggregated by state × transaction type × credit tier |

---

## Step 3 — Genie Space: Banking Fraud Investigation Hub

**Space ID:** `01f118649c9413ec89adfb36436d439a`  
**Script:** `scripts/configure_genie_fraud_space.py`  
**Warehouse:** `199def4518ef9b3e`

Natural language SQL analytics interface for fraud investigators, connected to all silver and gold tables.

### Connected Tables

| Table | Purpose |
|-------|---------|
| `silver_transactions` | Base transaction stream with fraud flags |
| `silver_login_logs` | Login events with IP/MFA risk signals |
| `silver_wire_transfer_ip_alerts` | Core wire fraud alerts |
| `fact_transactions` | Enriched, modelled transaction facts |
| `dim_user` | User dimension with SCD2 history |
| `dim_merchant` | Merchant dimension |
| `agg_fraud_velocity_by_user` | Per-user velocity aggregates |
| `agg_high_value_wire_risk` | Wire risk by geography and method |

### Instructions for Genie

- All monetary amounts are in Indian Rupees (INR)
- High-value wire threshold is ₹50,000
- `mfa_change_flag = true` indicates a multi-factor authentication change event
- `is_tor = true` indicates a login from the Tor network — treat as high risk
- Fraud velocity score is computed as: transaction count × avg amount / account age
- FPR (False Positive Ratio) = false alarms / (false alarms + confirmed fraud)
- Account Takeover Rate = cases with `mfa_change_flag + foreign IP` / total flagged users

### Certified SQL (Banking KPIs)

```sql
-- False Positive Ratio
SELECT
  COUNT(CASE WHEN automated_action = 'FALSE_ALARM' THEN 1 END)  AS false_positives,
  COUNT(CASE WHEN automated_action = 'CONFIRM_FRAUD' THEN 1 END) AS confirmed_fraud,
  ROUND(
    COUNT(CASE WHEN automated_action = 'FALSE_ALARM' THEN 1 END) * 100.0 /
    NULLIF(COUNT(*), 0), 2
  ) AS false_positive_ratio_pct
FROM financial_security.sv_bank_transactions_fraud_gold.fact_transactions
WHERE automated_action IN ('FALSE_ALARM', 'CONFIRM_FRAUD');

-- Account Takeover Rate
SELECT
  credit_tier,
  COUNT(DISTINCT user_id)                                        AS total_flagged_users,
  COUNT(DISTINCT CASE WHEN mfa_change_flag AND login_country_code != 'IN'
        THEN user_id END)                                        AS ato_risk_users,
  ROUND(COUNT(DISTINCT CASE WHEN mfa_change_flag AND login_country_code != 'IN'
        THEN user_id END) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS ato_rate_pct
FROM financial_security.sv_bank_transactions_fraud_silver.silver_wire_transfer_ip_alerts
GROUP BY credit_tier
ORDER BY ato_rate_pct DESC;
```

### Sample Questions for Investigators

- Show all HIGH and CRITICAL wire transfer alerts from TOR or foreign IPs
- What is the False Positive Ratio for high-value wire transfer alerts?
- Which users have the highest fraud velocity score this month?
- Show the daily trend of wire transfer fraud alerts in February 2026
- Which states have the highest wire fraud risk score?
- What is the wire fraud rate for NEFT vs RTGS vs IMPS?
- Show top 10 merchants by total confirmed fraud amount

---

## Step 4 — Lakebase OLTP Schema (PostgreSQL)

**Instance:** `sv-fraud-triage-db`  
**Database:** `fraud_ops`  
**Schema:** `fraud_triage`  
**Script:** `scripts/lakebase_fraud_triage_schema.py`

Managed PostgreSQL database for real-time transactional operations on fraud cases — optimised for low-latency reads/writes by analysts and the automated detection pipeline.

### Tables

#### `real_time_fraud_triage`
Primary case management table — one row per flagged transaction.

| Column | Type | Description |
|--------|------|-------------|
| `triage_id` | UUID PK | Auto-generated case identifier |
| `transaction_id` | VARCHAR | FK to Delta `fact_transactions` |
| `user_id` | VARCHAR | Customer identifier |
| `risk_score` | DECIMAL(5,2) | 0–100 composite fraud score |
| `risk_tier` | ENUM | CRITICAL / HIGH / MEDIUM / LOW |
| `triage_status` | ENUM | PENDING / IN_REVIEW / ESCALATED / RESOLVED / FALSE_ALARM / CLOSED |
| `automated_action` | ENUM | AUTO_BLOCK / REQUIRE_2FA / FLAG_REVIEW / MONITOR / ALLOW |
| `detected_fraud_type` | VARCHAR | Wire fraud, impossible travel, ATO, etc. |
| `risk_factors` | JSONB | Weighted factor breakdown `[{name, weight, value, description}]` |
| `transaction_amount` | DECIMAL | Transaction value in INR |
| `transaction_type` | VARCHAR | NEFT / RTGS / IMPS / WIRE |
| `review_sla_deadline` | TIMESTAMPTZ | Auto-computed: 2 hrs for CRITICAL, 24 hrs for HIGH, etc. |
| `assigned_to` | VARCHAR | Analyst email |
| `resolution_notes` | TEXT | Free-text analyst comments |
| `alert_generated_at` | TIMESTAMPTZ | When the alert was first created |

#### `triage_audit_log`
Immutable append-only log of every state change on a case.

| Column | Description |
|--------|-------------|
| `event_type` | STATUS_CHANGE / ASSIGNMENT / NOTE_ADDED / ESCALATION |
| `actor` | Analyst email or system identifier |
| `previous_value` / `new_value` | Before/after state |
| `logged_at` | Event timestamp |

#### `fraud_rule_triggers`
Which detection rules fired for each alert, with contribution scores.

| Column | Description |
|--------|-------------|
| `rule_id` | Rule identifier (e.g. `WIRE_IP_CHANGE_60M`) |
| `rule_name` | Human-readable name |
| `triggered_value` | The value that triggered the rule |
| `contribution_score` | Points added to the composite risk score |

### PostgreSQL Features Used
- **ENUM types** for `risk_tier_enum`, `triage_status_enum`, `automated_action_enum`
- **JSONB** for `risk_factors` with GIN index for fast JSON querying
- **Triggers** to auto-update `updated_at` timestamps and compute `review_sla_deadline`
- **Partial indexes** on `triage_status` for fast open-case queries
- **Composite indexes** on `(user_id, alert_generated_at)` for timeline queries

### Query Helper Library

**Script:** `scripts/query_fraud_triage.py`  
`FraudTriageDB` class providing pre-built analyst queries:
- `open_cases()` — all PENDING and IN_REVIEW cases
- `sla_breaches()` — cases past their SLA deadline
- `kpi_summary()` — counts by status and tier
- `case_detail(transaction_id)` — full case with audit log and rule triggers
- `update_case_status(txn_id, status, actor, notes)`

---

## Step 5 — Impossible Travel Detector

**Script:** `scripts/impossible_travel_detector.py`  
**Runtime:** Databricks Connect (serverless)

Detects "impossible travel" fraud: a user whose geolocation jumps more than **500 miles in under 10 minutes** across two consecutive transactions — physically impossible without simultaneous device compromise.

### Detection Logic

```
Haversine distance (miles) between consecutive transaction locations
÷ time gap (minutes) × 60 = implied speed (mph)

If distance > 500 miles AND time gap < 10 minutes → IMPOSSIBLE TRAVEL ALERT
```

**Haversine formula in pure Spark Column API:**
```python
def haversine_miles(lat1, lon1, lat2, lon2) -> Column:
    R = 3_958.8   # Earth radius in miles
    φ1, φ2 = F.radians(lat1), F.radians(lat2)
    Δφ = F.radians(lat2 - lat1)
    Δλ = F.radians(lon2 - lon1)
    a  = (F.pow(F.sin(Δφ / 2), 2)
          + F.cos(φ1) * F.cos(φ2) * F.pow(F.sin(Δλ / 2), 2))
    return F.lit(2 * R) * F.asin(F.sqrt(a))
```

**Risk score formula:**
```python
risk_score = 60                               # base for impossible travel
           + min(30, speed_mph / 1000 * 10)  # speed contribution (max 30 pts)
           + min(10, amount / 100_000 * 10)  # amount contribution (max 10 pts)
# capped at 100
```

### Two Operating Modes

| Mode | Use Case | Method |
|------|---------|--------|
| `--simulate` | Historical batch analysis | Window functions with `F.lag()` over `(user_id ORDER BY event_timestamp)` |
| Live stream | Real-time monitoring | `foreachBatch` with Delta Change Data Feed + stateful `MERGE` into a Delta state table |

### Outputs

- **Delta table:** `financial_security.sv_bank_transactions_fraud_silver.silver_impossible_travel_alerts`
- **Lakebase insert:** `fraud_triage.real_time_fraud_triage` — creates a new CRITICAL case with `detected_fraud_type = 'IMPOSSIBLE_TRAVEL'` and computed risk score

### Key Design Choices
- **No Python UDFs** — uses pure Spark Column API to avoid Python version compatibility issues with Databricks Connect serverless
- Delta state table with `MERGE` for stateful streaming (tracks last known location per user)
- `RocksDBStateProvider` config excluded (not allowed via Databricks Connect)

---

## Step 6 — Live Fraud Queue (Databricks App)

**App URL:** `https://live-fraud-queue-7474658534134271.aws.databricksapps.com`  
**Directory:** `live_fraud_queue/`  
**Framework:** Dash + dash-bootstrap-components (Bootstrap dark theme)

A real-time analyst workstation for reviewing, triaging, and resolving flagged transactions.

### Data Architecture

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Analytical queries | Databricks SQL Warehouse (`199def4518ef9b3e`) | KPI aggregations, AG Grid data, impossible travel context |
| Transactional CRUD | Lakebase PostgreSQL (`fraud_ops.fraud_triage`) | Case status updates, audit log writes, assignment |

### App Structure

```
live_fraud_queue/
├── app.py            # Dash layout and callbacks
├── backend.py        # Data layer (SQL Warehouse + Lakebase)
├── genie.py          # Genie Conversation API client
├── app.yaml          # Databricks App resource bindings
├── requirements.txt  # Python dependencies
└── assets/
    └── custom.css    # Dark theme design system (CSS variables)
```

### Tab 1 — Fraud Queue

**KPI Bar** (6 cards, auto-refreshing every 30 seconds):

| KPI | Description |
|-----|-------------|
| Open Cases | Total PENDING + IN_REVIEW cases |
| Critical | CRITICAL risk tier open cases |
| SLA Breaches | Cases past their review deadline |
| Confirmed Fraud | Total CONFIRM_FRAUD resolutions |
| False Positives | Total FALSE_ALARM resolutions |
| FPR | False Positive Ratio as a percentage |

**Filter Bar:**
- Status multi-select (Pending, In Review, Escalated, Resolved, False Alarm)
- Risk Tier multi-select (Critical, High, Medium, Low)

**AG Grid (ag-theme-alpine-dark):**

| Column | Highlight |
|--------|-----------|
| Risk Tier | Colour-coded: red/orange/blue/green |
| Risk Score | Numeric, 1 decimal |
| Amount ₹ | Indian locale formatting; bold if > ₹1,00,000 |
| MFA / VPN / TOR | Warning icons; TOR shown in red |
| Country | Orange highlight for non-IN logins |
| SLA | Red + bold if breached for open cases |

**Case Detail Slide-over Panel** (opens on row click):

| Tab | Content |
|-----|---------|
| Overview | Transaction metadata, location, login risk flags |
| Risk Factors | JSONB-parsed factor cards with progress bars |
| Audit | Chronological event log from `triage_audit_log` |
| Rules | Triggered rule table with contribution scores |
| Travel | Impossible travel alerts for the user's account |

**Analyst Actions** (in panel):
- **Release** (False Positive) — sets status to `FALSE_ALARM`
- **Confirm Fraud** — sets status to `RESOLVED` with fraud confirmation
- **Escalate** — sets status to `ESCALATED`
- **Assign** — assigns case to a named analyst

All actions write to Lakebase and append an entry to `triage_audit_log`.

### Tab 2 — Genie Chat

Conversational analytics via the **Banking Fraud Investigation Hub** Genie Space.

**Sidebar:**
- 8 pre-loaded suggested question chips (click to populate input)
- "New conversation" button to reset context

**Chat Interface:**
- User messages — right-aligned indigo bubbles
- Genie responses — left-aligned dark bubbles
- Collapsible "View generated SQL" block under each answer
- Inline result table (up to 20 rows displayed)
- Follow-up questions maintain full conversation context via `conversation_id`

**`genie.py` API client:**
- `start_conversation(question)` — POST to `/api/2.0/genie/spaces/{space_id}/start-conversation`, polls until `COMPLETED`
- `send_followup(conv_id, question)` — continues existing conversation
- Handles `COMPLETED` / `FAILED` / `TIMEOUT` / `CANCELLED` states
- Fetches query results from the `/query-result` endpoint and returns rows + column names

### Authentication & Permissions

| Resource | Auth Method | Permission |
|----------|------------|-----------|
| SQL Warehouse | `DATABRICKS_WAREHOUSE_ID` env var + SDK `Config()` | `CAN_USE` |
| Lakebase PostgreSQL | OAuth token via `WorkspaceClient(config=Config())` with background refresh thread | `CAN_CONNECT_AND_CREATE` on `fraud_ops` |
| Genie Space | Same SDK `Config()` SP credentials | `CAN_RUN` (granted via `/api/2.0/permissions/genie/{space_id}`) |

The app's **service principal** (`742e362a-d570-4d3a-8745-3678332856df`) has explicit PostgreSQL grants on the `fraud_triage` schema:

```sql
GRANT USAGE                          ON SCHEMA fraud_triage TO "742e362a...";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES     IN SCHEMA fraud_triage TO "742e362a...";
GRANT USAGE                          ON ALL SEQUENCES   IN SCHEMA fraud_triage TO "742e362a...";
GRANT EXECUTE                        ON ALL FUNCTIONS   IN SCHEMA fraud_triage TO "742e362a...";
-- Default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA fraud_triage GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO "742e362a...";
ALTER DEFAULT PRIVILEGES IN SCHEMA fraud_triage GRANT USAGE                          ON SEQUENCES  TO "742e362a...";
ALTER DEFAULT PRIVILEGES IN SCHEMA fraud_triage GRANT EXECUTE                        ON FUNCTIONS  TO "742e362a...";
```

---

## Unity Catalog Layout

```
financial_security
├── raw_data (schema)
│   └── mock_banking_data (volume)
│       ├── transactions/      ← 100K transactions (.jsonl)
│       ├── login_logs/        ← ~80K login events (.jsonl)
│       ├── users/             ← 10K users (.jsonl)
│       ├── merchants/         ← 2K merchants (.jsonl)
│       └── devices/           ← 15K devices (.jsonl)
│
├── sv_bank_transactions_fraud_bronze (schema)
│   ├── bronze_transactions
│   ├── bronze_login_logs
│   ├── bronze_users
│   └── bronze_merchants
│
├── sv_bank_transactions_fraud_silver (schema)
│   ├── silver_transactions
│   ├── silver_login_logs
│   ├── silver_users
│   ├── silver_merchants
│   ├── silver_wire_transfer_ip_alerts   ← Core fraud signal
│   └── silver_impossible_travel_alerts  ← Geo-velocity fraud signal
│
└── sv_bank_transactions_fraud_gold (schema)
    ├── dim_user             (SCD Type 2)
    ├── dim_merchant         (SCD Type 2)
    ├── fact_transactions    (SCD Type 1)
    ├── agg_fraud_velocity_by_user
    └── agg_high_value_wire_risk
```

---

## Key Fraud Detection Rules

| Rule | Threshold | Alert Table |
|------|-----------|-------------|
| High-value wire after IP change | Wire > ₹50,000 within 60 min of IP/MFA change | `silver_wire_transfer_ip_alerts` |
| Impossible travel | >500 miles in <10 minutes between transactions | `silver_impossible_travel_alerts` |
| TOR network login | Any transaction following a TOR login | `silver_wire_transfer_ip_alerts` (via `is_tor` flag) |
| Foreign IP wire transfer | Wire from non-Indian IP | `silver_wire_transfer_ip_alerts` (via `login_country_code != 'IN'`) |

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Data generation | Python, Faker, Pandas, NumPy |
| Pipeline orchestration | Databricks Asset Bundles (DABs) |
| Streaming pipeline | Lakeflow Spark Declarative Pipelines (SDP) |
| Batch/stream ingestion | Auto Loader (`cloudFiles`) |
| Change data capture | `dp.create_auto_cdc_flow` (SCD Type 1 & 2) |
| Data format | Delta Lake (Bronze/Silver/Gold), JSONL (raw) |
| Data governance | Unity Catalog |
| OLTP database | Lakebase Provisioned (managed PostgreSQL) |
| Real-time detection | Databricks Connect + Spark Structured Streaming |
| AI analytics | Databricks Genie Spaces (Conversation API) |
| Analyst application | Databricks Apps + Dash + AG Grid |
| Authentication | Databricks SDK `Config()` + OAuth token refresh |
