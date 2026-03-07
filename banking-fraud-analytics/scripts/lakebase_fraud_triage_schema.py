"""
Lakebase schema for Banking Fraud Real-Time Triage.

Tables created:
  fraud_triage_schema
  ├── real_time_fraud_triage          Primary triage record per alert
  ├── triage_audit_log                Immutable change history
  └── fraud_rule_triggers             Which detection rules fired per transaction

Run:
  python scripts/lakebase_fraud_triage_schema.py
"""

import uuid
import os
import psycopg
from databricks.sdk import WorkspaceClient

INSTANCE_NAME = "sv-fraud-triage-db"
DB_NAME       = "fraud_ops"
SCHEMA        = "fraud_triage"

# ── Connect ────────────────────────────────────────────────────────────────────
import configparser
cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser('~/.databrickscfg'))
host  = cfg['databricks_oneenv']['host']
token = cfg['databricks_oneenv']['token']

w = WorkspaceClient(host=host, token=token)
instance = w.database.get_database_instance(name=INSTANCE_NAME)
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=[INSTANCE_NAME]
)
username = w.current_user.me().user_name

conn_str = (
    f"host={instance.read_write_dns} "
    f"dbname={DB_NAME} "
    f"user={username} "
    f"password={cred.token} "
    f"sslmode=require "
    f"connect_timeout=15"
)
print(f"Connecting to {instance.read_write_dns} as {username} ...")

# ── DDL (uses S as placeholder to avoid conflict with JSONB {} braces) ─────────
S = SCHEMA  # noqa: shorthand used only in this template string
DDL = """
-- ═══════════════════════════════════════════════════════════════════════════
--  SCHEMA
-- ═══════════════════════════════════════════════════════════════════════════
CREATE SCHEMA IF NOT EXISTS {SCHEMA};
SET search_path TO {SCHEMA};

-- ═══════════════════════════════════════════════════════════════════════════
--  ENUM TYPES
-- ═══════════════════════════════════════════════════════════════════════════

DO $$ BEGIN
  CREATE TYPE triage_status AS ENUM (
    'PENDING',       -- just created, not yet picked up
    'IN_REVIEW',     -- analyst has opened the case
    'ESCALATED',     -- sent to senior analyst / compliance
    'RESOLVED',      -- decision reached
    'CLOSED',        -- case archived
    'FALSE_ALARM'    -- confirmed not fraud
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE automated_action AS ENUM (
    'NONE',                  -- no action taken
    'SOFT_BLOCK',            -- temporarily restrict outgoing wires
    'HARD_BLOCK',            -- full account freeze
    'STEP_UP_AUTH',          -- trigger MFA re-challenge
    'NOTIFY_USER',           -- send in-app / SMS alert to customer
    'FLAG_FOR_REVIEW',       -- route to human analyst queue
    'REFER_TO_COMPLIANCE',   -- escalate to AML / compliance
    'AUTO_DECLINED'          -- transaction declined in real-time
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE review_outcome AS ENUM (
    'CONFIRMED_FRAUD',
    'FALSE_POSITIVE',
    'SUSPICIOUS_KEEP_OPEN',
    'INSUFFICIENT_EVIDENCE',
    'CUSTOMER_CONFIRMED_LEGIT'
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE fraud_type AS ENUM (
    'ACCOUNT_TAKEOVER',
    'HIGH_VELOCITY',
    'GEO_ANOMALY',
    'CARD_FRAUD',
    'STRUCTURAL',
    'MULE_ACCOUNT',
    'SYNTHETIC_IDENTITY',
    'UNKNOWN'
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE rule_category AS ENUM (
    'IP_RISK',
    'VELOCITY',
    'AMOUNT_THRESHOLD',
    'GEO_ANOMALY',
    'DEVICE_ANOMALY',
    'TIME_OF_DAY',
    'MFA_CHANGE',
    'FOREIGN_LOGIN',
    'TOR_VPN'
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- ═══════════════════════════════════════════════════════════════════════════
--  CORE TABLE: real_time_fraud_triage
--  One row per transaction that has been flagged by the detection engine.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS {SCHEMA}.real_time_fraud_triage (

  -- ── Primary key ─────────────────────────────────────────────────────────
  triage_id               UUID          PRIMARY KEY DEFAULT gen_random_uuid(),

  -- ── Identifiers linking back to the fraud pipeline ────────────────────
  transaction_id          TEXT          NOT NULL,
  user_id                 TEXT          NOT NULL,
  alert_id                TEXT,                          -- silver_wire_transfer_ip_alerts.transaction_id
  triggering_login_id     TEXT,                          -- silver_login_logs.online_login_id

  -- ── Risk scoring ────────────────────────────────────────────────────────
  risk_score              NUMERIC(5,2)  NOT NULL         -- 0.00 – 100.00
                          CHECK (risk_score BETWEEN 0 AND 100),
  risk_tier               TEXT          NOT NULL         -- LOW / MEDIUM / HIGH / CRITICAL
                          CHECK (risk_tier IN ('LOW','MEDIUM','HIGH','CRITICAL')),
  model_version           TEXT,                          -- e.g. 'fraud_v3.1'
  risk_factors            JSONB         NOT NULL DEFAULT '[]',
  -- risk_factors shape: [{"name": "high_value_wire", "weight": 0.35, "value": 75000}]

  -- ── Fraud classification ────────────────────────────────────────────────
  detected_fraud_type     fraud_type    NOT NULL DEFAULT 'UNKNOWN',
  transaction_amount      NUMERIC(15,2),                 -- INR
  transaction_type        TEXT,                          -- NEFT, RTGS, IMPS, Net_Banking
  transaction_channel     TEXT,                          -- Mobile_App, Internet_Banking, etc.
  transaction_city        TEXT,
  transaction_state       TEXT,

  -- ── Login context at time of alert ─────────────────────────────────────
  login_ip_address        INET,
  login_country_code      CHAR(2),
  mfa_change_flag         BOOLEAN       NOT NULL DEFAULT FALSE,
  is_vpn                  BOOLEAN       NOT NULL DEFAULT FALSE,
  is_tor                  BOOLEAN       NOT NULL DEFAULT FALSE,
  minutes_after_login     NUMERIC(6,2),                  -- lag between login and transaction

  -- ── Automated response ──────────────────────────────────────────────────
  automated_action        automated_action NOT NULL DEFAULT 'NONE',
  action_taken_at         TIMESTAMPTZ,
  action_executed_by      TEXT,                          -- 'fraud_engine_v2' / service name

  -- ── Triage workflow ─────────────────────────────────────────────────────
  triage_status           triage_status NOT NULL DEFAULT 'PENDING',
  assigned_to             TEXT,                          -- analyst email / team queue
  escalation_reason       TEXT,

  -- ── Manual review outcome ───────────────────────────────────────────────
  reviewed_by             TEXT,
  review_outcome          review_outcome,
  review_notes            TEXT,
  reviewed_at             TIMESTAMPTZ,
  review_sla_deadline     TIMESTAMPTZ,                   -- SLA based on risk_tier

  -- ── Source metadata ─────────────────────────────────────────────────────
  source_system           TEXT          NOT NULL DEFAULT 'sv_bank_transactions_fraud_sdp',
  pipeline_run_id         TEXT,                          -- SDP update ID for lineage
  alert_generated_at      TIMESTAMPTZ   NOT NULL,        -- when fraud engine fired
  created_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  updated_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE {SCHEMA}.real_time_fraud_triage IS
  'Central fraud triage table. One row per flagged transaction. Tracks automated decisions, '
  'manual review outcomes, and links back to the Spark Declarative Pipeline alerts. '
  'Designed for sub-second writes from the fraud detection engine and concurrent analyst reads.';

COMMENT ON COLUMN {SCHEMA}.real_time_fraud_triage.risk_score IS
  'Composite risk score 0–100. Derived from fraud_velocity_score, alert_risk_level, '
  'amount, and contextual signals. Thresholds: LOW<30, MEDIUM 30-60, HIGH 60-80, CRITICAL>80.';
COMMENT ON COLUMN {SCHEMA}.real_time_fraud_triage.risk_factors IS
  'JSONB array of contributing risk signals: [{name, weight, value, description}]. '
  'Enables explainable AI for compliance and analyst review.';
COMMENT ON COLUMN {SCHEMA}.real_time_fraud_triage.automated_action IS
  'Action taken by the real-time engine without human intervention. '
  'HARD_BLOCK and REFER_TO_COMPLIANCE trigger immediate SLA escalation.';
COMMENT ON COLUMN {SCHEMA}.real_time_fraud_triage.review_sla_deadline IS
  'SLA: CRITICAL=1h, HIGH=4h, MEDIUM=24h, LOW=72h from alert_generated_at.';

-- ── Indexes ──────────────────────────────────────────────────────────────────

-- Fast lookup by transaction (FK to pipeline data)
CREATE INDEX IF NOT EXISTS idx_triage_transaction_id
  ON {SCHEMA}.real_time_fraud_triage (transaction_id);

-- User-centric investigation: all alerts for a given customer
CREATE INDEX IF NOT EXISTS idx_triage_user_id
  ON {SCHEMA}.real_time_fraud_triage (user_id);

-- Analyst queue: open cases sorted by risk
CREATE INDEX IF NOT EXISTS idx_triage_queue
  ON {SCHEMA}.real_time_fraud_triage (triage_status, risk_tier, alert_generated_at DESC)
  WHERE triage_status IN ('PENDING', 'IN_REVIEW', 'ESCALATED');

-- SLA monitoring: find overdue reviews
CREATE INDEX IF NOT EXISTS idx_triage_sla
  ON {SCHEMA}.real_time_fraud_triage (review_sla_deadline)
  WHERE review_sla_deadline IS NOT NULL
    AND triage_status NOT IN ('RESOLVED', 'CLOSED', 'FALSE_ALARM');

-- High-value wire fast filter
CREATE INDEX IF NOT EXISTS idx_triage_amount
  ON {SCHEMA}.real_time_fraud_triage (transaction_amount DESC)
  WHERE transaction_amount IS NOT NULL;

-- JSONB risk_factors GIN index for factor-level queries
CREATE INDEX IF NOT EXISTS idx_triage_risk_factors_gin
  ON {SCHEMA}.real_time_fraud_triage USING GIN (risk_factors);

-- Time-series dashboards and trend reporting
CREATE INDEX IF NOT EXISTS idx_triage_created_at
  ON {SCHEMA}.real_time_fraud_triage (created_at DESC);

-- ── Trigger: auto-set updated_at on any UPDATE ───────────────────────────────
CREATE OR REPLACE FUNCTION {SCHEMA}.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_triage_updated_at ON {SCHEMA}.real_time_fraud_triage;
CREATE TRIGGER trg_triage_updated_at
  BEFORE UPDATE ON {SCHEMA}.real_time_fraud_triage
  FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.set_updated_at();

-- ── Trigger: auto-compute review_sla_deadline based on risk_tier ─────────────
CREATE OR REPLACE FUNCTION {SCHEMA}.set_review_sla()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.review_sla_deadline IS NULL THEN
    NEW.review_sla_deadline := NEW.alert_generated_at + CASE NEW.risk_tier
      WHEN 'CRITICAL' THEN INTERVAL '1 hour'
      WHEN 'HIGH'     THEN INTERVAL '4 hours'
      WHEN 'MEDIUM'   THEN INTERVAL '24 hours'
      ELSE                  INTERVAL '72 hours'  -- LOW
    END;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_triage_sla ON {SCHEMA}.real_time_fraud_triage;
CREATE TRIGGER trg_triage_sla
  BEFORE INSERT ON {SCHEMA}.real_time_fraud_triage
  FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.set_review_sla();

-- ═══════════════════════════════════════════════════════════════════════════
--  AUDIT LOG TABLE: triage_audit_log
--  Append-only record of every status transition and action taken.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS {SCHEMA}.triage_audit_log (

  log_id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
  triage_id       UUID          NOT NULL
                  REFERENCES {SCHEMA}.real_time_fraud_triage(triage_id) ON DELETE CASCADE,
  transaction_id  TEXT          NOT NULL,

  -- What changed
  event_type      TEXT          NOT NULL,  -- STATUS_CHANGE | ACTION_TAKEN | REVIEW_SUBMITTED | ESCALATION
  old_value       JSONB,                   -- previous state snapshot
  new_value       JSONB,                   -- new state snapshot
  change_reason   TEXT,

  -- Who/what changed it
  actor           TEXT          NOT NULL,  -- analyst email or service name
  actor_type      TEXT          NOT NULL   -- HUMAN | SYSTEM | MODEL
                  CHECK (actor_type IN ('HUMAN', 'SYSTEM', 'MODEL')),

  logged_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE {SCHEMA}.triage_audit_log IS
  'Immutable audit trail for all triage state changes. Append-only — no UPDATEs. '
  'Required for AML compliance, SOC2, and fraud investigation chain-of-custody.';

CREATE INDEX IF NOT EXISTS idx_audit_triage_id
  ON {SCHEMA}.triage_audit_log (triage_id, logged_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_transaction_id
  ON {SCHEMA}.triage_audit_log (transaction_id);
CREATE INDEX IF NOT EXISTS idx_audit_actor
  ON {SCHEMA}.triage_audit_log (actor, logged_at DESC);

-- ── Trigger: auto-insert audit row on triage status change ───────────────────
CREATE OR REPLACE FUNCTION {SCHEMA}.audit_triage_changes()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO {SCHEMA}.triage_audit_log
      (triage_id, transaction_id, event_type, new_value, actor, actor_type)
    VALUES (
      NEW.triage_id, NEW.transaction_id, 'TRIAGE_CREATED',
      jsonb_build_object(
        'triage_status', NEW.triage_status,
        'automated_action', NEW.automated_action,
        'risk_score', NEW.risk_score,
        'risk_tier', NEW.risk_tier
      ),
      COALESCE(NEW.action_executed_by, 'fraud_engine'), 'SYSTEM'
    );
  ELSIF TG_OP = 'UPDATE' THEN
    IF OLD.triage_status <> NEW.triage_status THEN
      INSERT INTO {SCHEMA}.triage_audit_log
        (triage_id, transaction_id, event_type, old_value, new_value, actor, actor_type)
      VALUES (
        NEW.triage_id, NEW.transaction_id, 'STATUS_CHANGE',
        jsonb_build_object('triage_status', OLD.triage_status),
        jsonb_build_object('triage_status', NEW.triage_status),
        COALESCE(NEW.reviewed_by, NEW.assigned_to, 'system'), 'HUMAN'
      );
    END IF;
    IF OLD.automated_action <> NEW.automated_action THEN
      INSERT INTO {SCHEMA}.triage_audit_log
        (triage_id, transaction_id, event_type, old_value, new_value, actor, actor_type)
      VALUES (
        NEW.triage_id, NEW.transaction_id, 'ACTION_TAKEN',
        jsonb_build_object('automated_action', OLD.automated_action),
        jsonb_build_object('automated_action', NEW.automated_action,
                           'action_taken_at', NEW.action_taken_at),
        COALESCE(NEW.action_executed_by, 'fraud_engine'), 'SYSTEM'
      );
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_audit_triage ON {SCHEMA}.real_time_fraud_triage;
CREATE TRIGGER trg_audit_triage
  AFTER INSERT OR UPDATE ON {SCHEMA}.real_time_fraud_triage
  FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.audit_triage_changes();

-- ═══════════════════════════════════════════════════════════════════════════
--  RULE TRIGGERS TABLE: fraud_rule_triggers
--  Which detection rules fired for each flagged transaction.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS {SCHEMA}.fraud_rule_triggers (

  trigger_id      UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
  triage_id       UUID          NOT NULL
                  REFERENCES {SCHEMA}.real_time_fraud_triage(triage_id) ON DELETE CASCADE,
  transaction_id  TEXT          NOT NULL,

  -- Rule definition
  rule_id         TEXT          NOT NULL,   -- 'RULE_HV_WIRE_MFA_60MIN'
  rule_name       TEXT          NOT NULL,   -- human-readable label
  rule_category   rule_category NOT NULL,
  rule_version    TEXT,

  -- Trigger context
  triggered_value TEXT,           -- actual value that tripped the rule (e.g. '75000.00')
  threshold_value TEXT,           -- rule threshold (e.g. '50000.00')
  contribution_score NUMERIC(5,2), -- this rule's contribution to overall risk_score

  triggered_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE {SCHEMA}.fraud_rule_triggers IS
  'Normalised list of detection rules that contributed to a triage case. '
  'One row per rule per transaction. Enables rule-level analytics: which rules '
  'have highest precision, which generate most false positives, etc.';

CREATE INDEX IF NOT EXISTS idx_rule_triage_id
  ON {SCHEMA}.fraud_rule_triggers (triage_id);
CREATE INDEX IF NOT EXISTS idx_rule_id
  ON {SCHEMA}.fraud_rule_triggers (rule_id, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_rule_category
  ON {SCHEMA}.fraud_rule_triggers (rule_category, triggered_at DESC);

-- ═══════════════════════════════════════════════════════════════════════════
--  SEED: Insert representative sample rows
-- ═══════════════════════════════════════════════════════════════════════════
INSERT INTO {SCHEMA}.real_time_fraud_triage (
  transaction_id, user_id, alert_id, triggering_login_id,
  risk_score, risk_tier, model_version, risk_factors,
  detected_fraud_type, transaction_amount, transaction_type, transaction_channel,
  transaction_city, transaction_state,
  login_ip_address, login_country_code, mfa_change_flag, is_vpn, is_tor,
  minutes_after_login, automated_action, action_taken_at, action_executed_by,
  triage_status, assigned_to, source_system, pipeline_run_id, alert_generated_at
)
VALUES
  -- CRITICAL: TOR + high-value NEFT within 3 mins of login
  (
    'TXN-00100046', 'USR-007580', 'TXN-00100046', 'LOG-07970000',
    91.5, 'CRITICAL', 'fraud_v3.1',
    '[{"name":"tor_exit_node","weight":0.40,"value":1,"description":"Login via TOR exit node"},
      {"name":"high_value_wire","weight":0.35,"value":74659.89,"description":"Wire transfer > INR 50,000"},
      {"name":"foreign_login","weight":0.15,"value":"UA","description":"Login from Ukraine"},
      {"name":"quick_wire_after_login","weight":0.10,"value":3.0,"description":"Wire within 3 mins of risky login"}]'::JSONB,
    'ACCOUNT_TAKEOVER', 74659.89, 'NEFT', 'Mobile_App',
    'Bengaluru', 'Karnataka',
    '185.220.101.45', 'UA', FALSE, FALSE, TRUE,
    3.0, 'HARD_BLOCK', NOW() - INTERVAL '2 hours', 'fraud_engine_v3',
    'IN_REVIEW', 'fraud-analyst-1@bank.in', 'sv_bank_transactions_fraud_sdp', 'af570a37', NOW() - INTERVAL '2 hours'
  ),
  -- HIGH: VPN + MFA change + high-value Net_Banking
  (
    'TXN-00100041', 'USR-001324', 'TXN-00100041', 'LOG-00100001',
    72.3, 'HIGH', 'fraud_v3.1',
    '[{"name":"vpn_login","weight":0.35,"value":1,"description":"Login via known VPN IP"},
      {"name":"mfa_change","weight":0.30,"value":1,"description":"MFA settings changed before transaction"},
      {"name":"high_value_wire","weight":0.25,"value":57989.22,"description":"Wire transfer > INR 50,000"},
      {"name":"new_device","weight":0.10,"value":1,"description":"Transaction from new device"}]'::JSONB,
    'ACCOUNT_TAKEOVER', 57989.22, 'NEFT', 'Internet_Banking',
    'Mumbai', 'Maharashtra',
    '45.141.152.18', 'IN', TRUE, TRUE, FALSE,
    4.0, 'STEP_UP_AUTH', NOW() - INTERVAL '5 hours', 'fraud_engine_v3',
    'PENDING', NULL, 'sv_bank_transactions_fraud_sdp', 'af570a37', NOW() - INTERVAL '5 hours'
  ),
  -- MEDIUM: VPN domestic login, IMPS transfer
  (
    'TXN-00100051', 'USR-002634', 'TXN-00100051', 'LOG-00100002',
    44.8, 'MEDIUM', 'fraud_v3.1',
    '[{"name":"vpn_login","weight":0.40,"value":1,"description":"Login via VPN proxy"},
      {"name":"high_value_wire","weight":0.35,"value":95652.18,"description":"Wire transfer > INR 50,000"},
      {"name":"off_hours_transaction","weight":0.25,"value":2,"description":"Transaction at 2am"}]'::JSONB,
    'HIGH_VELOCITY', 95652.18, 'IMPS', 'Mobile_App',
    'Delhi', 'Delhi',
    '194.165.16.11', 'IN', FALSE, TRUE, FALSE,
    5.0, 'FLAG_FOR_REVIEW', NOW() - INTERVAL '12 hours', 'fraud_engine_v3',
    'PENDING', NULL, 'sv_bank_transactions_fraud_sdp', 'af570a37', NOW() - INTERVAL '12 hours'
  ),
  -- RESOLVED FALSE_POSITIVE: legitimate customer transfer
  (
    'TXN-00072831', 'USR-006224', 'TXN-00072831', 'LOG-00072831',
    38.0, 'MEDIUM', 'fraud_v3.1',
    '[{"name":"mfa_change","weight":0.50,"value":1,"description":"MFA settings changed"},
      {"name":"high_value_wire","weight":0.50,"value":120025.85,"description":"Wire transfer > INR 50,000"}]'::JSONB,
    'UNKNOWN', 120025.85, 'Net_Banking', 'Internet_Banking',
    'Chennai', 'Tamil Nadu',
    '103.90.180.22', 'IN', TRUE, FALSE, FALSE,
    3.0, 'NOTIFY_USER', NOW() - INTERVAL '26 hours', 'fraud_engine_v3',
    'RESOLVED', 'fraud-analyst-2@bank.in', 'sv_bank_transactions_fraud_sdp', 'af570a37', NOW() - INTERVAL '26 hours'
  )
ON CONFLICT (triage_id) DO NOTHING;

-- Update the resolved case with review outcome
UPDATE {SCHEMA}.real_time_fraud_triage
SET triage_status   = 'FALSE_ALARM',
    reviewed_by     = 'fraud-analyst-2@bank.in',
    review_outcome  = 'CUSTOMER_CONFIRMED_LEGIT',
    review_notes    = 'Customer called in. Confirmed self-initiated transfer to family account. MFA change was legitimate password rotation.',
    reviewed_at     = NOW() - INTERVAL '20 hours'
WHERE transaction_id = 'TXN-00072831';

-- Insert rule triggers for the CRITICAL case
INSERT INTO {SCHEMA}.fraud_rule_triggers
  (triage_id, transaction_id, rule_id, rule_name, rule_category, rule_version,
   triggered_value, threshold_value, contribution_score)
SELECT
  t.triage_id, 'TXN-00100046',
  r.rule_id, r.rule_name, r.rule_category::rule_category, 'v3.1',
  r.triggered_value, r.threshold_value, r.contribution_score
FROM {SCHEMA}.real_time_fraud_triage t
CROSS JOIN (
  VALUES
    ('RULE_TOR_EXIT', 'TOR Exit Node Login',     'TOR_VPN',        'TRUE',     'FALSE',    40.0),
    ('RULE_HV_WIRE',  'High-Value Wire > 50K INR','AMOUNT_THRESHOLD','74659.89','50000.00', 35.0),
    ('RULE_FOREIGN',  'Foreign Country Login',    'GEO_ANOMALY',    'UA',       'IN',       15.0),
    ('RULE_QUICK_WIRE','Wire < 5 min after Login','IP_RISK',        '3.0 min',  '60 min',   10.0)
) AS r(rule_id, rule_name, rule_category, triggered_value, threshold_value, contribution_score)
WHERE t.transaction_id = 'TXN-00100046';
""".replace("{SCHEMA}", SCHEMA)

# ── Execute ────────────────────────────────────────────────────────────────────
with psycopg.connect(conn_str) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
        print("Executing schema DDL...")
        cur.execute(DDL)
        print("DDL executed successfully.\n")

        # ── Verify ─────────────────────────────────────────────────────────
        cur.execute(f"""
            SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size
            FROM information_schema.tables
            WHERE table_schema = '{SCHEMA}'
            ORDER BY table_name
        """)
        print("Tables created:")
        for row in cur.fetchall():
            print(f"  {row[0]:<35} {row[1]}")

        cur.execute(f"""
            SELECT indexname, tablename
            FROM pg_indexes
            WHERE schemaname = '{SCHEMA}'
            ORDER BY tablename, indexname
        """)
        print("\nIndexes created:")
        for row in cur.fetchall():
            print(f"  {row[1]:<35} → {row[0]}")

        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.real_time_fraud_triage")
        print(f"\nSeed rows in real_time_fraud_triage: {cur.fetchone()[0]}")

        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.triage_audit_log")
        print(f"Audit log entries (auto-generated):  {cur.fetchone()[0]}")

        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.fraud_rule_triggers")
        print(f"Rule trigger rows:                   {cur.fetchone()[0]}")

        # ── Sample analytics queries ────────────────────────────────────────
        print("\n─── Open Cases by Risk Tier ──────────────────────────────────────────")
        cur.execute(f"""
            SELECT risk_tier, triage_status, automated_action, COUNT(*) AS cases,
                   ROUND(AVG(risk_score),1) AS avg_risk,
                   ROUND(AVG(transaction_amount),2) AS avg_amount_inr
            FROM {SCHEMA}.real_time_fraud_triage
            GROUP BY risk_tier, triage_status, automated_action
            ORDER BY avg_risk DESC
        """)
        cols = [d[0] for d in cur.description]
        print("  " + " | ".join(f"{c:<22}" for c in cols))
        print("  " + "-" * 100)
        for row in cur.fetchall():
            print("  " + " | ".join(f"{str(v):<22}" for v in row))

        print("\n─── Audit Trail for TXN-00100046 ────────────────────────────────────")
        cur.execute(f"""
            SELECT event_type, actor, actor_type,
                   new_value, logged_at
            FROM {SCHEMA}.triage_audit_log
            WHERE transaction_id = 'TXN-00100046'
            ORDER BY logged_at
        """)
        for row in cur.fetchall():
            print(f"  [{row[4].strftime('%H:%M:%S')}] {row[0]:<20} by {row[1]} ({row[2]}) → {row[3]}")

        print("\n─── Rule Contribution for TXN-00100046 ──────────────────────────────")
        cur.execute(f"""
            SELECT rule_id, rule_name, rule_category, triggered_value, threshold_value, contribution_score
            FROM {SCHEMA}.fraud_rule_triggers
            WHERE transaction_id = 'TXN-00100046'
            ORDER BY contribution_score DESC
        """)
        for row in cur.fetchall():
            print(f"  {row[0]:<25} {row[1]:<30} score={row[5]:.1f}  ({row[3]} vs threshold {row[4]})")

print(f"\n✓ Schema '{SCHEMA}' ready on instance '{INSTANCE_NAME}'")
print(f"  Host:  {instance.read_write_dns}")
print(f"  DB:    {DB_NAME}")
