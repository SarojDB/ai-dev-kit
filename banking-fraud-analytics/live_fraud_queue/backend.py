"""
Data layer for Live Fraud Queue.

Two backends:
  • Lakebase (PostgreSQL)  — fast CRUD on fraud_triage.real_time_fraud_triage
  • SQL Warehouse (Delta)  — read-only analytics (impossible travel alerts, silver/gold tables)

Auth strategy:
  - Deployed:  LAKEBASE_INSTANCE_NAME + LAKEBASE_DATABASE_NAME injected by Databricks Apps.
               App SP generates OAuth tokens via WorkspaceClient(Config()).
               Token cache refreshes every 50 min (before 1-hour expiry).
  - Local dev: SDK reads ~/.databrickscfg; same code path, no changes needed.
  - Mock mode: USE_MOCK_BACKEND=true returns static seed data for UI demos.
"""

from __future__ import annotations

import json
import os
import uuid
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

log = logging.getLogger("fraud_backend")

# ── Constants ─────────────────────────────────────────────────────────────────
LAKEBASE_SCHEMA    = "fraud_triage"
CATALOG            = "financial_security"
SILVER             = f"{CATALOG}.sv_bank_transactions_fraud_silver"
GOLD               = f"{CATALOG}.sv_bank_transactions_fraud_gold"

# Prefer env vars injected by Databricks Apps resource; fall back to local dev defaults
_LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE_NAME", "sv-fraud-triage-db")
_LAKEBASE_DB       = os.environ.get("LAKEBASE_DATABASE_NAME", "fraud_ops")
USE_MOCK           = os.environ.get("USE_MOCK_BACKEND", "false").lower() == "true"

TOKEN_REFRESH_SECS = 50 * 60   # refresh before the 1-hour token expiry


# ─────────────────────────────────────────────────────────────────────────────
# Lakebase: OAuth token cache with background refresh
# ─────────────────────────────────────────────────────────────────────────────
class _LakebaseTokenCache:
    """
    Keeps a fresh Databricks OAuth token for Lakebase in memory.
    A daemon thread refreshes it every 50 minutes so analyst sessions
    never hit the 1-hour token expiry mid-query.
    Config() auto-detects credentials: SP env vars when deployed, local
    profile when running locally.
    """

    def __init__(self):
        self._w        = WorkspaceClient(config=Config())
        self._instance = self._w.database.get_database_instance(name=_LAKEBASE_INSTANCE)
        self._username = self._w.current_user.me().user_name
        self._token    = ""
        self._lock     = threading.Lock()
        self._refresh()
        threading.Thread(target=self._refresh_loop, daemon=True).start()
        log.info("Lakebase token cache ready: instance=%s db=%s user=%s",
                 _LAKEBASE_INSTANCE, _LAKEBASE_DB, self._username)

    def _refresh(self):
        cred = self._w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[_LAKEBASE_INSTANCE],
        )
        with self._lock:
            self._token = cred.token
        log.debug("Lakebase token refreshed")

    def _refresh_loop(self):
        while True:
            time.sleep(TOKEN_REFRESH_SECS)
            try:
                self._refresh()
            except Exception as exc:
                log.error("Lakebase token refresh failed: %s", exc)

    def connect(self) -> psycopg2.extensions.connection:
        with self._lock:
            tok = self._token
        return psycopg2.connect(
            host=self._instance.read_write_dns,
            dbname=_LAKEBASE_DB,
            user=self._username,
            password=tok,
            sslmode="require",
            connect_timeout=15,
            options=f"-c search_path={LAKEBASE_SCHEMA}",
            cursor_factory=psycopg2.extras.RealDictCursor,
        )


# Lazy singleton — initialised on first DB call so the app starts fast
_token_cache: _LakebaseTokenCache | None = None
_cache_init_lock = threading.Lock()


def get_pg_conn() -> psycopg2.extensions.connection:
    """Return a live Lakebase connection, creating the token cache on first call."""
    global _token_cache
    if _token_cache is None:
        with _cache_init_lock:
            if _token_cache is None:
                _token_cache = _LakebaseTokenCache()
    return _token_cache.connect()


def _wh_conn():
    cfg   = Config()
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "199def4518ef9b3e")
    return dbsql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{wh_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Mock data — used when USE_MOCK_BACKEND=true
# ─────────────────────────────────────────────────────────────────────────────
_NOW = datetime.now(timezone.utc)

_MOCK_ROWS = [
    {
        "triage_id": "aaaaaaaa-0001-0001-0001-aaaaaaaaaaaa",
        "transaction_id": "TXN-00100046", "user_id": "USR-007580",
        "risk_score": 91.5, "risk_tier": "CRITICAL",
        "detected_fraud_type": "ACCOUNT_TAKEOVER",
        "transaction_amount": 74659.89, "transaction_type": "NEFT",
        "transaction_channel": "Mobile_App", "transaction_city": "Bengaluru",
        "transaction_state": "Karnataka", "mfa_change_flag": False,
        "is_vpn": False, "is_tor": True, "login_country_code": "UA",
        "minutes_after_login": 3.0, "automated_action": "HARD_BLOCK",
        "triage_status": "IN_REVIEW", "assigned_to": "fraud-analyst-1@bank.in",
        "escalation_reason": None,
        "review_sla_deadline": _NOW - timedelta(hours=1),
        "alert_generated_at": _NOW - timedelta(hours=2),
        "created_at": _NOW - timedelta(hours=2),
        "updated_at": _NOW - timedelta(hours=1),
    },
    {
        "triage_id": "bbbbbbbb-0002-0002-0002-bbbbbbbbbbbb",
        "transaction_id": "TXN-00100041", "user_id": "USR-001324",
        "risk_score": 72.3, "risk_tier": "HIGH",
        "detected_fraud_type": "ACCOUNT_TAKEOVER",
        "transaction_amount": 57989.22, "transaction_type": "NEFT",
        "transaction_channel": "Internet_Banking", "transaction_city": "Mumbai",
        "transaction_state": "Maharashtra", "mfa_change_flag": True,
        "is_vpn": True, "is_tor": False, "login_country_code": "IN",
        "minutes_after_login": 4.0, "automated_action": "STEP_UP_AUTH",
        "triage_status": "PENDING", "assigned_to": None,
        "escalation_reason": None,
        "review_sla_deadline": _NOW + timedelta(hours=3),
        "alert_generated_at": _NOW - timedelta(hours=5),
        "created_at": _NOW - timedelta(hours=5),
        "updated_at": _NOW - timedelta(hours=5),
    },
    {
        "triage_id": "cccccccc-0003-0003-0003-cccccccccccc",
        "transaction_id": "TXN-00100051", "user_id": "USR-002634",
        "risk_score": 44.8, "risk_tier": "MEDIUM",
        "detected_fraud_type": "HIGH_VELOCITY",
        "transaction_amount": 95652.18, "transaction_type": "IMPS",
        "transaction_channel": "Mobile_App", "transaction_city": "Delhi",
        "transaction_state": "Delhi", "mfa_change_flag": False,
        "is_vpn": True, "is_tor": False, "login_country_code": "IN",
        "minutes_after_login": 5.0, "automated_action": "FLAG_FOR_REVIEW",
        "triage_status": "PENDING", "assigned_to": None,
        "escalation_reason": None,
        "review_sla_deadline": _NOW + timedelta(hours=19),
        "alert_generated_at": _NOW - timedelta(hours=12),
        "created_at": _NOW - timedelta(hours=12),
        "updated_at": _NOW - timedelta(hours=12),
    },
    {
        "triage_id": "dddddddd-0004-0004-0004-dddddddddddd",
        "transaction_id": "TXN-00072831", "user_id": "USR-006224",
        "risk_score": 38.0, "risk_tier": "MEDIUM",
        "detected_fraud_type": "UNKNOWN",
        "transaction_amount": 120025.85, "transaction_type": "Net_Banking",
        "transaction_channel": "Internet_Banking", "transaction_city": "Chennai",
        "transaction_state": "Tamil Nadu", "mfa_change_flag": True,
        "is_vpn": False, "is_tor": False, "login_country_code": "IN",
        "minutes_after_login": 3.0, "automated_action": "NOTIFY_USER",
        "triage_status": "FALSE_ALARM", "assigned_to": "fraud-analyst-2@bank.in",
        "escalation_reason": None,
        "review_sla_deadline": _NOW - timedelta(hours=2),
        "alert_generated_at": _NOW - timedelta(hours=26),
        "created_at": _NOW - timedelta(hours=26),
        "updated_at": _NOW - timedelta(hours=20),
    },
]

_MOCK_KPIS = {
    "open_cases": 2, "critical_cases": 1, "sla_breaches": 1,
    "confirmed_fraud": 0, "false_alarms": 1, "total_cases": 4,
    "avg_risk_score": 61.7, "fpr_pct": 100.0,
}

_MOCK_DETAIL = {
    "triage": {**_MOCK_ROWS[0],
               "risk_factors": json.dumps([
                   {"name": "tor_exit_node",        "weight": 0.40, "value": 1,        "description": "Login via TOR exit node"},
                   {"name": "high_value_wire",       "weight": 0.35, "value": 74659.89, "description": "Wire transfer > INR 50,000"},
                   {"name": "foreign_login",         "weight": 0.15, "value": "UA",     "description": "Login from Ukraine"},
                   {"name": "quick_wire_after_login","weight": 0.10, "value": 3.0,      "description": "Wire within 3 mins of risky login"},
               ]),
               "review_notes": None, "reviewed_by": None,
               "reviewed_at": None, "review_outcome": None,
               "pipeline_run_id": "af570a37",
               "source_system": "sv_bank_transactions_fraud_sdp"},
    "audit": [
        {"event_type": "TRIAGE_CREATED",  "actor": "fraud_engine",           "actor_type": "SYSTEM",
         "old_value": None, "new_value": {"triage_status": "PENDING"},        "logged_at": _NOW - timedelta(hours=2)},
        {"event_type": "STATUS_CHANGE",   "actor": "fraud-analyst-1@bank.in","actor_type": "HUMAN",
         "old_value": {"triage_status": "PENDING"},
         "new_value": {"triage_status": "IN_REVIEW"},                         "logged_at": _NOW - timedelta(hours=1)},
    ],
    "rules": [
        {"rule_id": "RULE_TOR_EXIT",   "rule_name": "TOR Exit Node Login",       "rule_category": "TOR_VPN",
         "triggered_value": "TRUE",   "threshold_value": "FALSE",  "contribution_score": 40.0},
        {"rule_id": "RULE_HV_WIRE",    "rule_name": "High-Value Wire > 50K INR", "rule_category": "AMOUNT_THRESHOLD",
         "triggered_value": "74659",  "threshold_value": "50000",  "contribution_score": 35.0},
        {"rule_id": "RULE_FOREIGN",    "rule_name": "Foreign Country Login",     "rule_category": "GEO_ANOMALY",
         "triggered_value": "UA",     "threshold_value": "IN",     "contribution_score": 15.0},
        {"rule_id": "RULE_QUICK_WIRE", "rule_name": "Wire < 5 min after Login",  "rule_category": "IP_RISK",
         "triggered_value": "3 min",  "threshold_value": "60 min", "contribution_score": 10.0},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Queue reads
# ─────────────────────────────────────────────────────────────────────────────
QUEUE_COLS = """
    triage_id::text, transaction_id, user_id,
    risk_score, risk_tier,
    detected_fraud_type::text,
    transaction_amount, transaction_type, transaction_channel,
    transaction_city, transaction_state,
    mfa_change_flag, is_vpn, is_tor,
    login_country_code,
    minutes_after_login,
    automated_action::text, triage_status::text,
    assigned_to, escalation_reason,
    review_sla_deadline,
    alert_generated_at, created_at, updated_at
"""


def fetch_queue(
    status_filter: list[str] | None = None,
    risk_filter:   list[str] | None = None,
    limit: int = 200,
) -> list[dict]:
    if USE_MOCK:
        rows = _MOCK_ROWS
        if status_filter:
            rows = [r for r in rows if r["triage_status"] in status_filter]
        if risk_filter:
            rows = [r for r in rows if r["risk_tier"] in risk_filter]
        return rows[:limit]

    where, params = [], []
    if status_filter:
        where.append(f"triage_status::text IN ({','.join(['%s']*len(status_filter))})")
        params += status_filter
    if risk_filter:
        where.append(f"risk_tier IN ({','.join(['%s']*len(risk_filter))})")
        params += risk_filter

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT {QUEUE_COLS}
                FROM {LAKEBASE_SCHEMA}.real_time_fraud_triage
                {where_sql}
                ORDER BY risk_score DESC, alert_generated_at DESC
                LIMIT {limit}
            """, params)
            return [dict(r) for r in cur.fetchall()]


def fetch_queue_kpis() -> dict:
    if USE_MOCK:
        return _MOCK_KPIS

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE triage_status IN ('PENDING','IN_REVIEW','ESCALATED'))         AS open_cases,
                    COUNT(*) FILTER (WHERE triage_status IN ('PENDING','IN_REVIEW','ESCALATED')
                                      AND risk_tier = 'CRITICAL')                                        AS critical_cases,
                    COUNT(*) FILTER (WHERE review_sla_deadline < NOW()
                                      AND triage_status NOT IN ('RESOLVED','CLOSED','FALSE_ALARM'))       AS sla_breaches,
                    COUNT(*) FILTER (WHERE triage_status = 'FALSE_ALARM')                                AS false_alarms,
                    COUNT(*) FILTER (WHERE triage_status = 'RESOLVED'
                                      AND review_outcome = 'CONFIRMED_FRAUD')                            AS confirmed_fraud,
                    COUNT(*)                                                                              AS total_cases,
                    ROUND(AVG(risk_score)::numeric, 1)                                                   AS avg_risk_score
                FROM {LAKEBASE_SCHEMA}.real_time_fraud_triage
            """)
            row = dict(cur.fetchone())
    total_closed = (row["false_alarms"] or 0) + (row["confirmed_fraud"] or 0)
    row["fpr_pct"] = round(row["false_alarms"] / total_closed * 100, 1) if total_closed > 0 else 0.0
    return row


# ─────────────────────────────────────────────────────────────────────────────
# Case detail
# ─────────────────────────────────────────────────────────────────────────────
def fetch_case_detail(transaction_id: str) -> dict:
    if USE_MOCK:
        return _MOCK_DETAIL

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT *, triage_status::text, automated_action::text,
                       detected_fraud_type::text, review_outcome::text
                FROM {LAKEBASE_SCHEMA}.real_time_fraud_triage
                WHERE transaction_id = %s LIMIT 1
            """, (transaction_id,))
            triage = dict(cur.fetchone() or {})

            cur.execute(f"""
                SELECT event_type, actor, actor_type::text,
                       old_value, new_value, logged_at
                FROM {LAKEBASE_SCHEMA}.triage_audit_log
                WHERE transaction_id = %s ORDER BY logged_at DESC LIMIT 20
            """, (transaction_id,))
            audit = [dict(r) for r in cur.fetchall()]

            cur.execute(f"""
                SELECT rule_id, rule_name, rule_category::text,
                       triggered_value, threshold_value, contribution_score
                FROM {LAKEBASE_SCHEMA}.fraud_rule_triggers
                WHERE transaction_id = %s ORDER BY contribution_score DESC
            """, (transaction_id,))
            rules = [dict(r) for r in cur.fetchall()]

    return {"triage": triage, "audit": audit, "rules": rules}


def fetch_impossible_travel_context(user_id: str) -> list[dict]:
    try:
        with _wh_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT prev_city, curr_city, distance_miles, time_gap_minutes,
                           implied_speed_mph, risk_score, amount, transaction_type,
                           curr_event_timestamp
                    FROM {SILVER}.silver_impossible_travel_alerts
                    WHERE user_id = ? ORDER BY curr_event_timestamp DESC LIMIT 5
                """, (user_id,))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        log.warning("Impossible travel context unavailable: %s", exc)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Case actions
# ─────────────────────────────────────────────────────────────────────────────
def _update_case(transaction_id: str, updates: dict, analyst: str) -> bool:
    if USE_MOCK:
        log.info("[MOCK] update_case %s: %s", transaction_id, updates)
        return True
    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values     = list(updates.values()) + [analyst, transaction_id]
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {LAKEBASE_SCHEMA}.real_time_fraud_triage
                SET {set_clause}, reviewed_by = %s, reviewed_at = NOW()
                WHERE transaction_id = %s
            """, values)
        conn.commit()
    return True


def release_case(transaction_id: str, notes: str, analyst: str) -> bool:
    return _update_case(transaction_id, {
        "triage_status":  "FALSE_ALARM",
        "review_outcome": "CUSTOMER_CONFIRMED_LEGIT",
        "review_notes":   notes or "Released by analyst — false positive.",
    }, analyst)


def confirm_fraud(transaction_id: str, notes: str, analyst: str) -> bool:
    return _update_case(transaction_id, {
        "triage_status":  "RESOLVED",
        "review_outcome": "CONFIRMED_FRAUD",
        "review_notes":   notes or "Confirmed fraud by analyst.",
    }, analyst)


def escalate_case(transaction_id: str, reason: str, analyst: str) -> bool:
    return _update_case(transaction_id, {
        "triage_status":     "ESCALATED",
        "escalation_reason": reason or "Escalated for further review.",
    }, analyst)


def assign_case(transaction_id: str, assignee: str, analyst: str) -> bool:
    return _update_case(transaction_id, {
        "assigned_to":   assignee,
        "triage_status": "IN_REVIEW",
    }, analyst)
