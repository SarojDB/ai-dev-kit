"""
Reusable helper to query the fraud_triage Lakebase schema.

Usage (any Python environment — local, Databricks notebook, job):
    from scripts.query_fraud_triage import FraudTriageDB
    db = FraudTriageDB()
    df = db.query("SELECT * FROM fraud_triage.real_time_fraud_triage ORDER BY risk_score DESC")
    print(df)

The token is refreshed automatically every 50 minutes so it works in
long-running notebooks and jobs (Databricks tokens expire after 1 hour).
"""

from __future__ import annotations
import os
import uuid
import time
import threading
import configparser
from typing import Optional
import psycopg
import pandas as pd
from databricks.sdk import WorkspaceClient

INSTANCE_NAME = "sv-fraud-triage-db"
DB_NAME       = "fraud_ops"
SCHEMA        = "fraud_triage"


# ─────────────────────────────────────────────────────────────────────────────
class FraudTriageDB:
    """
    Thread-safe connection helper for the fraud_triage Lakebase schema.
    Auto-refreshes OAuth tokens before the 1-hour expiry.
    """

    def __init__(self, profile: str = "databricks_oneenv"):
        cfg = configparser.ConfigParser()
        cfg.read(os.path.expanduser("~/.databrickscfg"))
        host  = cfg[profile]["host"]
        token = cfg[profile]["token"]
        self._w        = WorkspaceClient(host=host, token=token)
        self._instance = self._w.database.get_database_instance(name=INSTANCE_NAME)
        self._username = self._w.current_user.me().user_name
        self._token    : str = ""
        self._lock     = threading.Lock()
        self._refresh_token()
        # Background thread refreshes every 50 min
        self._start_refresh_loop()

    # ── Connection ────────────────────────────────────────────────────────────
    def _refresh_token(self):
        cred = self._w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[INSTANCE_NAME],
        )
        with self._lock:
            self._token = cred.token

    def _start_refresh_loop(self):
        def loop():
            while True:
                time.sleep(50 * 60)
                self._refresh_token()
        t = threading.Thread(target=loop, daemon=True)
        t.start()

    def _conn_str(self) -> str:
        with self._lock:
            tok = self._token
        return (
            f"host={self._instance.read_write_dns} "
            f"dbname={DB_NAME} "
            f"user={self._username} "
            f"password={tok} "
            f"sslmode=require "
            f"connect_timeout=15 "
            f"options=-csearch_path={SCHEMA}"
        )

    # ── Public API ────────────────────────────────────────────────────────────
    def query(self, sql: str, params=None) -> pd.DataFrame:
        """Run a SELECT and return a pandas DataFrame."""
        with psycopg.connect(self._conn_str()) as conn:
            return pd.read_sql(sql, conn, params=params)

    def execute(self, sql: str, params=None) -> int:
        """Run INSERT / UPDATE / DELETE; returns rowcount."""
        with psycopg.connect(self._conn_str()) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
            return cur.rowcount

    # ── Pre-built analyst queries ─────────────────────────────────────────────
    def open_cases(self, min_risk_score: float = 0) -> pd.DataFrame:
        """All open triage cases above a given risk score, newest first."""
        return self.query(f"""
            SELECT triage_id, transaction_id, user_id,
                   risk_tier, risk_score, automated_action, triage_status,
                   transaction_amount, transaction_type, transaction_city,
                   login_country_code, mfa_change_flag, is_vpn, is_tor,
                   minutes_after_login, review_sla_deadline, assigned_to
            FROM {SCHEMA}.real_time_fraud_triage
            WHERE triage_status IN ('PENDING','IN_REVIEW','ESCALATED')
              AND risk_score >= {min_risk_score}
            ORDER BY risk_score DESC, alert_generated_at DESC
        """)

    def sla_breaches(self) -> pd.DataFrame:
        """Cases that have missed their SLA deadline."""
        return self.query(f"""
            SELECT transaction_id, user_id, risk_tier, triage_status,
                   review_sla_deadline,
                   ROUND(EXTRACT(EPOCH FROM (NOW() - review_sla_deadline)) / 3600, 1)
                     AS hours_overdue,
                   assigned_to
            FROM {SCHEMA}.real_time_fraud_triage
            WHERE review_sla_deadline < NOW()
              AND triage_status NOT IN ('RESOLVED','CLOSED','FALSE_ALARM')
            ORDER BY review_sla_deadline ASC
        """)

    def case_detail(self, transaction_id: str) -> dict:
        """Full case + audit trail + rule triggers for one transaction."""
        triage = self.query(f"""
            SELECT * FROM {SCHEMA}.real_time_fraud_triage
            WHERE transaction_id = %s
        """, (transaction_id,))

        audit = self.query(f"""
            SELECT event_type, actor, actor_type, old_value, new_value, logged_at
            FROM {SCHEMA}.triage_audit_log
            WHERE transaction_id = %s
            ORDER BY logged_at
        """, (transaction_id,))

        rules = self.query(f"""
            SELECT rule_id, rule_name, rule_category,
                   triggered_value, threshold_value, contribution_score
            FROM {SCHEMA}.fraud_rule_triggers
            WHERE transaction_id = %s
            ORDER BY contribution_score DESC
        """, (transaction_id,))

        return {"triage": triage, "audit": audit, "rules": rules}

    def kpi_summary(self) -> pd.DataFrame:
        """KPI dashboard: FPR, action distribution, avg risk by tier."""
        return self.query(f"""
            SELECT
                risk_tier,
                COUNT(*)                                                   AS total_cases,
                ROUND(AVG(risk_score)::numeric, 1)                        AS avg_risk_score,
                ROUND(AVG(transaction_amount)::numeric, 2)                AS avg_amount_inr,
                COUNT(*) FILTER (WHERE triage_status = 'PENDING')         AS pending,
                COUNT(*) FILTER (WHERE triage_status = 'IN_REVIEW')       AS in_review,
                COUNT(*) FILTER (WHERE triage_status = 'FALSE_ALARM')     AS false_alarms,
                ROUND(
                    COUNT(*) FILTER (WHERE triage_status = 'FALSE_ALARM') * 100.0
                    / NULLIF(COUNT(*), 0), 1
                )                                                          AS false_positive_rate_pct
            FROM {SCHEMA}.real_time_fraud_triage
            GROUP BY risk_tier
            ORDER BY avg_risk_score DESC
        """)

    def update_case_status(
        self,
        transaction_id: str,
        new_status: str,
        reviewed_by: str,
        outcome: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> int:
        """Analyst closes / resolves a case. Audit trigger fires automatically."""
        return self.execute(f"""
            UPDATE {SCHEMA}.real_time_fraud_triage
            SET triage_status  = %s,
                reviewed_by    = %s,
                review_outcome = %s,
                review_notes   = %s,
                reviewed_at    = NOW()
            WHERE transaction_id = %s
        """, (new_status, reviewed_by, outcome, notes, transaction_id))


# ─────────────────────────────────────────────────────────────────────────────
# Quick smoke-test when run directly
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    db = FraudTriageDB()

    print("── Open Cases ────────────────────────────────────────────────────")
    print(db.open_cases().to_string(index=False))

    print("\n── SLA Breaches ─────────────────────────────────────────────────")
    print(db.sla_breaches().to_string(index=False))

    print("\n── KPI Summary ──────────────────────────────────────────────────")
    print(db.kpi_summary().to_string(index=False))

    print("\n── Case Detail: TXN-00100046 ────────────────────────────────────")
    detail = db.case_detail("TXN-00100046")
    print("Triage:\n",  detail["triage"].to_string(index=False))
    print("Rules:\n",   detail["rules"].to_string(index=False))
    print("Audit:\n",   detail["audit"][["event_type","actor","actor_type","new_value","logged_at"]].to_string(index=False))
