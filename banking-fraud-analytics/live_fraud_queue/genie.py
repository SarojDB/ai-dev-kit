"""
Genie Conversation API client for the Live Fraud Queue app.

Wraps the Databricks Genie REST API to provide a streaming-friendly interface:
  start_conversation(question)  → (conversation_id, message_id, result)
  send_message(conv_id, text)   → (message_id, result)
  get_message_result(...)       → GenieResult

Uses Config() so it works both locally (profile) and deployed (SP env vars).
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field
from typing import Any

import requests
from databricks.sdk.core import Config

log = logging.getLogger("genie")

# ── Genie Space ───────────────────────────────────────────────────────────────
SPACE_ID      = "01f118649c9413ec89adfb36436d439a"   # Banking Fraud Investigation Hub
POLL_INTERVAL = 1.5   # seconds between status polls
MAX_WAIT_SECS = 120   # give up after 2 minutes


@dataclass
class GenieResult:
    question:        str = ""
    conversation_id: str = ""
    message_id:      str = ""
    status:          str = ""          # COMPLETED | FAILED | CANCELLED | TIMEOUT
    sql:             str = ""
    columns:         list[str] = field(default_factory=list)
    data:            list[list] = field(default_factory=list)
    row_count:       int = 0
    text_response:   str = ""
    error:           str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────
def _session() -> tuple[requests.Session, str]:
    """Return an authenticated requests.Session and the workspace host."""
    cfg = Config()
    sess = requests.Session()
    sess.headers.update(cfg.authenticate())
    sess.headers["Content-Type"] = "application/json"
    return sess, cfg.host


def _poll_message(sess: requests.Session, host: str,
                  conv_id: str, msg_id: str) -> dict:
    """Poll the message endpoint until a terminal state is reached."""
    url     = f"{host}/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conv_id}/messages/{msg_id}"
    elapsed = 0
    while elapsed < MAX_WAIT_SECS:
        r    = sess.get(url)
        r.raise_for_status()
        body = r.json()
        state = body.get("status", "")
        if state in ("COMPLETED", "FAILED", "CANCELLED"):
            return body
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    return {"status": "TIMEOUT"}


def _extract_result(sess: requests.Session, host: str,
                    conv_id: str, msg_id: str, msg_body: dict,
                    question: str) -> GenieResult:
    """Parse a terminal message body into a GenieResult."""
    status = msg_body.get("status", "")

    if status != "COMPLETED":
        return GenieResult(
            question=question,
            conversation_id=conv_id,
            message_id=msg_id,
            status=status,
            error=msg_body.get("error", {}).get("message", status),
        )

    # Text-only response (e.g. clarification request, or narrative answer)
    attachments = msg_body.get("attachments", [])
    text_response = ""
    sql_text      = ""
    columns: list[str] = []
    data:    list[list] = []

    for att in attachments:
        if att.get("type") == "text":
            text_response = att.get("text", {}).get("content", "")
        elif att.get("type") == "query":
            qry      = att.get("query", {})
            sql_text = qry.get("query", "")

    # Fetch query results if there is SQL
    row_count = 0
    if sql_text:
        qr_url = (f"{host}/api/2.0/genie/spaces/{SPACE_ID}"
                  f"/conversations/{conv_id}/messages/{msg_id}/query-result")
        qr = sess.get(qr_url)
        if qr.status_code == 200:
            result_body = qr.json().get("statement_response", {})
            schema      = result_body.get("manifest", {}).get("schema", {})
            columns     = [c.get("name", "") for c in schema.get("columns", [])]
            rows_data   = result_body.get("result", {}).get("data_typed_array", [])
            data        = [[v.get("str", "") for v in row.get("values", [])]
                           for row in rows_data]
            row_count   = len(data)

    return GenieResult(
        question=question,
        conversation_id=conv_id,
        message_id=msg_id,
        status="COMPLETED",
        sql=sql_text,
        columns=columns,
        data=data,
        row_count=row_count,
        text_response=text_response,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────
def start_conversation(question: str) -> GenieResult:
    """
    Start a new Genie conversation with the given question.
    Returns a GenieResult with the answer, generated SQL, and data rows.
    """
    sess, host = _session()
    url = f"{host}/api/2.0/genie/spaces/{SPACE_ID}/start-conversation"
    r   = sess.post(url, json={"content": question})
    r.raise_for_status()
    body    = r.json()
    conv_id = body.get("conversation_id", "")
    msg_id  = body.get("message_id", "")

    log.info("Genie conversation started: conv=%s msg=%s", conv_id, msg_id)
    msg_body = _poll_message(sess, host, conv_id, msg_id)
    return _extract_result(sess, host, conv_id, msg_id, msg_body, question)


def send_followup(conversation_id: str, question: str) -> GenieResult:
    """
    Continue an existing Genie conversation with a follow-up question.
    """
    sess, host = _session()
    url = (f"{host}/api/2.0/genie/spaces/{SPACE_ID}"
           f"/conversations/{conversation_id}/messages")
    r   = sess.post(url, json={"content": question})
    r.raise_for_status()
    body   = r.json()
    msg_id = body.get("message_id", "")

    log.info("Genie follow-up: conv=%s msg=%s", conversation_id, msg_id)
    msg_body = _poll_message(sess, host, conversation_id, msg_id)
    return _extract_result(sess, host, conversation_id, msg_id, msg_body, question)


# ─────────────────────────────────────────────────────────────────────────────
# Suggested questions (shown as quick-start chips)
# ─────────────────────────────────────────────────────────────────────────────
SUGGESTED_QUESTIONS = [
    "Show me all HIGH and CRITICAL wire transfer alerts from TOR or foreign IPs",
    "What is the False Positive Ratio for high-value wire transfer alerts?",
    "Which users have the highest fraud velocity score this month?",
    "What is the Account Takeover Rate broken down by credit tier?",
    "Show the daily trend of wire transfer fraud alerts in February 2026",
    "Which states have the highest wire fraud risk score?",
    "What is the wire fraud rate for NEFT vs RTGS vs IMPS?",
    "Show top 10 merchants by total confirmed fraud amount",
]
