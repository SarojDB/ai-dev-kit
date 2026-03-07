#!/usr/bin/env python3
"""
Configure the Banking Fraud Investigation Hub Genie Space.
Uses Databricks API to add tables, instructions, sample questions, and certified SQL.

Run: python scripts/configure_genie_fraud_space.py
Requires: Databricks profile configured (databricks auth login) or DATABRICKS_HOST + DATABRICKS_TOKEN
"""

import sys
from pathlib import Path

# Add repo root for imports
repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root))

try:
    from databricks_tools_core.agent_bricks import AgentBricksManager
except ImportError:
    # Fallback: install path for databricks-tools-core
    tools_path = repo_root / "databricks-tools-core"
    if tools_path.exists():
        sys.path.insert(0, str(tools_path))
    from databricks_tools_core.agent_bricks import AgentBricksManager

SPACE_ID = "01f118649c9413ec89adfb36436d439a"
WORKSPACE_URL = "https://dbc-8debdea9-18d5.cloud.databricks.com"

TABLES = [
    "financial_security.sv_bank_transactions_fraud_silver.silver_transactions",
    "financial_security.sv_bank_transactions_fraud_silver.silver_login_logs",
    "financial_security.sv_bank_transactions_fraud_silver.silver_wire_transfer_ip_alerts",
    "financial_security.sv_bank_transactions_fraud_gold.fact_transactions",
    "financial_security.sv_bank_transactions_fraud_gold.dim_user",
    "financial_security.sv_bank_transactions_fraud_gold.dim_merchant",
    "financial_security.sv_bank_transactions_fraud_gold.agg_fraud_velocity_by_user",
    "financial_security.sv_bank_transactions_fraud_gold.agg_high_value_wire_risk",
]

TITLE = "Banking Fraud Investigation Hub"
DESCRIPTION = """AI-powered fraud investigation workspace for banking wire transfer fraud, account takeovers, and login anomalies. Covers 100K+ transactions across February 2026. Use this space to triage alerts, investigate suspicious users, and compute fraud KPIs."""

INSTRUCTIONS = """## Fraud Investigation Assistant

### Business Rules
1. Wire transfer = transaction_type IN ('NEFT','RTGS','IMPS','Net_Banking')
2. High-value wire = is_high_value_wire = true (amount > INR 50,000 AND is wire type)
3. IP risk login = mfa_change_flag = true OR is_vpn = true OR is_tor = true OR country_code != 'IN'
4. Alert window = 60 minutes between risky login and subsequent wire transfer for same user_id
5. Off-hours = txn_hour_of_day < 6 OR txn_hour_of_day > 22

### KPI Definitions
- False Positive Ratio: flagged wires that are NOT fraud / total flagged wires * 100
- Account Takeover Rate: transactions with fraud_type='Account_Takeover' / total transactions * 100
- Wire Fraud Rate: fraud wire transactions / all wire transactions * 100
- Alert Precision: alerts where is_fraud=true / total alerts * 100

### Query Tips
- Use event_timestamp for time filters (not created_timestamp)
- Use silver_wire_transfer_ip_alerts for pre-computed alert queries
- Join transactions to users via user_id to get credit_tier and KYC status
- Use agg_fraud_velocity_by_user for user-level risk profiles
- Use agg_high_value_wire_risk for geographic risk hotspot analysis"""

SAMPLE_QUESTIONS = [
    "Show me all wire transfers over INR 50,000 where the user changed their MFA settings in the last 24 hours",
    "Which users have the highest fraud velocity score this month?",
    "What is the False Positive Ratio — what percentage of flagged wire transfers were not actually fraud?",
    "List all Account Takeover alerts by city and alert risk level",
    "Show all HIGH and CRITICAL risk alerts from TOR exit nodes or foreign IP addresses",
    "Which users had 3 or more failed login attempts followed by a successful wire transfer over INR 10,000?",
    "What is the Account Takeover Rate broken down by credit tier?",
    "Show the daily trend of fraud alerts in February 2026",
    "Which states have the highest wire fraud risk score?",
    "Find all users who logged in from outside India and made a wire transfer within 2 hours",
]

CERTIFIED_SQL = [
    {
        "title": "False Positive Ratio (FPR)",
        "content": """SELECT
  COUNT(*) AS total_high_value_wires,
  SUM(CASE WHEN is_fraud = false THEN 1 ELSE 0 END) AS false_positives,
  SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) AS true_positives,
  ROUND(SUM(CASE WHEN is_fraud = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS false_positive_ratio_pct,
  ROUND(SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS detection_precision_pct
FROM financial_security.sv_bank_transactions_fraud_silver.silver_transactions
WHERE is_high_value_wire = true""",
    },
    {
        "title": "Account Takeover Rate by Credit Tier",
        "content": """SELECT
  u.credit_tier,
  COUNT(DISTINCT t.user_id) AS total_users,
  COUNT(t.transaction_id) AS total_transactions,
  SUM(CASE WHEN t.fraud_type = 'Account_Takeover' THEN 1 ELSE 0 END) AS account_takeover_count,
  ROUND(SUM(CASE WHEN t.fraud_type = 'Account_Takeover' THEN 1 ELSE 0 END) * 100.0 / COUNT(t.transaction_id), 4) AS account_takeover_rate_pct
FROM financial_security.sv_bank_transactions_fraud_gold.fact_transactions t
JOIN financial_security.sv_bank_transactions_fraud_gold.dim_user u ON t.user_id = u.user_id
GROUP BY u.credit_tier
ORDER BY account_takeover_rate_pct DESC""",
    },
]


def main():
    print("Configuring Genie Space...")
    print(f"Space ID: {SPACE_ID}")
    print(f"Workspace: {WORKSPACE_URL}")
    print()

    try:
        manager = AgentBricksManager()
    except Exception as e:
        print(f"ERROR: Failed to create Databricks client: {e}")
        print("Ensure you have run 'databricks auth login' or set DATABRICKS_HOST and DATABRICKS_TOKEN")
        return 1

    # Step 1 & 2: Verify space exists and update tables, title, description, sample questions
    print("Step 1-2: Updating space (tables, title, description, sample questions)...")
    space = manager.genie_get(SPACE_ID)
    if not space:
        print(f"ERROR: Genie space {SPACE_ID} not found")
        return 1

    manager.genie_update(
        space_id=SPACE_ID,
        display_name=TITLE,
        description=DESCRIPTION,
        table_identifiers=TABLES,
        sample_questions=SAMPLE_QUESTIONS,
    )
    print("  ✓ Space updated successfully")

    # Step 4: Add instructions
    print("Step 4: Adding instructions...")
    manager.genie_add_text_instruction(
        space_id=SPACE_ID,
        content=INSTRUCTIONS,
        title="Fraud Investigation Assistant",
    )
    print("  ✓ Instructions added")

    # Step 6: Add certified SQL queries
    print("Step 6: Adding certified SQL queries...")
    manager.genie_add_sql_instructions_batch(SPACE_ID, CERTIFIED_SQL)
    print("  ✓ Certified SQL added")

    print()
    print("Configuration complete!")
    print(f"URL: {WORKSPACE_URL}/genie/rooms/{SPACE_ID}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
