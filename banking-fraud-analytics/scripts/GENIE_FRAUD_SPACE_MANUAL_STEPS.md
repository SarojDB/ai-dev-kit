# Banking Fraud Investigation Hub - Manual Configuration Guide

Genie Space ID: `01f118649c9413ec89adfb36436d439a`  
URL: https://dbc-8debdea9-18d5.cloud.databricks.com/genie/rooms/01f118649c9413ec89adfb36436d439a

## Step 1: Open the Genie Space
1. Go to: https://dbc-8debdea9-18d5.cloud.databricks.com/genie/rooms/01f118649c9413ec89adfb36436d439a
2. Log in if prompted

## Step 2: Add Tables (Edit Space → Configure Data)
Click **Edit Space** or **Settings** → **Configure Data**, then add these 8 tables:
- `financial_security.sv_bank_transactions_fraud_silver.silver_transactions`
- `financial_security.sv_bank_transactions_fraud_silver.silver_login_logs`
- `financial_security.sv_bank_transactions_fraud_silver.silver_wire_transfer_ip_alerts`
- `financial_security.sv_bank_transactions_fraud_gold.fact_transactions`
- `financial_security.sv_bank_transactions_fraud_gold.dim_user`
- `financial_security.sv_bank_transactions_fraud_gold.dim_merchant`
- `financial_security.sv_bank_transactions_fraud_gold.agg_fraud_velocity_by_user`
- `financial_security.sv_bank_transactions_fraud_gold.agg_high_value_wire_risk`

## Step 3: Update Title & Description
- **Title**: Banking Fraud Investigation Hub
- **Description**: AI-powered fraud investigation workspace for banking wire transfer fraud, account takeovers, and login anomalies. Covers 100K+ transactions across February 2026. Use this space to triage alerts, investigate suspicious users, and compute fraud KPIs.

## Step 4: Add Instructions
In the **Instructions** section, add the Fraud Investigation Assistant content (see script or below).

## Step 5: Add Sample Questions
In the **Sample Questions** section, add the 10 questions (see script).

## Step 6: Add Certified SQL
In **Curated SQL** or **Certified Questions**, add:
1. "False Positive Ratio (FPR)" – SQL from script
2. "Account Takeover Rate by Credit Tier" – SQL from script
