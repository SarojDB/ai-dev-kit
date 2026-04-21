#!/usr/bin/env python3
"""
Run APJ STS completed ASQ aggregates (rolling 6 months) from the lakehouse.

Companion: asq-completions-apj-sts-rolling-6m.md

Usage:
  python3 run_asq_completions.py
  python3 run_asq_completions.py --output-dir ./out --charts
  DATABRICKS_SQL_WAREHOUSE_ID=... python3 run_asq_completions.py

Requires: databricks-sdk. Authentication is **Databricks CLI OAuth** (`auth_type=databricks-cli`): run `databricks auth login --host <workspace-url>` for the profile you use (default: DEFAULT). Optional `--profile` or `DATABRICKS_CONFIG_PROFILE`. Do not set `DATABRICKS_TOKEN` if you want CLI-only.
Charts: pip install matplotlib pandas. With --charts, writes `asq_detail.csv`, **`asq_completions_all_engineers_monthly.png`** (all engineers on one chart, grouped monthly bars), per-engineer monthly PNGs, and **`asq_completions_team_all_members.png`** (all engineers, weekly grouped bars).
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# Salesforce display names — keep in sync with asq-completions-apj-sts-rolling-6m.md
APJ_STS_NAMES = (
    "Louis Chen",
    "Pui-Ching Lee",
    "ADARSH NANDAN",  # Salesforce User.Name is all-caps; "Adarsh Nandan" does not match
    "Hemapriya N",
    "Kavya Parashar",
    "Simran Vanjani",
    "Haley Won",
    "Yotaro Enomoto",
    "Anwesha Ghosh",
    "Hemanth Rishi",
)

STS_RECORD_TYPE_ID = "0128Y000001h44DQAQ"
DEFAULT_CATALOG = "main"
POLL_INTERVAL_SEC = 2
POLL_MAX_ROUNDS = 180

# Six calendar months ending in the current month (same month boundaries as SQL below).
ROLLING_ASQ_FILTER_SQL = (
    "CAST(ar.LastModifiedDate AS DATE) >= CAST(DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE(), -5)) AS DATE)"
)


def base_cte() -> str:
    def esc(s: str) -> str:
        return s.replace("'", "''")

    names_sql = ",\n    ".join(f"'{esc(n)}'" for n in APJ_STS_NAMES)
    return f"""
WITH latest_ar AS (
  SELECT * FROM main.sfdc_bronze.approvalrequest__c
  WHERE processDate = (SELECT MAX(processDate) FROM main.sfdc_bronze.approvalrequest__c)
),
latest_u AS (
  SELECT * FROM main.sfdc_bronze.user
  WHERE processDate = (SELECT MAX(processDate) FROM main.sfdc_bronze.user)
),
apj_sts AS (
  SELECT Id FROM latest_u
  WHERE Name IN (
    {names_sql}
  )
)
"""


def build_sql_totals(cte: str) -> str:
    return cte.strip() + f"""
SELECT u.Name AS engineer_name, COUNT(*) AS completed_asqs_total
FROM latest_ar ar
JOIN latest_u u ON ar.OwnerId = u.Id
WHERE ar.RecordTypeId IN ('{STS_RECORD_TYPE_ID}')
  AND ar.Status__c IN ('Complete', 'Completed')
  AND ar.OwnerId IN (SELECT Id FROM apj_sts)
  AND __ROLLING_FILTER__
GROUP BY u.Name
ORDER BY completed_asqs_total DESC
""".replace("__ROLLING_FILTER__", ROLLING_ASQ_FILTER_SQL)


def build_sql_weekly(cte: str) -> str:
    return cte.strip() + """
SELECT
  u.Name AS engineer_name,
  DATE_TRUNC('week', CAST(ar.LastModifiedDate AS TIMESTAMP)) AS week_start,
  COUNT(*) AS completed_asqs
FROM latest_ar ar
JOIN latest_u u ON ar.OwnerId = u.Id
WHERE ar.RecordTypeId IN ('__RT__')
  AND ar.Status__c IN ('Complete', 'Completed')
  AND ar.OwnerId IN (SELECT Id FROM apj_sts)
  AND __ROLLING_FILTER__
GROUP BY u.Name, DATE_TRUNC('week', CAST(ar.LastModifiedDate AS TIMESTAMP))
ORDER BY engineer_name, week_start
""".replace("__RT__", STS_RECORD_TYPE_ID).replace("__ROLLING_FILTER__", ROLLING_ASQ_FILTER_SQL)


def build_sql_monthly(cte: str) -> str:
    return cte.strip() + """
SELECT
  u.Name AS engineer_name,
  DATE_TRUNC('month', CAST(ar.LastModifiedDate AS TIMESTAMP)) AS month_start,
  COUNT(*) AS completed_asqs
FROM latest_ar ar
JOIN latest_u u ON ar.OwnerId = u.Id
WHERE ar.RecordTypeId IN ('__RT__')
  AND ar.Status__c IN ('Complete', 'Completed')
  AND ar.OwnerId IN (SELECT Id FROM apj_sts)
  AND __ROLLING_FILTER__
GROUP BY u.Name, DATE_TRUNC('month', CAST(ar.LastModifiedDate AS TIMESTAMP))
ORDER BY engineer_name, month_start
""".replace("__RT__", STS_RECORD_TYPE_ID).replace("__ROLLING_FILTER__", ROLLING_ASQ_FILTER_SQL)


def build_sql_detail(cte: str) -> str:
    """One row per completed ASQ (for per-engineer bar charts)."""
    return cte.strip() + f"""
SELECT
  u.Name AS engineer_name,
  ar.Name AS asq_name,
  CAST(ar.LastModifiedDate AS TIMESTAMP) AS completion_ts
FROM latest_ar ar
JOIN latest_u u ON ar.OwnerId = u.Id
WHERE ar.RecordTypeId IN ('{STS_RECORD_TYPE_ID}')
  AND ar.Status__c IN ('Complete', 'Completed')
  AND ar.OwnerId IN (SELECT Id FROM apj_sts)
  AND __ROLLING_FILTER__
ORDER BY u.Name, ar.LastModifiedDate, ar.Name
""".replace("__ROLLING_FILTER__", ROLLING_ASQ_FILTER_SQL)


def execute_sql(
    client: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    *,
    catalog: str,
    timeout_loops: int = POLL_MAX_ROUNDS,
) -> tuple[list[str], list[list[str]]]:
    resp = client.statement_execution.execute_statement(
        statement=sql.strip(),
        warehouse_id=warehouse_id,
        catalog=catalog,
        wait_timeout="0s",
    )
    for _ in range(timeout_loops):
        st = resp.status.state
        if st == StatementState.SUCCEEDED:
            cols = [c.name for c in resp.manifest.schema.columns]
            rows = [list(r) for r in (resp.result.data_array or [])]
            return cols, rows
        if st in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            err = resp.status.error
            msg = err.message if err else str(resp.status)
            raise RuntimeError(f"Statement {st}: {msg}")
        time.sleep(POLL_INTERVAL_SEC)
        resp = client.statement_execution.get_statement(resp.statement_id)
    raise TimeoutError("SQL statement did not finish in time")


def write_csv(path: Path, columns: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        w.writerows(rows)


def workspace_client_cli(*, profile: str | None) -> WorkspaceClient:
    """Use OAuth tokens from `databricks auth login`, not static PATs from env."""
    kwargs: dict = {"auth_type": "databricks-cli"}
    if profile:
        kwargs["profile"] = profile
    return WorkspaceClient(**kwargs)


def pick_warehouse_id(client: WorkspaceClient) -> str:
    env = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if env:
        return env.strip()
    for wh in client.warehouses.list():
        if wh.state and wh.state.value == "RUNNING":
            return wh.id
    raise RuntimeError(
        "No RUNNING SQL warehouse found. Set DATABRICKS_SQL_WAREHOUSE_ID or start a warehouse."
    )


def slug_filename(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", name.strip())
    return s.strip("_") or "engineer"


def _engineer_color_map(names: list[str]):
    import matplotlib.pyplot as plt

    ordered = sorted(names)
    n = len(ordered)
    cmap = plt.colormaps.get_cmap("tab20")
    scale = max(n - 1, 1)
    return {eng: cmap(i / scale) for i, eng in enumerate(ordered)}


def _grouped_bars_by_period(
    ax,
    df,
    *,
    period_col: str,
    engineers: list[str],
    colors: dict[str, tuple],
    ylabel: str,
    date_label_fmt: str,
) -> None:
    import numpy as np
    import pandas as pd

    periods = sorted(df[period_col].dropna().unique())
    if not periods or not engineers:
        return
    n_p = len(periods)
    n_e = len(engineers)
    x = np.arange(n_p, dtype=float)
    bar_w = 0.85 / max(n_e, 1)
    for i, eng in enumerate(engineers):
        heights: list[float] = []
        for p in periods:
            row = df[(df["engineer_name"] == eng) & (df[period_col] == p)]
            heights.append(float(row["completed_asqs"].iloc[0]) if len(row) else 0.0)
        ax.bar(
            x + i * bar_w,
            heights,
            bar_w,
            label=eng,
            color=colors[eng],
        )
    ax.set_xticks(x + bar_w * (n_e - 1) / 2 if n_e > 1 else x)
    labels = [pd.Timestamp(p).strftime(date_label_fmt) for p in periods]
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.3)


def _write_team_weekly_overview_chart(
    charts_dir: Path,
    wdf,
    *,
    engineers: list[str],
) -> None:
    """All engineers on one axes: weekly grouped bars (monthly consolidated is a separate PNG)."""
    import matplotlib.pyplot as plt

    colors = _engineer_color_map(engineers)
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.suptitle(
        "APJ STS — completed ASQs by week, all engineers (same 6-calendar-month window as monthly chart)"
    )
    _grouped_bars_by_period(
        ax,
        wdf,
        period_col="week_start",
        engineers=engineers,
        colors=colors,
        ylabel="Weekly completions",
        date_label_fmt="%Y-%m-%d",
    )
    ax.set_xlabel("Week start (UTC)")
    ax.legend(
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        fontsize=8,
        framealpha=0.9,
    )
    plt.tight_layout()
    out = charts_dir / "asq_completions_team_all_members.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def rolling_six_calendar_months_utc():
    """Six calendar months ending in the current month — matches SQL ADD_MONTHS(CURRENT_DATE(), -5) month spine."""
    import pandas as pd

    now = pd.Timestamp.now(tz="UTC")
    end_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0, nanosecond=0)
    start_month = end_month + pd.DateOffset(months=-5)
    return pd.date_range(start=start_month, end=end_month, freq="MS", tz="UTC")


def monthly_completion_pivot(mdf, engineers: list[str], months):
    """
    Pivot monthly.csv aggregation: rows = calendar month (Period), columns = engineer.
    Reindexed to the 6-month spine; sums duplicates; missing = 0.
    """
    import pandas as pd

    eng_sorted = sorted(engineers)
    spine_periods = pd.period_range(
        months[0].to_period("M"),
        months[-1].to_period("M"),
        freq="M",
    )
    if mdf.empty:
        return pd.DataFrame(0.0, index=spine_periods, columns=eng_sorted)

    mdf = mdf.copy()
    mdf["_period"] = pd.to_datetime(mdf["month_start"], utc=True).dt.to_period("M")
    piv = mdf.pivot_table(
        index="_period",
        columns="engineer_name",
        values="completed_asqs",
        aggfunc="sum",
        fill_value=0.0,
    )
    piv = piv.reindex(spine_periods, fill_value=0.0)
    for eng in eng_sorted:
        if eng not in piv.columns:
            piv[eng] = 0.0
    return piv[eng_sorted]


def write_per_engineer_monthly_bars(charts_dir: Path, piv, engineers: list[str]) -> None:
    """One PNG per engineer: monthly counts from the same pivot as the consolidated chart."""
    import matplotlib.pyplot as plt
    import numpy as np

    labels = [f"{p.year}-{p.month:02d}" for p in piv.index]
    x = np.arange(len(piv.index))
    width = 0.65

    for eng in engineers:
        heights = piv[eng].astype(float).tolist()
        fig, ax = plt.subplots(figsize=(11, 5))
        ax.bar(x, heights, width=width, color="#4575b4", edgecolor="white", linewidth=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=0, fontsize=10)
        ax.set_ylabel("Completed ASQs (count)")
        ax.set_xlabel("Calendar month (UTC)")
        ax.set_title(
            f"Completed ASQs by month — {eng}\n"
            f"Rolling 6 calendar months · Status Complete/Completed · "
            f"bucket = MONTH(LastModifiedDate)"
        )
        ax.grid(axis="y", alpha=0.35)
        ax.set_ylim(bottom=0)
        ymax = max(heights) if heights else 0
        if ymax > 0:
            ax.set_ylim(top=max(ymax * 1.12, 1))
        plt.tight_layout(pad=1.4)
        png = charts_dir / f"asq_completions_{slug_filename(eng)}.png"
        fig.savefig(png, dpi=150, bbox_inches="tight")
        plt.close(fig)


def write_consolidated_monthly_all_engineers(charts_dir: Path, piv, engineers: list[str]) -> None:
    """Grouped bars: same numbers as monthly.csv pivot (completed ASQs per engineer per month)."""
    import matplotlib.pyplot as plt
    import numpy as np

    if not engineers:
        return
    colors = _engineer_color_map(engineers)
    n_p = len(piv.index)
    n_e = len(engineers)
    x = np.arange(n_p, dtype=float)
    bar_w = 0.85 / max(n_e, 1)
    fig, ax = plt.subplots(figsize=(14, 7.5))
    for i, eng in enumerate(engineers):
        heights = piv[eng].astype(float).tolist()
        ax.bar(
            x + i * bar_w,
            heights,
            bar_w,
            label=eng,
            color=colors[eng],
            edgecolor="white",
            linewidth=0.5,
        )
    ax.set_xticks(x + bar_w * (n_e - 1) / 2 if n_e > 1 else x)
    labels = [f"{p.year}-{p.month:02d}" for p in piv.index]
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=10)
    ax.set_ylabel("Completed ASQs (count)")
    ax.set_xlabel("Calendar month (UTC)")
    ax.set_title(
        "Completed ASQs by month and engineer — rolling 6 calendar months\n"
        "Status: Complete / Completed · Bucket: MONTH(LastModifiedDate) · "
        "Same filter as monthly.csv / SQL warehouse"
    )
    ax.grid(axis="y", alpha=0.35)
    ax.set_ylim(bottom=0)
    ax.legend(
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        fontsize=8,
        framealpha=0.95,
    )
    plt.tight_layout()
    out = charts_dir / "asq_completions_all_engineers_monthly.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)


def write_charts_weekly_monthly(
    out_dir: Path,
    weekly_rows: list[list[str]],
    monthly_rows: list[list[str]],
) -> None:
    import pandas as pd

    wcols = ["engineer_name", "week_start", "completed_asqs"]
    mcols = ["engineer_name", "month_start", "completed_asqs"]
    wdf = pd.DataFrame(weekly_rows, columns=wcols)
    mdf = pd.DataFrame(monthly_rows, columns=mcols)
    if wdf.empty and mdf.empty:
        return

    wdf["week_start"] = pd.to_datetime(wdf["week_start"], utc=True)
    mdf["month_start"] = pd.to_datetime(mdf["month_start"], utc=True)
    wdf["completed_asqs"] = pd.to_numeric(wdf["completed_asqs"], errors="coerce").fillna(0)
    mdf["completed_asqs"] = pd.to_numeric(mdf["completed_asqs"], errors="coerce").fillna(0)

    engineers = sorted(set(wdf["engineer_name"].dropna().unique()) | set(mdf["engineer_name"].dropna().unique()))
    charts_dir = out_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    months = rolling_six_calendar_months_utc()
    piv = monthly_completion_pivot(mdf, engineers, months)
    write_consolidated_monthly_all_engineers(charts_dir, piv, engineers)
    write_per_engineer_monthly_bars(charts_dir, piv, engineers)

    _write_team_weekly_overview_chart(charts_dir, wdf, engineers=sorted(engineers))


def main() -> int:
    parser = argparse.ArgumentParser(description="APJ STS ASQ completion metrics (rolling 6 months)")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd() / "asq_completions_output",
        help="Directory for CSV (and charts with --charts)",
    )
    parser.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID"),
        help="SQL warehouse ID (default: env DATABRICKS_SQL_WAREHOUSE_ID or first RUNNING warehouse)",
    )
    parser.add_argument("--catalog", default=DEFAULT_CATALOG, help="Unity Catalog name (default: main)")
    parser.add_argument(
        "--profile",
        default=os.environ.get("DATABRICKS_CONFIG_PROFILE"),
        help="Databricks config profile (default: env DATABRICKS_CONFIG_PROFILE or SDK default, usually DEFAULT)",
    )
    parser.add_argument(
        "--charts",
        action="store_true",
        help="Write PNGs: asq_completions_all_engineers_monthly.png, per-engineer monthly, team weekly (matplotlib, pandas)",
    )
    args = parser.parse_args()

    cte = base_cte()
    sql_totals = build_sql_totals(cte)
    sql_weekly = build_sql_weekly(cte)
    sql_monthly = build_sql_monthly(cte)
    sql_detail = build_sql_detail(cte)

    client = workspace_client_cli(profile=args.profile)
    wid = args.warehouse_id or pick_warehouse_id(client)

    out = args.output_dir.resolve()
    prof = args.profile or "(default profile)"
    print(f"Auth:      databricks-cli (profile {prof})\nWarehouse: {wid}\nOutput:    {out}\n")

    tcols, trows = execute_sql(client, wid, sql_totals, catalog=args.catalog)
    wcols, wrows = execute_sql(client, wid, sql_weekly, catalog=args.catalog)
    mcols, mrows = execute_sql(client, wid, sql_monthly, catalog=args.catalog)

    write_csv(out / "totals.csv", tcols, trows)
    write_csv(out / "weekly.csv", wcols, wrows)
    write_csv(out / "monthly.csv", mcols, mrows)
    print(f"Wrote {out / 'totals.csv'} ({len(trows)} rows)")
    print(f"Wrote {out / 'weekly.csv'} ({len(wrows)} rows)")
    print(f"Wrote {out / 'monthly.csv'} ({len(mrows)} rows)")

    dcols: list[str] = []
    drows: list[list[str]] = []
    if args.charts:
        dcols, drows = execute_sql(client, wid, sql_detail, catalog=args.catalog)
        write_csv(out / "asq_detail.csv", dcols, drows)
        print(f"Wrote {out / 'asq_detail.csv'} ({len(drows)} rows)")
        try:
            write_charts_weekly_monthly(out, wrows, mrows)
            chart_dir = out / "charts"
            paths = sorted(chart_dir.glob("*.png"))
            print(f"Wrote {len(paths)} chart(s) under {chart_dir}")
            consolidated = chart_dir / "asq_completions_all_engineers_monthly.png"
            if consolidated.exists():
                print(f"All engineers (monthly): {consolidated}")
            team = chart_dir / "asq_completions_team_all_members.png"
            if team.exists():
                print(f"All engineers (weekly): {team}")
        except ImportError as e:
            print("Charts skipped (install pandas and matplotlib):", e, file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130) from None
