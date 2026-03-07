"""
Live Fraud Queue — Databricks App
Modern two-tab interface:
  Tab 1 – Fraud Queue   : KPI bar, filterable AG Grid, case-detail slide-over panel
  Tab 2 – Genie Chat    : Conversational analytics via Banking Fraud Investigation Hub
"""

import json
import os
from datetime import datetime, timezone

import dash
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from dash import html, dcc, Input, Output, State, callback, no_update, clientside_callback
from dash.exceptions import PreventUpdate

import backend as bk
import genie as gn

# ─────────────────────────────────────────────────────────────────────────────
# App init
# ─────────────────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        dbc.icons.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
    ],
    title="Live Fraud Queue",
    update_title=None,
    suppress_callback_exceptions=True,
)
server = app.server

# ─────────────────────────────────────────────────────────────────────────────
# Design tokens
# ─────────────────────────────────────────────────────────────────────────────
PALETTE = {
    "bg":          "#0f1117",
    "surface":     "#1a1d27",
    "surface2":    "#22263a",
    "border":      "#2d3148",
    "text":        "#e2e8f0",
    "muted":       "#8892a4",
    "critical":    "#ef4444",
    "high":        "#f97316",
    "medium":      "#3b82f6",
    "low":         "#22c55e",
    "accent":      "#6366f1",
    "accent_dark": "#4f46e5",
    "success":     "#22c55e",
    "warning":     "#f59e0b",
}

# Styles live in assets/custom.css — Dash serves it automatically

RISK_HEX = {"CRITICAL": PALETTE["critical"], "HIGH": PALETTE["high"],
             "MEDIUM": PALETTE["medium"], "LOW": PALETTE["low"]}

ANALYSTS = [
    "fraud-analyst-1@bank.in",
    "fraud-analyst-2@bank.in",
    "fraud-analyst-3@bank.in",
    "compliance@bank.in",
    "senior-investigator@bank.in",
]

# ─────────────────────────────────────────────────────────────────────────────
# AG Grid column definitions
# ─────────────────────────────────────────────────────────────────────────────
COLDEFS = [
    {
        "field": "risk_tier", "headerName": "Risk", "width": 100, "pinned": "left",
        "cellStyle": {"function": "({'CRITICAL':{'color':'#ef4444'},'HIGH':{'color':'#f97316'},'MEDIUM':{'color':'#3b82f6'},'LOW':{'color':'#22c55e'}})[params.value] || {}"},
        "cellRenderer": "agTextCellRenderer",
    },
    {"field": "risk_score", "headerName": "Score", "width": 80, "type": "numericColumn",
     "valueFormatter": {"function": "params.value?.toFixed(1)"}},
    {"field": "transaction_id", "headerName": "Transaction", "width": 148, "pinned": "left",
     "cellStyle": {"function": "({fontFamily:'SF Mono,Fira Code,monospace',fontSize:'12px'})"}},
    {"field": "user_id", "headerName": "User", "width": 128},
    {"field": "detected_fraud_type", "headerName": "Type", "width": 158},
    {
        "field": "transaction_amount", "headerName": "Amount ₹", "width": 130,
        "type": "numericColumn",
        "valueFormatter": {"function": "params.value ? '₹'+params.value.toLocaleString('en-IN',{minimumFractionDigits:0}) : '-'"},
        "cellStyle": {"function": "params.value > 100000 ? {fontWeight:'600'} : {}"},
    },
    {"field": "transaction_type", "headerName": "Method", "width": 100},
    {"field": "transaction_city", "headerName": "City", "width": 110},
    {
        "field": "triage_status", "headerName": "Status", "width": 120,
        "cellStyle": {"function": "({'PENDING':{'color':'#94a3b8'},'IN_REVIEW':{'color':'#6366f1'},'ESCALATED':{'color':'#ef4444'},'RESOLVED':{'color':'#22c55e'},'FALSE_ALARM':{'color':'#22c55e'}})[params.value] || {}"},
    },
    {"field": "automated_action", "headerName": "Auto Action", "width": 140},
    {
        "field": "review_sla_deadline", "headerName": "SLA", "width": 160,
        "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleString('en-IN',{dateStyle:'short',timeStyle:'short'}) : '-'"},
        "cellStyle": {"function": "params.value && new Date(params.value) < new Date() && !['RESOLVED','CLOSED','FALSE_ALARM'].includes(params.data?.triage_status) ? {color:'#ef4444',fontWeight:'600'} : {}"},
    },
    {"field": "assigned_to", "headerName": "Assigned To", "width": 185},
    {
        "field": "mfa_change_flag", "headerName": "MFA", "width": 70,
        "valueFormatter": {"function": "params.value ? '⚠' : '–'"},
        "cellStyle": {"function": "params.value ? {color:'#f97316',textAlign:'center'} : {textAlign:'center',color:'#475569'}"},
    },
    {
        "field": "is_vpn", "headerName": "VPN", "width": 65,
        "valueFormatter": {"function": "params.value ? '⚠' : '–'"},
        "cellStyle": {"function": "params.value ? {color:'#f97316',textAlign:'center'} : {textAlign:'center',color:'#475569'}"},
    },
    {
        "field": "is_tor", "headerName": "TOR", "width": 65,
        "valueFormatter": {"function": "params.value ? '🔴' : '–'"},
        "cellStyle": {"function": "params.value ? {color:'#ef4444',textAlign:'center',fontWeight:'700'} : {textAlign:'center',color:'#475569'}"},
    },
    {"field": "login_country_code", "headerName": "Country", "width": 85,
     "cellStyle": {"function": "params.value && params.value !== 'IN' ? {color:'#f97316',fontWeight:'600'} : {}"}},
    {
        "field": "alert_generated_at", "headerName": "Alerted", "width": 165,
        "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleString('en-IN',{dateStyle:'short',timeStyle:'short'}) : '-'"},
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# KPI card component
# ─────────────────────────────────────────────────────────────────────────────
def kpi_card(label: str, value, color: str = PALETTE["text"],
             icon: str = "bi-bar-chart-fill") -> html.Div:
    return html.Div([
        html.Div([
            html.I(className=f"bi {icon}", style={"color": color, "fontSize": "1.1rem"}),
            html.Span(label, className="kpi-label ms-2"),
        ], className="d-flex align-items-center"),
        html.Div(str(value), className="kpi-value", style={"color": color}),
    ], className="kpi-card")


def build_kpi_row(kpis: dict) -> dbc.Row:
    return dbc.Row([
        dbc.Col(kpi_card("Open Cases",      kpis.get("open_cases", 0),
                         PALETTE["text"],      "bi-inbox-fill"), xs=6, lg=2),
        dbc.Col(kpi_card("Critical",         kpis.get("critical_cases", 0),
                         PALETTE["critical"],  "bi-exclamation-octagon-fill"), xs=6, lg=2),
        dbc.Col(kpi_card("SLA Breaches",     kpis.get("sla_breaches", 0),
                         PALETTE["warning"],   "bi-clock-fill"), xs=6, lg=2),
        dbc.Col(kpi_card("Confirmed Fraud",  kpis.get("confirmed_fraud", 0),
                         PALETTE["high"],      "bi-shield-exclamation"), xs=6, lg=2),
        dbc.Col(kpi_card("False Positives",  kpis.get("false_alarms", 0),
                         PALETTE["low"],       "bi-check-circle-fill"), xs=6, lg=2),
        dbc.Col(kpi_card("FPR",              f"{kpis.get('fpr_pct', 0):.1f}%",
                         PALETTE["accent"],    "bi-percent"), xs=6, lg=2),
    ], className="g-2 mb-3")


# ─────────────────────────────────────────────────────────────────────────────
# Detail slide-over panel
# ─────────────────────────────────────────────────────────────────────────────
def _drow(label: str, value) -> html.Div:
    return html.Div([
        html.Span(label, className="lbl"),
        html.Span(value if isinstance(value, str) else "", className="val") if isinstance(value, str) else value,
    ], className="detail-row")


def build_risk_factors(rf_raw) -> html.Div:
    try:
        factors = json.loads(rf_raw) if isinstance(rf_raw, str) else (rf_raw or [])
    except Exception:
        return html.Div()
    cards = []
    for f in factors:
        w = int(float(f.get("weight", 0)) * 100)
        cards.append(
            html.Div([
                html.Div([
                    html.Span(f.get("name", "").replace("_", " ").title(),
                              style={"fontSize": "0.72rem", "color": PALETTE["muted"], "fontWeight": "600", "textTransform": "uppercase", "letterSpacing": "0.05em"}),
                    html.Span(str(f.get("value", "")),
                              style={"float": "right", "fontWeight": "700", "fontSize": "0.85rem"}),
                ]),
                dbc.Progress(value=w, color="danger" if w > 35 else "warning",
                             style={"height": "3px", "marginTop": "4px", "marginBottom": "4px"},
                             className="bg-transparent"),
                html.Div(f.get("description", ""),
                         style={"fontSize": "0.75rem", "color": PALETTE["muted"]}),
            ], style={"background": PALETTE["surface2"], "border": f"1px solid {PALETTE['border']}",
                      "borderRadius": "8px", "padding": "10px 12px", "marginBottom": "8px"})
        )
    return html.Div(cards)


def build_detail_panel() -> html.Div:
    return html.Div([
        # Backdrop
        html.Div(id="detail-backdrop", className="detail-backdrop", n_clicks=0),
        # Panel
        html.Div([
            # Header
            html.Div([
                html.Div([
                    html.Span(id="panel-txn-id",
                              style={"fontFamily": "SF Mono,Fira Code,monospace", "fontSize": "0.9rem", "fontWeight": "600"}),
                    html.Span(id="panel-risk-badge", className="ms-2"),
                ], className="d-flex align-items-center"),
                html.Button("✕", id="panel-close", n_clicks=0,
                            style={"background": "none", "border": "none", "color": PALETTE["muted"],
                                   "fontSize": "1.2rem", "cursor": "pointer", "marginLeft": "auto"}),
            ], className="d-flex align-items-center mb-3 pb-2",
               style={"borderBottom": f"1px solid {PALETTE['border']}"}),

            # Tabs inside panel
            dbc.Tabs([
                dbc.Tab(html.Div(id="panel-overview"), label="Overview", tab_id="pt-overview"),
                dbc.Tab(html.Div(id="panel-factors"),  label="Risk Factors", tab_id="pt-factors"),
                dbc.Tab(html.Div(id="panel-audit"),    label="Audit", tab_id="pt-audit"),
                dbc.Tab(html.Div(id="panel-rules"),    label="Rules", tab_id="pt-rules"),
                dbc.Tab(html.Div(id="panel-travel"),   label="Travel", tab_id="pt-travel"),
            ], id="panel-tabs", active_tab="pt-overview",
               className="mb-3", style={"fontSize": "0.8rem"}),

            # Action area
            html.Div([
                html.Div("Analyst Action", style={"fontSize": "0.7rem", "fontWeight": "700",
                         "textTransform": "uppercase", "letterSpacing": "0.08em",
                         "color": PALETTE["muted"], "marginBottom": "10px"}),
                dbc.Input(id="analyst-name", placeholder="your@email.com",
                          value=ANALYSTS[0], size="sm", className="mb-2",
                          style={"background": PALETTE["surface2"], "border": f"1px solid {PALETTE['border']}",
                                 "color": PALETTE["text"], "fontSize": "0.82rem"}),
                dbc.Textarea(id="action-notes", placeholder="Review notes…", rows=2,
                             className="mb-2",
                             style={"background": PALETTE["surface2"], "border": f"1px solid {PALETTE['border']}",
                                    "color": PALETTE["text"], "fontSize": "0.82rem", "resize": "none"}),
                dbc.Row([
                    dbc.Col(dbc.Button([html.I(className="bi bi-check2-circle me-1"), "Release"],
                                       id="btn-release", size="sm", className="w-100 action-btn",
                                       style={"background": PALETTE["low"], "border": "none", "color": "#000"}), xs=6),
                    dbc.Col(dbc.Button([html.I(className="bi bi-x-octagon me-1"), "Confirm Fraud"],
                                       id="btn-confirm", size="sm", className="w-100 action-btn",
                                       style={"background": PALETTE["critical"], "border": "none"}), xs=6),
                ], className="g-2 mb-2"),
                dbc.Row([
                    dbc.Col(dbc.Button([html.I(className="bi bi-arrow-up-circle me-1"), "Escalate"],
                                       id="btn-escalate", size="sm", className="w-100 action-btn",
                                       style={"background": PALETTE["warning"], "border": "none", "color": "#000"}), xs=6),
                    dbc.Col([
                        dbc.Select(id="assign-select",
                                   options=[{"label": a.split("@")[0], "value": a} for a in ANALYSTS],
                                   value=ANALYSTS[0], size="sm",
                                   style={"background": PALETTE["surface2"], "border": f"1px solid {PALETTE['border']}",
                                          "color": PALETTE["text"], "fontSize": "0.8rem"}),
                        dbc.Button([html.I(className="bi bi-person-check me-1"), "Assign"],
                                   id="btn-assign", size="sm", className="w-100 mt-1 action-btn",
                                   style={"background": PALETTE["accent"], "border": "none"}),
                    ], xs=6),
                ], className="g-2"),
                html.Div(id="action-feedback", className="mt-2"),
            ], style={"background": PALETTE["surface2"], "border": f"1px solid {PALETTE['border']}",
                      "borderRadius": "10px", "padding": "14px", "marginTop": "1rem"}),
        ], id="detail-panel", className="detail-panel"),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# Genie Chat tab
# ─────────────────────────────────────────────────────────────────────────────
def build_genie_tab() -> html.Div:
    chips = html.Div(
        [html.Span(q, className="chip me-1 mb-1", id={"type": "genie-chip", "index": i})
         for i, q in enumerate(gn.SUGGESTED_QUESTIONS)],
        className="mb-3",
    )

    return html.Div([
        dcc.Store(id="genie-conv-id"),          # current conversation_id
        dcc.Store(id="genie-history", data=[]), # list of {role, content, sql, data, columns}

        html.Div([
            # ── Sidebar: header + chips ───────────────────────────────────
            html.Div([
                html.Div([
                    html.I(className="bi bi-stars me-2",
                           style={"color": PALETTE["accent"], "fontSize": "1.1rem"}),
                    html.Span("Banking Fraud Investigation Hub",
                              style={"fontWeight": "600", "fontSize": "0.95rem"}),
                ], className="d-flex align-items-center mb-1"),
                html.Div("AI-powered analytics · Natural language → SQL",
                         style={"fontSize": "0.72rem", "color": PALETTE["muted"], "marginBottom": "1rem"}),
                html.Div("Suggested questions",
                         style={"fontSize": "0.7rem", "fontWeight": "700", "textTransform": "uppercase",
                                "letterSpacing": "0.08em", "color": PALETTE["muted"], "marginBottom": "0.5rem"}),
                chips,
                html.Button([html.I(className="bi bi-trash me-1"), "New conversation"],
                            id="btn-new-chat", n_clicks=0,
                            style={"background": "none", "border": f"1px solid {PALETTE['border']}",
                                   "color": PALETTE["muted"], "borderRadius": "7px",
                                   "padding": "5px 12px", "fontSize": "0.78rem",
                                   "cursor": "pointer", "marginTop": "0.5rem",
                                   "transition": "all 0.15s"}),
            ], style={"width": "280px", "flexShrink": "0",
                      "background": PALETTE["surface"], "border": f"1px solid {PALETTE['border']}",
                      "borderRadius": "12px", "padding": "1.25rem", "alignSelf": "flex-start",
                      "position": "sticky", "top": "0"}),

            # ── Chat area ─────────────────────────────────────────────────
            html.Div([
                # Messages
                html.Div(id="chat-messages",
                         className="chat-messages",
                         style={"background": PALETTE["bg"],
                                "borderRadius": "12px 12px 0 0",
                                "border": f"1px solid {PALETTE['border']}",
                                "borderBottom": "none",
                                "flex": "1", "overflow-y": "auto",
                                "minHeight": "400px", "maxHeight": "calc(100vh - 280px)"}),
                # Input row
                html.Div([
                    dbc.Row([
                        dbc.Col(
                            dbc.Textarea(
                                id="genie-input",
                                placeholder="Ask anything about fraud patterns, KPIs, alerts, or transactions…",
                                rows=2,
                                style={"background": PALETTE["surface2"],
                                       "border": f"1px solid {PALETTE['border']}",
                                       "color": PALETTE["text"], "borderRadius": "8px",
                                       "fontSize": "0.88rem", "resize": "none"},
                            ), xs=10,
                        ),
                        dbc.Col(
                            dbc.Button([html.I(className="bi bi-send-fill")],
                                       id="btn-genie-send", color="primary", size="sm",
                                       className="w-100 h-100",
                                       style={"background": PALETTE["accent"], "border": "none",
                                              "borderRadius": "8px", "fontSize": "1.1rem"}),
                            xs=2,
                        ),
                    ], className="g-2"),
                    html.Div(id="genie-status", className="mt-1",
                             style={"fontSize": "0.75rem", "color": PALETTE["muted"]}),
                ], style={"background": PALETTE["surface"],
                          "border": f"1px solid {PALETTE['border']}",
                          "borderTop": "none",
                          "borderRadius": "0 0 12px 12px",
                          "padding": "12px 16px"}),
            ], style={"flex": "1", "display": "flex", "flexDirection": "column", "minWidth": "0"}),

        ], style={"display": "flex", "gap": "1.25rem", "alignItems": "flex-start"}),
    ], style={"padding": "1.5rem"})


def _render_message(role: str, content: str, sql: str = "",
                    columns: list = None, data: list = None) -> html.Div:
    """Render a single chat bubble with optional SQL + result table."""
    if role == "user":
        return html.Div(html.Div(content, className="bubble"), className="msg-user")

    # Genie message
    inner = [html.Div(content, className="bubble") if content else None]
    inner = [x for x in inner if x]

    if sql:
        inner.append(
            html.Details([
                html.Summary("View generated SQL",
                             style={"fontSize": "0.75rem", "color": PALETTE["accent"],
                                    "cursor": "pointer", "marginTop": "6px"}),
                html.Div(sql, className="sql-block"),
            ])
        )

    if data and columns:
        # Show first 20 rows
        inner.append(
            html.Div([
                html.Div(f"{len(data):,} row{'s' if len(data) != 1 else ''} returned",
                         style={"fontSize": "0.75rem", "color": PALETTE["muted"], "marginTop": "8px", "marginBottom": "4px"}),
                html.Div(
                    html.Table([
                        html.Thead(html.Tr([html.Th(c) for c in columns])),
                        html.Tbody([
                            html.Tr([html.Td(str(v)) for v in row])
                            for row in data[:20]
                        ]),
                    ], className="result-table"),
                    style={"overflowX": "auto", "background": PALETTE["surface2"],
                           "borderRadius": "8px", "padding": "2px 4px",
                           "border": f"1px solid {PALETTE['border']}"},
                ),
            ])
        )

    return html.Div(html.Div(inner, className="bubble"), className="msg-genie")


# ─────────────────────────────────────────────────────────────────────────────
# Layout
# ─────────────────────────────────────────────────────────────────────────────
app.layout = html.Div([

    # Stores & timers
    dcc.Store(id="selected-txn-id"),
    dcc.Store(id="panel-open", data=False),
    dcc.Interval(id="refresh-timer", interval=30_000, n_intervals=0),

    # Toast notifications
    dbc.Toast(id="action-toast", is_open=False, dismissable=True, duration=4000,
              style={"position": "fixed", "top": "1rem", "right": "1rem", "zIndex": 9999,
                     "background": PALETTE["surface"], "border": f"1px solid {PALETTE['border']}",
                     "color": PALETTE["text"], "minWidth": "300px"}),

    # ── Top bar ──────────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            html.Span("⬡", style={"color": PALETTE["critical"], "fontSize": "1.25rem", "marginRight": "8px"}),
            html.Span("Live Fraud Queue", style={"fontWeight": "700", "fontSize": "1rem"}),
            dbc.Badge("LIVE", color="danger", className="ms-2",
                      style={"fontSize": "0.6rem", "verticalAlign": "middle"}),
        ], className="d-flex align-items-center"),
        html.Div([
            html.Span(id="last-refresh-time",
                      style={"fontSize": "0.75rem", "color": PALETTE["muted"], "marginRight": "12px"}),
            html.Button([html.I(className="bi bi-arrow-clockwise me-1"), "Refresh"],
                        id="btn-refresh", n_clicks=0,
                        style={"background": "none", "border": f"1px solid {PALETTE['border']}",
                               "color": PALETTE["muted"], "borderRadius": "7px",
                               "padding": "4px 12px", "fontSize": "0.78rem", "cursor": "pointer"}),
        ], className="d-flex align-items-center ms-auto"),
    ], className="app-topbar"),

    # ── Main tab nav ─────────────────────────────────────────────────────────
    dbc.Tabs([
        dbc.Tab(label="🛡️  Fraud Queue",  tab_id="tab-queue"),
        dbc.Tab(label="✨  Genie Chat",   tab_id="tab-genie"),
    ], id="main-tabs", active_tab="tab-queue",
       className="main-tabs"),

    # ── Tab content ──────────────────────────────────────────────────────────
    html.Div(id="tab-content", style={"position": "relative"}),

    # Detail slide-over panel (always in DOM)
    build_detail_panel(),
], style={"background": PALETTE["bg"], "minHeight": "100vh", "color": PALETTE["text"]})


# ─────────────────────────────────────────────────────────────────────────────
# Callbacks
# ─────────────────────────────────────────────────────────────────────────────

def _build_queue_shell() -> html.Div:
    """Static queue tab structure — filters always in DOM so callbacks can reference them."""
    return html.Div([
        # KPI row — populated by refresh_queue callback
        html.Div(id="kpi-row-container"),
        # Filter bar — STATIC, always in DOM
        html.Div([
            dbc.Row([
                dbc.Col([
                    html.Label("Status", style={"fontSize": "0.72rem", "fontWeight": "600",
                               "textTransform": "uppercase", "letterSpacing": "0.08em",
                               "color": PALETTE["muted"], "marginBottom": "4px"}),
                    dcc.Dropdown(
                        id="filter-status",
                        options=[
                            {"label": "Pending",    "value": "PENDING"},
                            {"label": "In Review",  "value": "IN_REVIEW"},
                            {"label": "Escalated",  "value": "ESCALATED"},
                            {"label": "Resolved",   "value": "RESOLVED"},
                            {"label": "False Alarm","value": "FALSE_ALARM"},
                        ],
                        value=["PENDING", "IN_REVIEW", "ESCALATED"],
                        multi=True, clearable=True,
                        style={"fontSize": "0.82rem"},
                        className="dropdown-custom",
                    ),
                ], md=6),
                dbc.Col([
                    html.Label("Risk Tier", style={"fontSize": "0.72rem", "fontWeight": "600",
                               "textTransform": "uppercase", "letterSpacing": "0.08em",
                               "color": PALETTE["muted"], "marginBottom": "4px"}),
                    dcc.Dropdown(
                        id="filter-risk",
                        options=[
                            {"label": "Critical", "value": "CRITICAL"},
                            {"label": "High",     "value": "HIGH"},
                            {"label": "Medium",   "value": "MEDIUM"},
                            {"label": "Low",      "value": "LOW"},
                        ],
                        value=[], multi=True, clearable=True,
                        style={"fontSize": "0.82rem"},
                        className="dropdown-custom",
                    ),
                ], md=6),
            ], className="g-2"),
        ], className="filter-bar mb-3"),
        # Grid placeholder — populated by refresh_queue callback
        html.Div(id="grid-container"),
    ], style={"padding": "1.5rem"})


@callback(
    Output("tab-content", "children"),
    Input("main-tabs", "active_tab"),
    prevent_initial_call=False,
)
def switch_tab(tab):
    if tab == "tab-genie":
        return build_genie_tab()
    return _build_queue_shell()


@callback(
    Output("kpi-row-container", "children"),
    Output("grid-container", "children"),
    Output("last-refresh-time", "children"),
    Input("main-tabs", "active_tab"),   # fires on initial load + tab switch
    Input("refresh-timer", "n_intervals"),
    Input("btn-refresh", "n_clicks"),
    Input("filter-status", "value"),
    Input("filter-risk", "value"),
    prevent_initial_call=False,
)
def refresh_queue(active_tab, n_intervals, n_clicks, status_filter, risk_filter):
    if active_tab == "tab-genie":
        raise PreventUpdate

    try:
        kpis = bk.fetch_queue_kpis()
        rows = bk.fetch_queue(
            status_filter=status_filter or None,
            risk_filter=risk_filter or None,
        )
    except Exception as exc:
        err = dbc.Alert(f"Backend error: {exc}", color="danger")
        ts  = datetime.now().strftime("%H:%M:%S")
        return err, err, f"Error at {ts}"

    # Serialise datetimes
    for r in rows:
        for k, v in r.items():
            if isinstance(v, datetime):
                r[k] = v.isoformat()

    ts = f"Updated {datetime.now().strftime('%H:%M:%S')} · {len(rows)} cases"

    grid = dag.AgGrid(
        id="queue-grid",
        rowData=rows,
        columnDefs=COLDEFS,
        defaultColDef={"resizable": True, "sortable": True, "filter": True,
                       "suppressMovable": False},
        dashGridOptions={
            "rowSelection": "single",
            "animateRows": True,
            "rowHeight": 40,
            "headerHeight": 38,
            "pagination": True,
            "paginationPageSize": 25,
            "theme": "ag-theme-alpine-dark",
            "suppressCellFocus": True,
        },
        style={"height": "calc(100vh - 370px)", "width": "100%"},
        className="ag-theme-alpine-dark",
    )
    return build_kpi_row(kpis), grid, ts


# ── Detail panel open/close ──────────────────────────────────────────────────
@callback(
    Output("detail-panel", "className"),
    Output("detail-backdrop", "className"),
    Output("selected-txn-id", "data"),
    Output("panel-txn-id", "children"),
    Output("panel-risk-badge", "children"),
    Output("panel-overview", "children"),
    Output("panel-factors", "children"),
    Output("panel-audit", "children"),
    Output("panel-rules", "children"),
    Output("panel-travel", "children"),
    Input("queue-grid", "selectedRows"),
    Input("panel-close", "n_clicks"),
    Input("detail-backdrop", "n_clicks"),
    State("selected-txn-id", "data"),
    prevent_initial_call=True,
)
def toggle_panel(selected, close_clicks, backdrop_clicks, current_txn):
    ctx     = dash.callback_context
    trigger = ctx.triggered_id if ctx.triggered_id else ""

    CLOSED = ("detail-panel", "detail-backdrop", no_update,
              no_update, no_update, no_update, no_update, no_update, no_update, no_update)

    if trigger in ("panel-close", "detail-backdrop") or not selected:
        return CLOSED

    row    = selected[0]
    txn_id = row.get("transaction_id", "")
    tier   = row.get("risk_tier", "")
    color  = RISK_HEX.get(tier, PALETTE["text"])

    try:
        detail = bk.fetch_case_detail(txn_id)
        travel = bk.fetch_impossible_travel_context(row.get("user_id", ""))
    except Exception as exc:
        err = dbc.Alert(str(exc), color="danger")
        return ("detail-panel open", "detail-backdrop open", txn_id,
                txn_id, "", err, err, err, err, err)

    t = detail["triage"]

    # Overview
    overview = html.Div([
        _drow("Transaction", t.get("transaction_id", "")),
        _drow("User", t.get("user_id", "")),
        _drow("Amount",  f"₹{t.get('transaction_amount', 0):,.2f}" if t.get("transaction_amount") else "—"),
        _drow("Type / Channel", f"{t.get('transaction_type','')} · {t.get('transaction_channel','')}"),
        _drow("Location", f"{t.get('transaction_city','')} / {t.get('transaction_state','')}"),
        _drow("Mins after login", str(t.get("minutes_after_login", "—"))),
        html.Hr(style={"borderColor": PALETTE["border"], "margin": "0.75rem 0"}),
        _drow("Status", dbc.Badge(t.get("triage_status",""), color="secondary", className="ms-1")),
        _drow("Fraud type", t.get("detected_fraud_type", "")),
        _drow("Auto action", t.get("automated_action", "")),
        _drow("Assigned to", t.get("assigned_to") or "—"),
        _drow("SLA deadline", str(t.get("review_sla_deadline", "—"))[:19]),
        html.Hr(style={"borderColor": PALETTE["border"], "margin": "0.75rem 0"}),
        _drow("MFA change", "⚠ Yes" if t.get("mfa_change_flag") else "No"),
        _drow("VPN",        "⚠ Yes" if t.get("is_vpn") else "No"),
        _drow("TOR",        "🔴 Yes" if t.get("is_tor") else "No"),
        _drow("Country",    t.get("login_country_code", "—")),
    ], style={"paddingTop": "0.5rem"})

    # Audit
    audit_items = []
    for a in detail["audit"]:
        ts = a.get("logged_at")
        ts_str = ts.strftime("%d %b %H:%M") if isinstance(ts, datetime) else str(ts)[:16]
        audit_items.append(html.Div([
            html.Span(ts_str, style={"fontSize": "0.72rem", "color": PALETTE["muted"], "minWidth": "80px"}),
            html.Span(a.get("event_type",""), style={"fontSize": "0.72rem", "fontWeight": "600",
                      "background": PALETTE["surface2"], "padding": "1px 6px",
                      "borderRadius": "4px", "color": PALETTE["accent"]}),
            html.Span(f" {a.get('actor','')}", style={"fontSize": "0.78rem", "color": PALETTE["muted"]}),
        ], className="d-flex align-items-center gap-2 mb-2"))
    audit = html.Div(audit_items or [html.Div("No audit history.", style={"color": PALETTE["muted"]})],
                     style={"paddingTop": "0.5rem"})

    # Rules
    rule_rows = []
    for r in detail["rules"]:
        rule_rows.append(html.Tr([
            html.Td(r.get("rule_id",""),    style={"fontFamily": "monospace", "fontSize": "0.72rem"}),
            html.Td(r.get("rule_name",""),  style={"fontSize": "0.78rem"}),
            html.Td(r.get("triggered_value",""), style={"fontSize": "0.78rem"}),
            html.Td(f"{r.get('contribution_score',0):.0f}",
                    style={"fontWeight": "700", "textAlign": "right", "color": PALETTE["high"]}),
        ]))
    rules = html.Div(
        html.Table([
            html.Thead(html.Tr([html.Th("Rule"), html.Th("Name"),
                                html.Th("Value"), html.Th("Score")],
                               style={"fontSize": "0.7rem", "color": PALETTE["muted"]})),
            html.Tbody(rule_rows),
        ], className="result-table w-100") if rule_rows
        else html.Div("No rule triggers.", style={"color": PALETTE["muted"]}),
        style={"paddingTop": "0.5rem", "overflowX": "auto"}
    )

    # Travel
    if travel:
        tr_rows = [html.Tr([
            html.Td(f"{tr.get('prev_city','')} → {tr.get('curr_city','')}", style={"fontSize": "0.78rem"}),
            html.Td(f"{tr.get('distance_miles',0):.0f} mi", style={"fontSize": "0.78rem", "color": PALETTE["high"]}),
            html.Td(f"{tr.get('time_gap_minutes',0):.1f} min", style={"fontSize": "0.78rem"}),
            html.Td(f"{tr.get('implied_speed_mph',0):.0f} mph", style={"fontSize": "0.78rem"}),
        ]) for tr in travel]
        travel_tab = html.Table([
            html.Thead(html.Tr([html.Th("Route"), html.Th("Distance"),
                                html.Th("Time"), html.Th("Speed")],
                               style={"fontSize": "0.7rem", "color": PALETTE["muted"]})),
            html.Tbody(tr_rows),
        ], className="result-table w-100")
    else:
        travel_tab = html.Div("No impossible travel alerts for this user.",
                              style={"color": PALETTE["muted"]})

    badge = html.Span(tier, className=f"badge-risk-{tier}")
    return ("detail-panel open", "detail-backdrop open", txn_id,
            txn_id, badge, overview,
            build_risk_factors(t.get("risk_factors")),
            audit, rules, travel_tab)


# ── Case action buttons ──────────────────────────────────────────────────────
@callback(
    Output("action-toast", "is_open"),
    Output("action-toast", "children"),
    Output("action-feedback", "children"),
    Input("btn-release",  "n_clicks"),
    Input("btn-confirm",  "n_clicks"),
    Input("btn-escalate", "n_clicks"),
    Input("btn-assign",   "n_clicks"),
    State("selected-txn-id",  "data"),
    State("action-notes",     "value"),
    State("analyst-name",     "value"),
    State("assign-select",    "value"),
    prevent_initial_call=True,
)
def handle_action(nr, nc, ne, na, txn_id, notes, analyst, assignee):
    ctx     = dash.callback_context
    trigger = ctx.triggered_id
    if not trigger or not txn_id:
        raise PreventUpdate

    analyst = analyst or "analyst@bank.in"
    try:
        if trigger == "btn-release":
            bk.release_case(txn_id, notes or "", analyst)
            msg, colour = f"Released {txn_id}", PALETTE["low"]
        elif trigger == "btn-confirm":
            bk.confirm_fraud(txn_id, notes or "", analyst)
            msg, colour = f"Confirmed fraud: {txn_id}", PALETTE["critical"]
        elif trigger == "btn-escalate":
            bk.escalate_case(txn_id, notes or "", analyst)
            msg, colour = f"Escalated {txn_id}", PALETTE["warning"]
        elif trigger == "btn-assign":
            bk.assign_case(txn_id, assignee, analyst)
            msg, colour = f"Assigned {txn_id} → {assignee}", PALETTE["accent"]
        else:
            raise PreventUpdate
        fb = html.Div(msg, style={"fontSize": "0.8rem", "color": colour,
                                  "padding": "6px 8px", "borderRadius": "6px",
                                  "background": colour + "15", "marginTop": "6px"})
        return True, msg, fb
    except Exception as exc:
        err = html.Div(str(exc), style={"fontSize": "0.8rem", "color": PALETTE["critical"],
                                        "padding": "6px 8px", "borderRadius": "6px",
                                        "background": PALETTE["critical"] + "15"})
        return True, str(exc), err


# ── Genie chat ───────────────────────────────────────────────────────────────
@callback(
    Output("genie-input", "value"),
    Input({"type": "genie-chip", "index": dash.ALL}, "n_clicks"),
    State({"type": "genie-chip", "index": dash.ALL}, "children"),
    prevent_initial_call=True,
)
def chip_to_input(n_clicks_list, labels):
    ctx = dash.callback_context
    if not ctx.triggered or not any(n_clicks_list):
        raise PreventUpdate
    # Find which chip was clicked
    idx = next((i for i, n in enumerate(n_clicks_list) if n), None)
    if idx is None:
        raise PreventUpdate
    return labels[idx]


@callback(
    Output("chat-messages",   "children"),
    Output("genie-conv-id",   "data"),
    Output("genie-history",   "data"),
    Output("genie-input",     "value", allow_duplicate=True),
    Output("genie-status",    "children"),
    Input("btn-genie-send",   "n_clicks"),
    Input("btn-new-chat",     "n_clicks"),
    State("genie-input",      "value"),
    State("genie-conv-id",    "data"),
    State("genie-history",    "data"),
    prevent_initial_call=True,
)
def handle_genie(send_clicks, new_clicks, question, conv_id, history):
    ctx     = dash.callback_context
    trigger = ctx.triggered_id

    if trigger == "btn-new-chat":
        welcome = _render_message(
            "genie",
            "New conversation started. Ask anything about banking fraud patterns, "
            "KPIs, wire transfer alerts, or user risk profiles.",
        )
        return [welcome], None, [], "", ""

    if not question or not question.strip():
        raise PreventUpdate

    question = question.strip()
    history  = history or []

    # Add user bubble immediately
    history.append({"role": "user", "content": question,
                    "sql": "", "columns": [], "data": []})

    try:
        if conv_id:
            result = gn.send_followup(conv_id, question)
        else:
            result = gn.start_conversation(question)

        new_conv_id = result.conversation_id

        if result.status == "COMPLETED":
            reply = result.text_response or (
                f"{result.row_count:,} row{'s' if result.row_count != 1 else ''} returned."
                if result.data else "Query completed."
            )
            history.append({
                "role": "genie", "content": reply,
                "sql": result.sql,
                "columns": result.columns,
                "data": result.data[:50],  # cap at 50 rows for display
            })
            status = f"✓ {result.row_count:,} rows · {result.sql[:60]}…" if result.sql else "✓ Completed"
        else:
            error_msg = result.error or result.status
            history.append({
                "role": "genie",
                "content": f"⚠ Could not complete query: {error_msg}. Please try rephrasing.",
                "sql": "", "columns": [], "data": [],
            })
            new_conv_id = conv_id
            status = f"⚠ {result.status}"

    except Exception as exc:
        history.append({
            "role": "genie",
            "content": f"Error contacting Genie: {exc}",
            "sql": "", "columns": [], "data": [],
        })
        new_conv_id = conv_id
        status = f"Error: {exc}"

    # Re-render all messages
    bubbles = [
        _render_message(m["role"], m["content"], m.get("sql", ""),
                        m.get("columns", []), m.get("data", []))
        for m in history
    ]

    return bubbles, new_conv_id, history, "", status


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
