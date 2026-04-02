#!/usr/bin/env python3
"""
APJ STS ASQ Follow-up Triage
Skill: /Users/saroj.venkatesh/ai-dev-kit/databricks-skills/apj-sts/asq-followup-triage.md

Usage:
  python3 run-asq-followup-triage.py

Requires: sf CLI authenticated as saroj.venkatesh@databricks.com
"""

import subprocess, json, datetime, sys

ORG = "saroj.venkatesh@databricks.com"
QUEUE_ID = "00G8Y000006Ce6mUAC"  # Technical_Onboarding_Services_APJ
API = "v66.0"
NOW = datetime.datetime.now(datetime.UTC)


# ── Helpers ──────────────────────────────────────────────────────────────────

def sf_rest(path):
    """GET via sf api request rest (handles auth automatically)."""
    r = subprocess.run(
        ["sf", "api", "request", "rest", path, "-o", ORG],
        capture_output=True, text=True
    )
    return json.loads(r.stdout)


def sf_query(q):
    """Run SOQL via sf data query."""
    q = " ".join(q.split())
    r = subprocess.run(
        ["sf", "data", "query", "-o", ORG, "--query", q, "--json"],
        capture_output=True, text=True
    )
    return json.loads(r.stdout).get("result", {}).get("records", [])


def parse_dt(s):
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.UTC)
    except Exception:
        return None


def age_str(dt):
    if not dt:
        return "?"
    h = (NOW - dt).total_seconds() / 3600
    if h < 48:
        return f"{h:.1f}h"
    return f"{h/24:.1f}d"


def get_all_comments(element):
    """
    Fetch comments for a feed element via the capabilities/comments/items endpoint.
    The top-level comments.total field is unreliable (returns 0 for rich-text comments);
    always fetch directly from the endpoint.
    """
    elem_id = element.get("id", "")
    if not elem_id:
        return []
    data = sf_rest(
        f"/services/data/{API}/chatter/feed-elements/{elem_id}/capabilities/comments/items?pageSize=50"
    )
    return data.get("items", [])


def comment_text(c):
    """Extract plain text from a comment's body (handles both text and messageSegments)."""
    body = c.get("body") or {}
    # try plain text first
    t = body.get("text", "") or ""
    if t.strip():
        return t.strip()
    # fall back to messageSegments
    parts = []
    for seg in body.get("messageSegments", []):
        seg_text = seg.get("text", "") or ""
        if seg.get("type") == "Mention":
            parts.append(seg_text)
        elif seg.get("type") == "Link":
            parts.append(seg.get("url", seg_text))
        elif seg_text.strip():
            parts.append(seg_text.strip())
    return " ".join(parts).strip()


# ── Main triage ───────────────────────────────────────────────────────────────

def run_triage():
    print(f"APJ STS Follow-up Triage — {NOW.strftime('%Y-%m-%d %H:%M')} UTC\n")

    asqs = sf_query(f"""
        SELECT Id, Name, Status__c, Support_Type__c,
               Start_Date__c, End_Date__c,
               Requestor__c, Requestor__r.Name,
               Account_Name__c, Use_Case__c,
               LastModifiedDate, CreatedDate
        FROM ApprovalRequest__c
        WHERE OwnerId = '{QUEUE_ID}'
        AND Status__c IN ('On Hold', 'Under Review')
        ORDER BY LastModifiedDate DESC
    """)

    # Filter: skip ASQs whose start date is more than 10 days away (not yet actionable)
    def start_days_away(a):
        s = a.get("Start_Date__c", "")
        if not s:
            return 0
        try:
            sd = datetime.datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=datetime.UTC)
            return (sd - NOW).days
        except Exception:
            return 0

    skipped = [a for a in asqs if start_days_away(a) > 10]
    asqs    = [a for a in asqs if start_days_away(a) <= 10]

    print(f"Queue: {len(asqs)} active | {len(skipped)} skipped (start > 10d away)\n")
    if skipped:
        for s in skipped:
            print(f"  SKIPPED: {s['Name']} | {s.get('Account_Name__c','?')} | start {s.get('Start_Date__c','')} ({start_days_away(s)}d away)")
        print()
    print("=" * 80 + "\n")

    results = []

    for asq in asqs:
        aid        = asq["Id"]
        name       = asq["Name"]
        status     = asq["Status__c"]
        stype      = asq.get("Support_Type__c", "")
        account    = asq.get("Account_Name__c", "?")
        start      = asq.get("Start_Date__c", "")
        end        = asq.get("End_Date__c", "")
        uc_id      = asq.get("Use_Case__c", "")
        req_name   = (asq.get("Requestor__r") or {}).get("Name", "")
        req_id     = asq.get("Requestor__c", "")

        print(f"=== {name} | {account} | {status} | {stype} ===")
        print(f"  ID: {aid} | Requestor: {req_name or '(null)'}")

        # ── Date flags ───────────────────────────────────────────────────────
        date_flags = []
        if start:
            try:
                sd = datetime.datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=datetime.UTC)
                d  = (sd - NOW).days
                if d < 0:   date_flags.append(f"START PAST ({start})")
                elif d == 0: date_flags.append("START TODAY")
                elif d == 1: date_flags.append(f"START TOMORROW ({start})")
                else:        date_flags.append(f"start {start} ({d}d)")
            except Exception:
                pass
        if end:
            try:
                ed = datetime.datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=datetime.UTC)
                if ed < NOW:
                    date_flags.append(f"END PASSED ({end})")
            except Exception:
                pass

        cdt = parse_dt(asq.get("CreatedDate", ""))
        if cdt:
            print(f"  Created: {cdt.strftime('%Y-%m-%d')} ({(NOW-cdt).days}d ago)")
        print(f"  Dates: {' | '.join(date_flags) if date_flags else 'ok'}")

        # ── Step 2a: Attachments ─────────────────────────────────────────────
        att_recs = sf_query(
            f"SELECT ContentDocument.Title FROM ContentDocumentLink WHERE LinkedEntityId = '{aid}'"
        )
        att_titles = [(a.get("ContentDocument") or {}).get("Title", "?") for a in att_recs]
        ws_attached = any(
            kw in t.lower() for t in att_titles
            for kw in ("checklist", "workspace", "setup")
        )
        print(f"  Attachments: {att_titles or 'None'} | WS checklist: {ws_attached}")

        # ── Step 2b: UC stage (live) ─────────────────────────────────────────
        uc_stage = uc_name = None
        if uc_id:
            try:
                uc_recs = sf_query(
                    f"SELECT Id, Name, Stage__c FROM UseCase__c WHERE Id = '{uc_id}'"
                )
                if uc_recs:
                    uc_stage = uc_recs[0].get("Stage__c", "?")
                    uc_name  = uc_recs[0].get("Name", "?")
                    print(f"  UseCase: {uc_name} | Stage: {uc_stage}")
            except Exception as e:
                print(f"  UseCase: err ({e})")
        else:
            print("  UseCase: none linked")

        # ── Step 3: Read all chatter ─────────────────────────────────────────
        chatter  = sf_rest(f"/services/data/{API}/chatter/feeds/record/{aid}/feed-elements?pageSize=50")
        elements = chatter.get("elements", [])
        print(f"  Chatter feed elements: {len(elements)}")

        # Find last triage post
        last_triage_idx  = None
        last_triage_date = None
        triage_posts     = []
        for i, e in enumerate(elements):
            actor = (e.get("actor") or {}).get("displayName", "")
            if "Saroj" in actor:
                triage_posts.append(i)
                if last_triage_idx is None:
                    last_triage_idx  = i
                    last_triage_date = parse_dt(e.get("createdDate", ""))

        age_h = None
        if last_triage_date:
            age_h    = (NOW - last_triage_date).total_seconds() / 3600
            overdue  = "  ⚠️ OVERDUE" if age_h > 72 else ""
            print(f"  Last triage: {last_triage_date.strftime('%Y-%m-%d %H:%M')} UTC "
                  f"({age_h:.1f}h ago){overdue} | Posts: {len(triage_posts)}")

        # ── Collect responses after last triage post ─────────────────────────
        responses = []

        if last_triage_idx is not None:
            # newer feed elements (posted after last triage)
            for e in elements[:last_triage_idx]:
                actor = (e.get("actor") or {}).get("displayName", "")
                if "Saroj" not in actor:
                    body  = (e.get("body") or {}).get("text", "") or ""
                    ts    = e.get("createdDate", "")[:16].replace("T", " ")
                    responses.append({"actor": actor, "body": body, "ts": ts, "source": "post"})

            # comments on the last triage post — ALWAYS fetch via capabilities endpoint
            for c in get_all_comments(elements[last_triage_idx]):
                ca   = (c.get("user") or {}).get("displayName", "?")
                ca_id = (c.get("user") or {}).get("id", "")
                if "Saroj" not in ca:
                    cb = comment_text(c)
                    ct = c.get("createdDate", "")[:16].replace("T", " ")
                    responses.append({"actor": ca, "actor_id": ca_id, "body": cb, "ts": ct, "source": "comment"})

        if responses:
            print(f"  ✅ RESPONSES ({len(responses)}):")
            for r in responses:
                print(f"    [{r['ts']}] {r['actor']}: {r['body'][:500]}")
        else:
            print("  No responses after last triage post")

        last_triage_body = ""
        if last_triage_idx is not None:
            last_triage_body = (elements[last_triage_idx].get("body") or {}).get("text", "")
            print(f"  Last triage msg: {last_triage_body[:300]}")

        # ── Pre-post refresh (Step 2b / skill rule) ──────────────────────────
        # Always re-query ASQ record, UC stage, and attachments immediately
        # before surfacing any proposed action — ensures proposed posts reflect
        # the latest SFDC state, not the snapshot at query time.
        fresh_status = status
        fresh_uc_stage = uc_stage
        fresh_ws_attached = ws_attached
        fresh_att_titles = att_titles

        if responses or (age_h and age_h > 72):
            print(f"  [Pre-post refresh]")
            # Re-query ASQ status + owner
            fresh_asq = sf_query(
                f"SELECT Status__c, OwnerId, Owner.Name FROM ApprovalRequest__c WHERE Id = '{aid}'"
            )
            if fresh_asq:
                fresh_status   = fresh_asq[0].get("Status__c", status)
                fresh_owner    = (fresh_asq[0].get("Owner") or {}).get("Name", "?")
                fresh_owner_id = fresh_asq[0].get("OwnerId", "")
                print(f"    Status: {fresh_status} | Owner: {fresh_owner} ({fresh_owner_id})")

            # Re-query attachments
            fresh_att_recs = sf_query(
                f"SELECT ContentDocument.Title FROM ContentDocumentLink WHERE LinkedEntityId = '{aid}'"
            )
            fresh_att_titles  = [(a.get("ContentDocument") or {}).get("Title", "?") for a in fresh_att_recs]
            fresh_ws_attached = any(
                kw in t.lower() for t in fresh_att_titles
                for kw in ("checklist", "workspace", "setup")
            )
            print(f"    Attachments (fresh): {fresh_att_titles or 'None'} | WS checklist: {fresh_ws_attached}")

            # Re-query UC stage
            if uc_id:
                try:
                    fresh_uc_recs = sf_query(
                        f"SELECT Stage__c, Name FROM UseCase__c WHERE Id = '{uc_id}'"
                    )
                    if fresh_uc_recs:
                        fresh_uc_stage = fresh_uc_recs[0].get("Stage__c", uc_stage)
                        fresh_uc_name  = fresh_uc_recs[0].get("Name", uc_name)
                        changed = " ← UPDATED" if fresh_uc_stage != uc_stage else ""
                        print(f"    UC Stage (fresh): {fresh_uc_name} | {fresh_uc_stage}{changed}")
                except Exception:
                    pass

        results.append({
            "name": name, "account": account, "status": fresh_status, "stype": stype,
            "start": start, "end": end, "date_flags": date_flags,
            "ws_attached": fresh_ws_attached, "att_titles": fresh_att_titles,
            "uc_stage": fresh_uc_stage, "uc_name": uc_name,
            "triage_count": len(triage_posts), "last_triage_date": last_triage_date,
            "age_h": age_h, "responses": responses, "last_triage_body": last_triage_body,
            "req_name": req_name, "req_id": req_id, "aid": aid,
        })
        print()

    # ── Summary table ─────────────────────────────────────────────────────────
    print("=" * 80)
    print(f"{'ASQ':20} {'Account':28} {'Status':14} {'Last Triage':12} {'Responses':15} Flags")
    print("=" * 80)
    for r in results:
        a_str  = f"{r['age_h']:.1f}h" if r["age_h"] else "no triage"
        ov     = " ⚠️" if r["age_h"] and r["age_h"] > 72 else ""
        resp   = f"✅ {len(r['responses'])} resp" if r["responses"] else "no response"
        flags  = " | ".join(r["date_flags"]) if r["date_flags"] else ""
        print(f"{r['name']:20} {r['account'][:28]:28} {r['status']:14} {a_str+ov:14} {resp:15} {flags}")

    return results


if __name__ == "__main__":
    run_triage()
