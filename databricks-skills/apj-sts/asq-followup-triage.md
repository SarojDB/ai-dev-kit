# APJ STS ASQ Follow-up Triage

Goal: Triage responses to ASQs that were put 'On Hold' or 'Under Review'. Follow the steps listed below in plan mode and recommend actions that can be manually reviewed before execution.

## Steps

### 1. Retrieve On Hold / Under Review ASQs
Start by getting a list of all ASQs in the **Technical_Onboarding_Services_APJ** queue in SFDC with status **'On Hold'** or **'Under Review'**.

### 2. Pre-Chatter Checks (run before reading chatter)

Before reading chatter history, perform the following checks on each ASQ record:

**a. Attachments check**
Query `ContentDocumentLink` for each ASQ. If a workspace setup checklist is already attached, do not ask for it again — proceed directly to engineer assignment steps.

**b. Use case stage (live check)**
Re-query the linked use case stage directly from SFDC — do not rely on stage information from initial triage, as it may be stale. If the stage has been advanced since initial triage, acknowledge the progress and drop any stage-related blocker from the reminder.

The UC is **not** a direct field on `ApprovalRequest__c`. It is linked via the `Approved_UseCase__c` junction object. Query it as follows:

```
SELECT Id, Use_Case__c, Stage__c
FROM Approved_UseCase__c
WHERE Approval_Request__c = '{asq_id}'
```

The `Stage__c` field on `Approved_UseCase__c` maps to UC stages as follows: `3-Evaluating` = U3, `4-Confirming` = U4, `5-Onboarding` = U5, `6-Consuming` = U6. If you need the `Concatenated_Stage_Name__c` value, take the `Use_Case__c` ID returned and run a separate query: `SELECT Id, Concatenated_Stage_Name__c FROM UseCase__c WHERE Id = '<uc_id>'`.

Apply UC stage requirements based on `Support_Type__c`:

- **Platform Administration** (workspace setup requests): UC stage **U2 or above** is acceptable. Do **not** flag or block these ASQs for UC stage — focus on obtaining the workspace setup checklist instead.
- **All other support types** (ML & GenAI, Data Engineering, etc.): UC stage **U3 or above** is required before an engineer can be assigned.

Do **not** query `UseCase__c` as a field directly on `ApprovalRequest__c` — that field does not exist and will return null or a 400 error.

**c. Requestor fallback**
If `Requestor__r` is null on the ASQ record, extract the requestor from the first @mention in the earliest triage chatter post and use that user for structured mentions in all chatter updates.

**d. Scope check for non-Platform Administration ASQs**
For ASQs where `Support_Type__c` is not `Platform Administration` (e.g. ML & GenAI, Data Engineering), verify the request is within STS scope before defaulting to a workspace checklist ask. The workspace checklist flow only applies to workspace setup requests.

### 3. Read All Chatter History
For **every** ASQ retrieved, read **all** chatter posts and comments in full — do not limit to recent activity. This is required to understand the full history, identify the last triage chatter sent, and determine whether a response has been received.

**Two-step fetch required:** The Chatter `feed-elements` endpoint (`GET /chatter/feeds/record/{id}/feed-elements`) returns top-level posts with a `comment_count` field but does **not** include inline comment content. For every post where `comment_count > 0`, fetch the comments separately via:
```
GET /chatter/feed-elements/{postId}/capabilities/comments/items
```
Skipping this step will cause inline responses from requestors to be silently missed.

For each ASQ, identify:
- The **last chatter post sent by the triage team** and its date
- Whether there has been a **response from the requestor or any other party** after that post — including inline comments fetched via the above endpoint
- The **full content** of any response received

### 4. Determine Action Based on Chatter State
Apply the following rules strictly:

- **No response received AND last triage chatter was sent within the last 3 days:** Do **NOT** formulate a chatter update. Mark as "Waiting — no action needed".
- **No response received AND last triage chatter was sent more than 3 days ago:** Send a reminder to the requestor via SFDC chatter/activity using a **structured Salesforce Mention**.
- **Response received from requestor or other party:** Formulate a chatter update based on the content of the response (see Step 6).
- **No chatter history at all:** Send initial follow-up to requestor.

**Duplicate post guard:** Before posting any chatter, verify the most recent post does not already contain the same core message sent within the last 2 hours. If a duplicate is detected, skip — do not post again.

**End date expiry:** If the ASQ's end date has passed and no response has been received, explicitly ask whether the engagement is still required or should be closed — regardless of the 3-day window. Do not continue sending checklist or schedule reminders past the end date without acknowledging it.

**Start date proximity:** If the ASQ's start date is today or tomorrow, flag it for immediate attention regardless of the 3-day window and surface it in the output table even if no chatter action is due.

**Read description before templating:** Before sending a checklist ask, read the full request description. If it indicates an existing workspace (e.g. "e2 trial account created", "workspace already provisioned"), do not ask generically for workspace type and checklist — instead ask for the workspace URL and confirm remaining setup steps.

**Final notice threshold:** After 3 or more unanswered triage posts with zero engagement (no chatter responses, no record updates, no attachments added), send a final notice with an explicit close-by date rather than another standard reminder.

### 5. Escalate Long-Standing Under Review ASQs
For ASQs that have been 'Under Review' for more than 1 week:
- Change status to **'On Hold'**
- Update the SFDC chatter/activity informing the requestor using a **structured Salesforce Mention**

### 6. Recommend Next Steps for Updated ASQs
For ASQs where a response to triage chatter has been received, recommend the next steps:
- **Ready for triage:** Before proceeding to engineer assignment, run the **repeat engagement check** (see Step 6a below). If no repeat engagement is detected, follow the steps and practices as per the ASQ triage process (see `new-asq-triage.md`). When assigning to an engineer, set status to **'In Progress'** — there is no 'Assigned' status. Also update the **OwnerId** of the ASQ record to the assigned engineer's Salesforce User ID.
- **Workspace setup ASQ — checklist still not attached:** Re-send a reminder to the requestor using a **structured Salesforce Mention** with a link to [go/wssetup-cheatsheet](https://sites.google.com/databricks.com/sts-workspace-setup) and keep status as **'Under Review'**.
- **No longer required:** Modify the status to **"Rejected"** and recommend action based on latest comment on SFDC chatter/activity

#### 6a. Repeat Engagement Check (run before assignment for all non-workspace-setup ASQs)

Before assigning an engineer to any ASQ that is ready for triage, check whether STS has previously engaged with the same account on the same or closely related scope:

**Step 1 — Query prior ASQs for the account:**
```
SELECT Id, Name, Status__c, Support_Type__c, CreatedDate, Owner.Name
FROM ApprovalRequest__c
WHERE Account_Name__c LIKE '%<account_name>%'
AND CreatedDate >= <date_4_months_ago>T00:00:00Z
ORDER BY CreatedDate ASC
```

**Step 2 — Cross-reference the use case:**
For any prior ASQs found with status `Complete` or `In Progress`, query `Approved_UseCase__c` for both the prior and current ASQ and compare the `Use_Case__c` IDs. If the **same use case ID** is linked to both ASQs, flag this as a repeat engagement.

**Step 3 — Apply repeat engagement policy:**
If a prior completed ASQ is found for the same account with the same use case and similar support scope:
- Do **not** proceed directly to assignment.
- Post a chatter update to the requestor that:
  - References the prior ASQ number and engagement dates
  - States that STS cannot be repeatedly engaged for the same service on the same use case
  - Asks the requestor to confirm what steps and deliverables from the prior engagement have been executed by the customer
  - Asks what specifically is outstanding or has regressed to require re-engagement
  - Suggests a scoping call to define what is incremental versus duplicating prior work
- Keep status as **'Under Review'** pending the requestor's response.

**Step 4 — Continuity engineer recommendation:**
If the repeat engagement check passes (or after scope is confirmed as incremental), recommend the same engineer who executed the prior ASQ for continuity — they will have context on the customer environment. Reference the prior ASQ in the assignment chatter.

### 7. Output Format
Show the final results in a **tabular format** with recommendations on all ASQs analyzed.

### 8. Chatter Updates
Formulate an update for the SFDC activity/chatter feed to inform the requestor and assignee. **Always include the original requestor in every SFDC chatter update using a structured Salesforce Mention, even when assigning to a different engineer.**

**MANDATORY:** Every SFDC chatter/activity update — without exception — must end with the following statement:

> *'Triaged with the help of Databricks FE AI agents. Please respond via the ASQ or apj-sts slack channel if we got this wrong. We are still evaluating and refining our AI tools and execution.'*

This applies to all chatter posts including: reminders, status change notifications, assignment notifications, requestor visibility updates, closure messages, and any other chatter activity on ASQs.

### 9. Structured Mentions
Ensure a **structured Salesforce Mention** is used to respond to requestors or to include engineers assigned the ASQ or marked for #Shadow on the ASQ.
