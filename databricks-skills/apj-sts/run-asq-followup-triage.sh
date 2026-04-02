#!/bin/bash
# APJ STS Follow-up ASQ Triage Runner
# Queries SFDC for On Hold / Under Review ASQs and surfaces items needing attention.

EXCLUDE="'AR-000114773'"
LOG_FILE="/tmp/asq-triage-$(TZ='Asia/Kolkata' date +%Y%m%d-%H%M).log"

echo "=== APJ STS Follow-up Triage — $(TZ='Asia/Kolkata' date '+%d %b %Y %I:%M %p IST') ===" | tee "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Authenticate
ACCESS_TOKEN=$(sf org display --json 2>/dev/null | jq -r '.result.accessToken')
INSTANCE_URL=$(sf org display --json 2>/dev/null | jq -r '.result.instanceUrl')

if [ -z "$ACCESS_TOKEN" ] || [ "$ACCESS_TOKEN" = "null" ]; then
  echo "ERROR: Not authenticated to Salesforce. Run: sf org login web --instance-url=https://databricks.my.salesforce.com/ --set-default" | tee -a "$LOG_FILE"
  osascript -e 'display notification "Not authenticated to Salesforce — login required before triage." with title "ASQ Triage ❌" sound name "Basso"'
  exit 1
fi

# Fetch On Hold / Under Review ASQs (excluding MIXI AR-000114773)
ASQS=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
  "$INSTANCE_URL/services/data/v62.0/query?q=SELECT+Id,Name,Status__c,Account_Name__c,Start_Date__c,End_Date__c,CreatedDate+FROM+ApprovalRequest__c+WHERE+OwnerId='00G8Y000006Ce6mUAC'+AND+(Status__c='On+Hold'+OR+Status__c='Under+Review')+AND+Name+!=$EXCLUDE+ORDER+BY+CreatedDate+ASC")

TOTAL=$(echo "$ASQS" | jq '.totalSize')
echo "Found $TOTAL ASQs under review / on hold." | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

TODAY=$(date -u +%Y-%m-%d)
URGENT=()
NEEDS_REMINDER=()
WAITING=()

while IFS= read -r row; do
  NAME=$(echo "$row" | jq -r '.Name')
  ACCOUNT=$(echo "$row" | jq -r '.Account_Name__c')
  STATUS=$(echo "$row" | jq -r '.Status__c')
  START=$(echo "$row" | jq -r '.Start_Date__c')
  END=$(echo "$row" | jq -r '.End_Date__c')
  SFDC_ID=$(echo "$row" | jq -r '.Id')

  # Get latest chatter post date
  LAST_CHATTER=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
    "$INSTANCE_URL/services/data/v62.0/chatter/feeds/record/$SFDC_ID/feed-elements?pageSize=1" \
    | jq -r '.elements[0].createdDate // empty')

  # Check for response (any post NOT by Saroj after last triage post)
  ALL_CHATTER=$(curl -s -H "Authorization: Bearer $ACCESS_TOKEN" \
    "$INSTANCE_URL/services/data/v62.0/chatter/feeds/record/$SFDC_ID/feed-elements?pageSize=25")
  LAST_TRIAGE_DATE=$(echo "$ALL_CHATTER" | jq -r '[.elements[] | select(.actor.displayName == "Saroj Venkatesh")] | .[0].createdDate // empty')
  RESPONSE=$(echo "$ALL_CHATTER" | jq -r --arg d "$LAST_TRIAGE_DATE" \
    '[.elements[] | select(.actor.displayName != "Saroj Venkatesh" and .createdDate > $d)] | .[0].actor.displayName // empty')

  # Days since last triage chatter
  if [ -n "$LAST_TRIAGE_DATE" ]; then
    LAST_DATE=$(echo "$LAST_TRIAGE_DATE" | cut -c1-10)
    DAYS_SINCE=$(( ( $(date -j -f "%Y-%m-%d" "$TODAY" +%s) - $(date -j -f "%Y-%m-%d" "$LAST_DATE" +%s) ) / 86400 ))
  else
    DAYS_SINCE=99
  fi

  # Start date proximity
  START_DAYS=$(( ( $(date -j -f "%Y-%m-%d" "${START:-9999-12-31}" +%s 2>/dev/null || echo 0) - $(date -j -f "%Y-%m-%d" "$TODAY" +%s) ) / 86400 ))

  LINE="$NAME | $ACCOUNT | $STATUS | Start: $START | Last triage: ${LAST_DATE:-none} (${DAYS_SINCE}d ago)"

  if [ -n "$RESPONSE" ]; then
    LINE="$LINE | ✅ Response from $RESPONSE"
    URGENT+=("🔵 RESPONSE: $LINE")
  elif [ "$START_DAYS" -le 1 ] 2>/dev/null; then
    URGENT+=("🔴 URGENT (start today/tomorrow): $LINE")
  elif [ "$DAYS_SINCE" -gt 3 ]; then
    NEEDS_REMINDER+=("🟡 REMINDER DUE (${DAYS_SINCE}d): $LINE")
  else
    WAITING+=("⏳ Waiting: $LINE")
  fi

done < <(echo "$ASQS" | jq -c '.records[]')

echo "--- URGENT / RESPONSES ---" | tee -a "$LOG_FILE"
for item in "${URGENT[@]}"; do echo "  $item" | tee -a "$LOG_FILE"; done
[ ${#URGENT[@]} -eq 0 ] && echo "  None" | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "--- REMINDERS DUE (>3 days) ---" | tee -a "$LOG_FILE"
for item in "${NEEDS_REMINDER[@]}"; do echo "  $item" | tee -a "$LOG_FILE"; done
[ ${#NEEDS_REMINDER[@]} -eq 0 ] && echo "  None" | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "--- WAITING (within 3-day window) ---" | tee -a "$LOG_FILE"
for item in "${WAITING[@]}"; do echo "  $item" | tee -a "$LOG_FILE"; done
[ ${#WAITING[@]} -eq 0 ] && echo "  None" | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "Full log: $LOG_FILE" | tee -a "$LOG_FILE"

# macOS notification
URGENT_COUNT=${#URGENT[@]}
REMINDER_COUNT=${#NEEDS_REMINDER[@]}
NOTIF_MSG="$TOTAL ASQs | 🔴 ${URGENT_COUNT} urgent | 🟡 ${REMINDER_COUNT} reminders due"
osascript -e "display notification \"$NOTIF_MSG\" with title \"APJ ASQ Triage 🔔\" sound name \"Glass\""

# Open log in terminal
open -a Terminal "$LOG_FILE" 2>/dev/null || cat "$LOG_FILE"
