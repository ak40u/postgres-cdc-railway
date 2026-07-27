#!/usr/bin/env bash
# End-to-end check of a deployed change-data-capture pipeline.
#
# One assertion carries the whole thing: write a row, then watch it arrive as a
# change event without anybody having asked for it. That path runs through
# Postgres's write-ahead log, a replication slot, Debezium, and an HTTP sink -
# and there is no way to fake it from either end.
#
#   scripts/verify-cdc.sh https://your-sink.up.railway.app 'the-token'
set -uo pipefail

BASE="${1:?usage: verify-cdc.sh <base-url> <api-token>}"
TOKEN="${2:?usage: verify-cdc.sh <base-url> <api-token>}"
BASE="${BASE%/}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
failed=0

ok()   { echo "  ok   $1${2:+ - $2}"; }
fail() { echo "  FAIL $1 - $2"; failed=1; }
api()  { curl -s --max-time 60 -H "authorization: Bearer $TOKEN" "$@"; }

echo "checking $BASE"

code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 "$BASE/health")
[ "$code" = "200" ] && ok "health" || fail "health" "got $code"

code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 "$BASE/items")
[ "$code" = "401" ] && ok "the application data is not public" || fail "the application data is not public" "got $code"

# 1. Write a row with a name nothing else could produce.
MARKER="widget-$RANDOM$RANDOM"
created=$(api -X POST "$BASE/items" -H 'content-type: application/json' -d "{\"name\":\"$MARKER\",\"quantity\":1}")
case "$created" in
  *"$MARKER"*) ok "row written to the watched table" "$MARKER" ;;
  *) fail "row written to the watched table" "${created:0:160}" ;;
esac

# 2. Wait for it to come back as a change event. Nobody sent it - Postgres
#    published it, Debezium read the log, the sink received it.
found=""
deadline=$(( $(date +%s) + WAIT_SECONDS ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  events=$(api "$BASE/events?limit=50")
  case "$events" in
    *"$MARKER"*) found="$events"; break ;;
  esac
  sleep 5
done

if [ -n "$found" ]; then
  op=$(python3 -c '
import json, sys
data = json.loads(sys.argv[1])
marker = sys.argv[2]
for event in data.get("events", []):
    if marker in json.dumps(event):
        print(event.get("operation") or "?", event.get("source_table") or "?")
        break
' "$found" "$MARKER")
  ok "the insert arrived as a change event" "op=$op"
else
  fail "the insert arrived as a change event" "nothing in ${WAIT_SECONDS}s - check the replication slot and the Debezium logs"
fi

# 3. An update is a second event, so the stream is not just a one-off.
ID=$(python3 -c '
import json, sys
try:
    print(json.loads(sys.argv[1]).get("id", ""))
except Exception:
    pass
' "$created")
if [ -n "$ID" ]; then
  api -X PATCH "$BASE/items/$ID" -H 'content-type: application/json' -d '{"quantity":42}' > /dev/null
  updated=""
  deadline=$(( $(date +%s) + 60 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    events=$(api "$BASE/events?limit=50")
    hits=$(python3 -c '
import json, sys
data = json.loads(sys.argv[1])
marker = sys.argv[2]
print(sum(1 for e in data.get("events", []) if marker in json.dumps(e)))
' "$events" "$MARKER")
    [ "$hits" -ge 2 ] && { updated="yes"; break; }
    sleep 5
  done
  [ -n "$updated" ] && ok "the update arrived as a second event" || fail "the update arrived as a second event" "only one event in 60s"
fi

echo
[ "$failed" = "0" ] && echo "all checks passed" || { echo "some checks failed"; exit 1; }
