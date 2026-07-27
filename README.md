# Postgres change data capture, for Railway

Every insert, update and delete in a Postgres schema, delivered to an HTTP
endpoint of yours — without polling, without touching your application, and
without Kafka.

## Why this exists

Search the catalogue for change data capture and there is nothing: 3991
templates, and Debezium is not mentioned once. Postgres is the most-installed
template on the platform.

So the usual answer is a `SELECT ... WHERE updated_at > ?` loop somewhere, which
misses deletes, misses anything that changed twice between polls, and gets
slower as the table grows.

This is the other answer: Postgres already writes every change to its
write-ahead log. Debezium reads that log through a replication slot and posts
what it finds to a URL.

## The shape

```
your app ──▶ app.items          (a table like any other)
                 │
        write-ahead log
                 │
             Debezium Server    (a replication slot, no Kafka)
                 │
             HTTP POST ──▶ /events   ← replace with your endpoint
```

Three services: Postgres with logical decoding turned on, Debezium Server, and a
small receiver that stores what arrives so you can see it working.

## Try it

```bash
curl -X POST https://your-sink.up.railway.app/items \
  -H "authorization: Bearer $API_TOKEN" -H 'content-type: application/json' \
  -d '{"name":"widget","quantity":1}'

# a moment later
curl https://your-sink.up.railway.app/events -H "authorization: Bearer $API_TOKEN"
```

The event carries the new row, the operation (`c`, `u`, `d`) and the table it
came from.

## Prove it works

```bash
scripts/verify-cdc.sh https://your-sink.up.railway.app 'the-token'
```

One assertion carries the whole thing: write a row, then watch it arrive as a
change event that nobody sent. That path runs through the write-ahead log, a
replication slot, Debezium and an HTTP POST — and cannot be faked from either
end. It then updates the row and waits for the second event.

## Making it yours

Point `DEBEZIUM_SINK_HTTP_URL` at your own service and delete the receiver. Your
endpoint should answer 2xx when it has stored the change and 5xx when it has
not: Debezium retries on anything else, which is exactly the behaviour you want
from a delivery you cannot afford to lose.

Change `DEBEZIUM_SOURCE_SCHEMA_INCLUDE_LIST` to the schema you care about, or
use `DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST` for individual tables.

## What to watch out for

- **A replication slot nobody reads fills your disk.** Postgres keeps every WAL
  segment a slot has not consumed. If Debezium is stopped for a week, the disk
  grows for a week. Watch it:
  ```sql
  select slot_name, active,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as behind
  from pg_replication_slots;
  ```
  If you remove this template, drop the slot: `select pg_drop_replication_slot('debezium_slot')`.
- **The receiver must not write to a watched table.** Storing an event in a table
  Debezium is watching turns one change into an endless loop. Here the events go
  to `public.cdc_events` while only `app.*` is watched.
- **Offsets are kept in the container, not on a volume.** Postgres's replication
  slot is the authoritative position, so a restart resumes from there — at the
  cost of re-delivering the last few changes. Add a volume on `/data` to the
  Debezium service if you would rather it resumed exactly.
- **Events are at-least-once.** A retry after a partial failure delivers the same
  change twice; make your endpoint idempotent.

## Configuration

| Variable | Where | Purpose |
|----------|-------|---------|
| `DEBEZIUM_SOURCE_DATABASE_*` | Debezium | Host, port, user, password, database |
| `DEBEZIUM_SOURCE_SCHEMA_INCLUDE_LIST` | Debezium | Which schema is watched; `app` by default |
| `DEBEZIUM_SINK_HTTP_URL` | Debezium | Where changes are posted |
| `DEBEZIUM_SOURCE_SLOT_NAME` | Debezium | The replication slot; one per consumer |
| `API_TOKEN` | Sink | Protects the application routes |

Postgres runs with `wal_level=logical`; without it there is no logical decoding
and no CDC at all.

## License

MIT. Debezium is Apache-2.0.
