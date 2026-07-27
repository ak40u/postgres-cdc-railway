# Deploy and Host Postgres Change Data Capture on Railway

Every insert, update and delete in a Postgres schema, delivered to an HTTP endpoint of yours — without polling, without changing your application, and without Kafka.

Three services: Postgres with logical decoding enabled, Debezium Server, and a small receiver that records what arrives so you can watch it work.

## About Hosting Change Data Capture

Search this catalogue for change data capture and there is nothing. Out of 3991 templates, Debezium is not mentioned once — while Postgres is the most-installed template on the platform, at over 340,000 deployments.

So the usual answer is a polling loop: `select ... where updated_at > ?`, every few seconds, forever. It misses deletes entirely. It misses anything that changed twice between two polls. It gets slower as the table grows, and it runs whether or not anything happened.

Postgres already writes every change to its write-ahead log. Debezium reads that log through a replication slot and posts what it finds to a URL — no broker in between, no schema registry, no cluster.

## Common Use Cases

- **Keep a search index current.** Every row change becomes a document update, without your application knowing anything about the index.
- **Notify on state changes.** An order becomes `paid` in the database and a webhook fires, with no code in the payment path.
- **Mirror to a warehouse.** Stream changes to ClickHouse or anywhere else instead of re-copying tables nightly.

## Dependencies for Change Data Capture Hosting

### Deployment Dependencies

- [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html) 3.6 — standalone, no Kafka Connect
- Postgres 17 with `wal_level=logical`
- A receiver service demonstrating the sink; replace it with your own
- [Template source](https://github.com/ak40u/postgres-cdc-railway)

### Implementation Details

- **Postgres starts through its own entrypoint**, not by replacing the command. A start command replaces the entrypoint too, so `postgres -c wal_level=logical` runs as root and the server refuses outright: *"root execution of the PostgreSQL server is not permitted"*. Going through `docker-entrypoint.sh` keeps the switch to the unprivileged user — and the chown of the mounted volume.
- **The receiver writes outside the watched schema.** Storing an event in a table Debezium is watching turns one change into an endless loop; here events land in `public.cdc_events` while only `app.*` is watched.
- **The receiver answers 5xx on failure**, so Debezium retries instead of dropping the change. Delivery is at-least-once by design.
- **Offsets are kept on a volume.** Without one, a restart re-reads from wherever the slot happens to be — duplicates at best, silence at worst.
- **`ExtractNewRecordState` is applied**, so the payload is the row plus a little metadata rather than Debezium's full before/after envelope. Easier to consume, and the operation is still there as `__op`.

The repository carries `scripts/verify-cdc.sh`, whose central assertion cannot be faked from either end: write a row, then watch it arrive as a change event nobody sent — through the write-ahead log, a replication slot, Debezium and an HTTP POST. It then updates the row and waits for the second event.

**One thing to watch:** a replication slot nobody reads keeps every WAL segment it has not consumed, so a Debezium that is stopped for a week grows your disk for a week. The README carries the query to check slot lag and the command to drop the slot if you remove this.

## Why Deploy Change Data Capture on Railway?

Railway is a singular platform to deploy your infrastructure stack. Railway will host your infrastructure so you don't have to deal with configuration, while allowing you to vertically and horizontally scale it.

By deploying change data capture on Railway, you are one step closer to supporting a complete full-stack application with minimal burden. Host your servers, databases, AI agents, and more on Railway.
