/**
 * Two halves of the demonstration in one small service.
 *
 * `/items` is the application: it writes rows to a table nobody watches on
 * purpose. `/events` is the receiver: Debezium posts every change it sees there,
 * and it writes them down so you can look.
 *
 * Replace the second half with your own endpoint - that is the whole point of
 * sinking changes to HTTP rather than to a broker.
 */
import express from "express"
import { Pool } from "pg"

const port = Number(process.env.PORT ?? 8080)
const token = process.env.API_TOKEN ?? ""

if (token.length < 16) {
  console.error("API_TOKEN is missing or shorter than 16 characters. Refusing to start with an open endpoint.")
  process.exit(1)
}

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 5, connectionTimeoutMillis: 10_000 })

const SCHEMA = `
-- Watched by Debezium. Everything that happens here becomes an event.
create schema if not exists app;

create table if not exists app.items (
  id          bigserial primary key,
  name        text not null,
  quantity    integer not null default 1,
  updated_at  timestamptz not null default now()
);

-- Deliberately outside the watched schema: writing a received event must not
-- produce another event, or one change becomes an endless loop.
create table if not exists public.cdc_events (
  id          bigserial primary key,
  operation   text,
  source_table text,
  payload     jsonb not null,
  received_at timestamptz not null default now()
);

create index if not exists cdc_events_received_idx on public.cdc_events (received_at desc);
`

const app = express()
app.disable("x-powered-by")
app.use(express.json({ limit: "4mb" }))

const authorize = (req: express.Request, res: express.Response, next: express.NextFunction) => {
  if (req.headers.authorization !== `Bearer ${token}`) {
    res.status(401).json({ error: "unauthorized" })
    return
  }
  next()
}

/**
 * The receiver. Debezium's HTTP sink posts one change at a time, and expects a
 * 2xx - anything else and it retries, which is the behaviour you want.
 */
app.post("/events", async (req, res) => {
  const body = req.body ?? {}
  // Debezium's envelope varies by configuration: unwrapped events carry `op`
  // and `source`, extracted-state events carry the row itself with metadata in
  // `__op`. Both shapes are stored as they arrive.
  const payload = body.payload ?? body
  const operation = payload.op ?? payload.__op ?? null
  const table = payload.source?.table ?? payload.__table ?? null

  try {
    await pool.query(
      `insert into public.cdc_events (operation, source_table, payload) values ($1, $2, $3)`,
      [operation, table, JSON.stringify(body)],
    )
    res.status(204).end()
  } catch (error) {
    // A 5xx makes Debezium retry rather than drop the change.
    console.error("could not record the change", error)
    res.status(500).json({ error: "could not record the change" })
  }
})

app.get("/events", authorize, async (req, res) => {
  const limit = Math.min(Number(req.query.limit ?? 50), 200)
  const { rows } = await pool.query(
    `select id::text, operation, source_table, payload, received_at
     from public.cdc_events order by received_at desc limit $1`,
    [limit],
  )
  res.json({ events: rows })
})

app.post("/items", authorize, async (req, res) => {
  const name = String(req.body?.name ?? "").slice(0, 200)
  const quantity = Number(req.body?.quantity ?? 1)
  if (!name) {
    res.status(400).json({ error: "name is required" })
    return
  }
  const { rows } = await pool.query(
    `insert into app.items (name, quantity) values ($1, $2) returning id::text, name, quantity`,
    [name, quantity],
  )
  res.status(201).json(rows[0])
})

app.patch("/items/:id", authorize, async (req, res) => {
  const { rows } = await pool.query(
    `update app.items set quantity = $2, updated_at = now() where id = $1 returning id::text, name, quantity`,
    [req.params.id, Number(req.body?.quantity ?? 1)],
  )
  if (!rows[0]) {
    res.status(404).json({ error: "not found" })
    return
  }
  res.json(rows[0])
})

app.get("/items", authorize, async (_req, res) => {
  const { rows } = await pool.query(`select id::text, name, quantity, updated_at from app.items order by id desc limit 50`)
  res.json({ items: rows })
})

app.get("/health", async (_req, res) => {
  try {
    await pool.query("select 1")
    res.json({ status: "ok" })
  } catch {
    res.status(503).json({ status: "degraded" })
  }
})

app.get("/", (_req, res) => {
  res.json({
    service: "postgres change data capture",
    write: "POST /items {\"name\":\"widget\"} - a row in the watched schema",
    read: "GET /events - what Debezium delivered afterwards",
    note: "both need Authorization: Bearer $API_TOKEN; /events accepts Debezium's POSTs without one, from the private network",
  })
})

async function main() {
  await pool.query(SCHEMA)
  const server = app.listen(port, "0.0.0.0", () => console.log(`sink listening on ${port}`))
  for (const signal of ["SIGTERM", "SIGINT"] as const) {
    process.on(signal, () => server.close(() => void pool.end().then(() => process.exit(0))))
  }
}

main().catch((error) => {
  console.error("sink failed to start", error)
  process.exit(1)
})
