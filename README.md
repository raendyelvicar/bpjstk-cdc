# BPJS KESEHATAN CDC POC: SQL Server ⇄ PostgreSQL

Active-active replication between SQL Server and PostgreSQL for the BPJS TK
tables (membership, monthly contribution, claim). Both directions run through
Debezium and Kafka:

```
SQL Server ──(Debezium source)──► mssql-bpjsk-cdc.*  ──(ksqlDB loop filter)──► *.to-pg    ──(JDBC sink)──► PostgreSQL
PostgreSQL ──(Debezium source)──► pg-bpjsk-cdc.*     ──(ksqlDB loop filter)──► *.to-mssql ──(JDBC sink)──► SQL Server
```

- **Loop filter** ([sql/ksqldb-01-loop-filter.sql](sql/ksqldb-01-loop-filter.sql)): a change only goes to the
  other side if it started on this side. The origin is the **database login that wrote the row**, not
  any user ID in the data: on every insert and update the triggers stamp the connecting login into a
  `db_login` column (`ORIGINAL_LOGIN()` on SQL Server, `session_user` on PostgreSQL). Each JDBC sink
  connects with its own login (`cdc_sink` by default), so a change whose `db_login` is that node's sink
  login is a replicated row and is dropped. `db_login` itself is never replicated.
- **Last-writer-wins triggers** (`sql/*-03-lww-trigger.sql`): on each node, an update is applied only if
  it has a newer `updated_at`. If the timestamps are equal, the higher user ID wins. A replicated echo
  loses this check, so it writes no new change and the loop stops.
- **Key ranges**: SQL Server IDs go from 1 to 999,999,999. PostgreSQL IDs start at 1,000,000,000.

## Prerequisites

- Docker with Docker Compose v2. Give Docker at least 6–8 GB of RAM, because this stack runs SQL Server,
  Kafka, Connect and ksqlDB.
- `bash`, `curl` and `jq` (on macOS: `brew install jq`).
- Python 3.9+ for the automated tests.
- Optional: `psql` and `sqlcmd`. If you don't have them, `setup-db.sh` uses the copies inside the
  containers.

> **Apple Silicon:** the SQL Server image runs as `linux/amd64`. In Docker Desktop, turn on
> *Use Rosetta for x86/amd64 emulation*. Without it SQL Server is slow or won't start.

## 1. Configure the environment

Settings live in one file per environment, picked with `APP_ENV` (default `local`):

| `APP_ENV` | File | Target |
| --- | --- | --- |
| `local` (default) | `.env.local` | local Docker stack |
| `development` | `.env.development` | `*.kafka.kodehive.id` (Connect, ksqlDB, broker) and the `*.kodehive.id` databases |
| `staging` | `.env.staging` | placeholder values for now |

```bash
cp .env.example .env.local
APP_ENV=development bash scripts/register-connectors.sh --dry-run   # any other environment
```

Set both passwords in `.env.local`:

```dotenv
MSSQL_PASSWORD=YourStr0ng!Passw0rd   # SQL Server `sa` password. It must meet SQL Server's complexity rules
PG_PASSWORD=postgres_password        # `postgres` superuser and `debezium_replication` user
```

The defaults (`localhost`, ports 1433 and 5432, database `bpjsk_poc`, sink logins `cdc_sink`) work with
the local Docker stack, so you don't need to set anything else. All `.env.*` files except `.env.example`
are git-ignored.

### Settings for another environment (for example staging)

Every script, the connector configs and the tests read their settings from `.env.$APP_ENV` through
[scripts/env.sh](scripts/env.sh). [.env.example](.env.example) lists every setting and its default. A
variable set in the shell wins over the file, and `ENV_FILE=<file>` selects another file, so you can keep
one per environment.

- **Connector configs** in `configs/` use `${env:VAR}` placeholders.
  [scripts/render-config.sh](scripts/render-config.sh) fills them in from the env file before they are sent.
  With `CONNECT_SECRETS=inline` (the default), passwords are written into the configs. Anyone with access
  to the Connect REST API can read them, which matches the other connectors on the staging worker. With
  `CONNECT_SECRETS=worker`, passwords stay `${env:...}`, and the Connect worker must have them set.
- **Hosts.** `MSSQL_HOST` / `PG_HOST` are the databases as this machine reaches them. The Connect worker
  uses the same values unless you set `CONNECT_MSSQL_HOST` / `CONNECT_PG_HOST`. When the hosts aren't set,
  the worker uses the compose service names.
- **Logins.** `MSSQL_ADMIN_USER` / `PG_ADMIN_USER` run `setup-db.sh` and the tests. `MSSQL_USER` / `PG_USER`
  are the Debezium sources; the PostgreSQL one needs `REPLICATION`. `MSSQL_SINK_USER` / `PG_SINK_USER` are
  the sinks. The scripts refuse a sink login that is also the admin login (or, for PostgreSQL, the source
  login), because the loop filter would silently drop that login's writes.
- **Preview** what would be sent, with passwords masked: `bash scripts/register-connectors.sh --dry-run`.
- **The tests** refuse to run against databases that aren't local unless you pass `--allow-remote`,
  because they insert and delete `CDCT…` rows.

## 2. Start the stack

```bash
docker compose --env-file .env.local up -d --build
```

The first run builds the Kafka Connect image from [Dockerfile.connect](Dockerfile.connect), which adds the
Debezium JDBC sink to the Debezium Connect image. This takes a few minutes.

| Service         | URL / port              | Purpose                              |
|-----------------|-------------------------|--------------------------------------|
| `sqlserver`     | `localhost:1433`        | SQL Server 2019 (Agent enabled, CDC) |
| `postgres`      | `localhost:5432`        | PostgreSQL 18 (`wal_level=logical`)  |
| `kafka`         | `localhost:9092`        | Kafka broker (KRaft, single node)    |
| `kafka-connect` | http://localhost:8083   | Debezium Connect REST API            |
| `ksqldb-server` | http://localhost:8088   | ksqlDB (loop filter)                 |
| `kafka-ui`      | http://localhost:8080   | Topics, connectors and ksqlDB UI     |
| `prometheus`    | http://localhost:9090   | Not wired up yet                     |
| `grafana`       | http://localhost:3000   | Not wired up yet (admin/admin)       |

Before you continue, wait until SQL Server and Connect are up:

```bash
docker compose ps
curl -s localhost:8083/ | jq .     # Connect returns its version once it is ready
```

## 3. Create the schemas, CDC and triggers

```bash
bash scripts/setup-db.sh
```

This script runs these files in order:

- SQL Server: [mssql-01-schema.sql](sql/mssql-01-schema.sql) creates the `bpjsk_poc` database and tables,
  [mssql-02-enable-cdc.sql](sql/mssql-02-enable-cdc.sql) enables CDC, and
  [mssql-03-lww-trigger.sql](sql/mssql-03-lww-trigger.sql) adds the LWW triggers (they also stamp
  `db_login`), and [mssql-05-sink-login.sql](sql/mssql-05-sink-login.sql) creates the sink login.
- PostgreSQL: [postgres-01-schema.sql](sql/postgres-01-schema.sql) creates the tables,
  [postgres-02-replication.sql](sql/postgres-02-replication.sql) creates the `debezium_replication` role,
  the publication and the sequence offsets, [postgres-03-lww-trigger.sql](sql/postgres-03-lww-trigger.sql)
  adds the LWW and `db_login` triggers, and [postgres-05-sink-role.sql](sql/postgres-05-sink-role.sql)
  creates the sink role.

You can run all of these scripts again safely.

**Optional, but recommended for the full test suite:** turn off the foreign keys on both nodes.
Each table replicates through its own topic, so a child row can arrive before its parent:

```bash
set -a; source .env.local; set +a
docker exec -i postgres psql -U postgres -d bpjsk_poc < sql/postgres-04-disable-fk.sql
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_PASSWORD" -C -I -b \
  -i /dev/stdin < sql/mssql-04-disable-fk.sql
```

Each file has a RESTORE section at the bottom that puts the foreign keys back.

## 4. Register the connectors and the ksqlDB filter

```bash
bash scripts/register-connectors.sh
```

The script sends each config with `PUT /connectors/<name>/config`. This creates a connector that doesn't
exist yet and updates one that does, in place. Consumer offsets are kept, and a paused connector stays
paused. To update only some connectors, pass their files, for example
`bash scripts/register-connectors.sh configs/sink/*.json`. Without arguments, it runs these steps in order:

1. Registers the two Debezium sources. They create the CDC topics.
2. Runs [scripts/register-ksqldb.sh](scripts/register-ksqldb.sh), which applies the loop filter and writes
   the `*.to-pg` and `*.to-mssql` topics.
3. Registers the two JDBC sinks, which read those topics.

After you change the filter, apply it with `bash scripts/register-ksqldb.sh --replace`. This command
upgrades the running queries in place (`CREATE OR REPLACE`), so they keep their offsets and don't re-read
the CDC topics. Don't drop and re-create the streams. Re-created queries replay all history, and with the
foreign keys on, a replayed child row can fail the sink.

Check that everything is running:

```bash
for c in bpjsk-mssql-source bpjsk-postgres-source bpjsk-postgres-sink bpjsk-mssql-sink; do
  curl -s localhost:8083/connectors/$c/status | jq -c '{name, state: .connector.state, tasks: [.tasks[].state]}'
done
```

All four connectors should report `RUNNING`. You can also check them in Kafka UI at http://localhost:8080.

## 5. Test the replication

### Automated suite

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install psycopg2-binary pymssql requests kafka-python

python tests/test_cdc.py               # all tests (TC-01 … TC-17), then clean up the test rows
python tests/test_cdc.py TC-01 TC-05   # selected tests only
python tests/test_cdc.py --keep        # keep the test rows for inspection
```

The suite tests both replication directions, the loop filter, LWW tie-breaks, stale and dropped updates,
hard and soft deletes, DLQ routing and NIK changes. Test rows use BPJS numbers that start with `CDCT` and
NIKs that start with `99`.

**Loopback evidence (TC-17).** TC-17 checks that no change is sent back when both sinks use the same login.
It reads both sinks' logins from Kafka Connect and the running ksqlDB filters, and it is skipped when the
sinks use different logins. It then writes one row from SQL Server and then from PostgreSQL, and records
every event for that row:

- in the four topics (the CDC topics and the filtered `.to-*` topics),
- in SQL Server's own CDC change table,
- in the final rows on both sides.

It counts everything twice, 20 seconds apart, because a loop would keep adding events. The report goes to
`tests/evidence/TC-17-no-loopback-<UTC time>.md`. Run it alone with
`python tests/test_cdc.py TC-17`. Add `--allow-remote` for development.

### Manual DML

Run [tests/manual/mssql-test-dml.sql](tests/manual/mssql-test-dml.sql) or
[tests/manual/pg-test-dml.sql](tests/manual/pg-test-dml.sql) one step at a time. After each step, check
the other database.

## Operating through the APIs (Postman)

Everything on the Kafka side can also be done through the Kafka Connect and ksqlDB REST APIs, with the
**BPJS CDC Ops** Postman collection in [postman/collections/bpjsk-cdc-ops](postman/collections/bpjsk-cdc-ops)
and its `local`, `development` and `staging` environments in [postman/environments](postman/environments).
The database step (`setup-db.sh`) has no API: run it once per environment first.

| Folder | What it does |
|---|---|
| `1 Check` | Read only: connector states, each connector's target host, port and login, sink logins and `dropLogin`, ksqlDB health, the six filter queries |
| `2 Pause` | Pause the four connectors (sinks first) |
| `3 Update connectors` | `PUT` the four configs: create, or update in place with offsets kept |
| `4 Loop filter` | Create the raw streams if missing, then upgrade each filter in place, waiting until ksqlDB has executed it |
| `5 Resume` | Resume the four connectors (sources first) |
| `6 Restart failed tasks` | Restart only failed connector instances and tasks |

Run folders with the Postman CLI. [scripts/postman-run.sh](scripts/postman-run.sh) fills every value,
passwords included, from `.env.$APP_ENV`, the same settings the shell scripts use:

```bash
APP_ENV=development bash scripts/postman-run.sh "1 Check"
APP_ENV=development bash scripts/postman-run.sh "2 Pause" "3 Update connectors"
```

At least one folder is required. Extra CLI flags go after `--`. The committed environments have no
passwords. In the Postman app, fill the secret variables in your own copy of the environment.

The collection is **generated**: request bodies come from `configs/*.json` and the statements from
`sql/ksqldb-01-loop-filter.sql`. After changing either one, run `python3 scripts/gen-postman.py`. Don't
edit the files under `postman/collections/bpjsk-cdc-ops/` by hand.

## Rules for writing data

Every writer, whether an app or a person, must follow these rules. Otherwise changes are dropped without
any error:

- Never connect as a sink login (`MSSQL_SINK_USER` / `PG_SINK_USER`). The loop filter treats every change
  made by those logins as a replicated echo, so it never sends them to the other node. Apps and people
  can use any other login. `created_by` / `updated_by` can be any user ID, because they are only used to
  break LWW ties.
- Every `UPDATE`, including soft deletes, must set a **newer** `updated_at` and set `updated_by`.
  Otherwise the LWW trigger skips the update.
- Use UTC timestamps.

## Upgrading a stack from the user-ID loop filter

On a local stack, the easiest way is to start over with `docker compose down -v`. To keep the data (for
example on staging), set `APP_ENV` and that environment's `.env.*` file (see
[Settings for another environment](#settings-for-another-environment-for-example-staging)), check it with
`bash scripts/register-connectors.sh --dry-run`, and then follow these steps in order. `$CONNECT_URL` below
is the Connect endpoint (add `-u user:password` if it uses basic auth).

1. **Stop app writes and let replication drain.** Recreating a SQL Server capture instance drops its
   change table, so any change Debezium hasn't read yet is lost. Wait until the consumer lag of the
   sources, the ksqlDB queries and the sinks is 0.
2. **Pause all four connectors:**
   `for c in bpjsk-mssql-source bpjsk-postgres-source bpjsk-postgres-sink bpjsk-mssql-sink; do curl -s -X PUT $CONNECT_URL/connectors/$c/pause; done`
3. **Run `bash scripts/setup-db.sh`.** It adds `db_login` on both nodes, updates the triggers, creates the
   sink logins and recreates the SQL Server capture instances (it prints one "Recreating…" line per table).
4. **Update the four connectors in place:**
   `bash scripts/register-connectors.sh configs/source/*.json configs/sink/*.json`. Their offsets are kept,
   and they stay paused.
5. **Run `bash scripts/register-ksqldb.sh --replace`.** It upgrades the six filter queries in place and
   waits for each one. Afterwards ksqlDB rebalances its stream threads for one to a few minutes, which
   pauses filtering but loses nothing. Wait until `$KSQLDB_URL/healthcheck` reports healthy and the
   `_confluent-ksql-*` consumer groups have no lag.
6. **Resume the connectors**, sources first, then the sinks. Then resume app writes.
7. **Check.** All four connectors are `RUNNING`, new rows get `db_login` set to the writer's login on the
   origin and to the sink login on the replica, and the `*.to-pg` / `*.to-mssql` topics carry no events
   whose `db_login` is a sink login.

Changes written before the upgrade have no `db_login`, so the filter lets them through. If they are
echoes, the LWW triggers stop them as before.

To roll back, apply the previous filter with `SQL_FILE=<old file> bash scripts/register-ksqldb.sh --replace`
and register the previous connector configs. The `db_login` column and the triggers can stay.

## Troubleshooting

- **SQL Server changes don't replicate.** CDC needs SQL Server Agent (`MSSQL_AGENT_ENABLED=true` in the
  compose file). `mssql-02-enable-cdc.sql` prints a warning if Agent isn't running.
- **A connector shows `FAILED`.** Check `docker logs kafka-connect` or
  `curl localhost:8083/connectors/<name>/status`. Restart it with
  `curl -X POST "localhost:8083/connectors/<name>/restart?includeTasks=true"`.
- **Records go missing.** Sink errors go to the `dlq.bpjsk-replication` topic. The Connect error context
  is in the message headers.
- **You changed a connector config.** Run `register-connectors.sh` (or pass just that file) again. It
  updates the connector in place. The ksqlDB step is idempotent.
- **You changed a ksqlDB filter.** Run `bash scripts/register-ksqldb.sh --replace`. Don't `DROP` and
  re-create the streams: new queries replay all history (see step 4).

## Stop / reset

```bash
docker compose down        # stop and keep the data
docker compose down -v     # stop and delete all data (Kafka, SQL Server, PostgreSQL volumes)
```

After `down -v`, repeat steps 2–4.

## Project layout

```
configs/source/      Debezium source connector configs (SQL Server, PostgreSQL)
configs/sink/        Debezium JDBC sink configs (column renames, routing, DLQ)
sql/                 Schema, CDC/replication, LWW triggers, FK toggles, ksqlDB loop filter
scripts/             env.sh (settings), render-config.sh, setup-db.sh, register-connectors.sh, register-ksqldb.sh,
                     gen-postman.py, postman-run.sh
postman/             Postman collection (generated) and environments
tests/test_cdc.py    Automated end-to-end test suite
tests/manual/        Step-by-step manual DML scripts
Dockerfile.connect   Debezium Connect image with the JDBC sink and scripting plugins
```
