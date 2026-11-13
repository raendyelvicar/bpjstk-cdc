# BPJS TK CDC POC: SQL Server ⇄ PostgreSQL

Active-active replication between SQL Server and PostgreSQL for the BPJS TK
tables (membership, monthly contribution, claim). Both directions run through
Debezium and Kafka:

```
SQL Server ──(Debezium source)──► mssql-bpjsk-cdc.*  ──(ksqlDB loop filter)──► *.to-pg    ──(JDBC sink)──► PostgreSQL
PostgreSQL ──(Debezium source)──► pg-bpjsk-cdc.*     ──(ksqlDB loop filter)──► *.to-mssql ──(JDBC sink)──► SQL Server
```

- **Loop filter** ([sql/ksqldb-01-loop-filter.sql](sql/ksqldb-01-loop-filter.sql)): a change only goes to the
  other side if it started on this side. The origin comes from `updated_by` / `created_by`:
  SQL Server users are `< 1000000`, PostgreSQL users are `>= 1000000`.
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

```bash
cp .env.example .env
```

Set both passwords in `.env`:

```dotenv
MSSQL_PASSWORD=YourStr0ng!Passw0rd   # SQL Server `sa` password. It must meet SQL Server's complexity rules
PG_PASSWORD=postgres_password        # `postgres` superuser and `debezium_replication` user
```

The defaults (`localhost`, ports 1433 and 5432, database `bpjsk_poc`) work with the local Docker stack, so
you don't need to set anything else. `.env` is git-ignored.

## 2. Start the stack

```bash
docker compose up -d --build
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
  [mssql-03-lww-trigger.sql](sql/mssql-03-lww-trigger.sql) adds the LWW triggers.
- PostgreSQL: [postgres-01-schema.sql](sql/postgres-01-schema.sql) creates the tables,
  [postgres-02-replication.sql](sql/postgres-02-replication.sql) creates the `debezium_replication` role,
  the publication and the sequence offsets, and [postgres-03-lww-trigger.sql](sql/postgres-03-lww-trigger.sql)
  adds the LWW triggers.

You can run all of these scripts again safely.

**Optional, but recommended for the full test suite:** turn off the foreign keys on both nodes.
Each table replicates through its own topic, so a child row can arrive before its parent:

```bash
set -a; source .env; set +a
docker exec -i postgres psql -U postgres -d bpjsk_poc < sql/postgres-04-disable-fk.sql
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_PASSWORD" -C -I -b \
  -i /dev/stdin < sql/mssql-04-disable-fk.sql
```

Each file has a RESTORE section at the bottom that puts the foreign keys back.

## 4. Register the connectors and the ksqlDB filter

```bash
bash scripts/register-connectors.sh
```

This script runs the steps in this order:

1. Registers the two Debezium sources. They create the CDC topics.
2. Runs [scripts/register-ksqldb.sh](scripts/register-ksqldb.sh), which applies the loop filter and writes
   the `*.to-pg` and `*.to-mssql` topics.
3. Registers the two JDBC sinks, which read those topics.

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

python tests/test_cdc.py               # all tests (TC-01 … TC-14), then clean up the test rows
python tests/test_cdc.py TC-01 TC-05   # selected tests only
python tests/test_cdc.py --keep        # keep the test rows for inspection
```

The suite tests both replication directions, the loop filter, LWW tie-breaks, stale and dropped updates,
hard and soft deletes, DLQ routing and NIK changes. Test rows use BPJS numbers that start with `CDCT` and
NIKs that start with `99`.

### Manual DML

Run [tests/manual/mssql-test-dml.sql](tests/manual/mssql-test-dml.sql) or
[tests/manual/pg-test-dml.sql](tests/manual/pg-test-dml.sql) one step at a time. After each step, check
the other database.

## Rules for writing data

Every writer, whether an app or a person, must follow these rules. Otherwise changes are dropped without
any error:

- Set `created_by` / `updated_by` from the correct ID range: SQL Server users `< 1000000`, PostgreSQL
  users `>= 1000000`. The loop filter uses these to decide where a change came from.
- Every `UPDATE`, including soft deletes, must set a **newer** `updated_at` and set `updated_by`.
  Otherwise the LWW trigger skips the update.
- Use UTC timestamps.

## Troubleshooting

- **SQL Server changes don't replicate.** CDC needs SQL Server Agent (`MSSQL_AGENT_ENABLED=true` in the
  compose file). `mssql-02-enable-cdc.sql` prints a warning if Agent isn't running.
- **A connector shows `FAILED`.** Check `docker logs kafka-connect` or
  `curl localhost:8083/connectors/<name>/status`. Restart it with
  `curl -X POST "localhost:8083/connectors/<name>/restart?includeTasks=true"`.
- **Records go missing.** Sink errors go to the `dlq.bpjsk-replication` topic. The Connect error context
  is in the message headers.
- **You need to register a connector again.** Delete it first with
  `curl -X DELETE localhost:8083/connectors/<name>`, then run `register-connectors.sh` again. The ksqlDB
  step is idempotent.
- **You changed a ksqlDB filter.** `TERMINATE` the query, `DROP STREAM` the output stream (not the
  topic), then run `bash scripts/register-ksqldb.sh` again.

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
scripts/             setup-db.sh, register-connectors.sh, register-ksqldb.sh
tests/test_cdc.py    Automated end-to-end test suite
tests/manual/        Step-by-step manual DML scripts
Dockerfile.connect   Debezium Connect image with the JDBC sink and scripting plugins
```
