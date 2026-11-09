#!/usr/bin/env python3
"""CDC active-active test suite: SQL Server <-> PostgreSQL (reference schema)."""
import datetime as dt
import os
import sys
import time
import uuid

import psycopg2
import pymssql
import requests

try:
    from kafka import KafkaConsumer, KafkaProducer
except ImportError:
    KafkaConsumer = KafkaProducer = None


def load_env(path=".env"):
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))


load_env()

MSSQL_PASSWORD = os.environ["MSSQL_PASSWORD"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq.orders-replication")
MSSQL_TOPIC = "mssql-cdc.ProductionDB.dbo.Orders"
MSSQL_NODE, PG_NODE = "MSSQL_NODE_01", "PG_NODE_01"
TIMEOUT = 30  # seconds to wait for replication
CONVERGE_HOLD = 10  # seconds both nodes must keep matching after converging

class MSCursor:
    """Keeps the pyodbc-style calls (? placeholders, chained fetchone) working on pymssql."""
    def __init__(self, c):
        self.c = c

    def execute(self, sql, *args):
        self.c.execute(sql.replace("?", "%s"), args)
        return self

    def fetchone(self):
        return self.c.fetchone()


class MSConn:
    def __init__(self, conn):
        self.conn = conn

    def cursor(self):
        return MSCursor(self.conn.cursor())


ms = MSConn(pymssql.connect(
    server="localhost", port=1433, user="sa", password=MSSQL_PASSWORD,
    database="ProductionDB", autocommit=True,
))

pg = psycopg2.connect(host="localhost", port=5432, dbname="ProductionDB",
                      user="postgres", password=PG_PASSWORD)
pg.autocommit = True


class Skip(Exception):
    pass


# ---------- helpers ----------
def utc_now():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def ms_ts(t):  # SQL Server DATETIME2 takes a naive UTC value
    return t.replace(tzinfo=None)


def ms_insert(oid, status="NEW", version=1, by=MSSQL_NODE, ts=None):
    ts = ts or utc_now()
    ms.cursor().execute(
        "INSERT INTO dbo.Orders (OrderID, CustomerID, OrderStatus, OrderTotal, last_updated_at, last_updated_by, version) "
        "VALUES (?, 'CUST-1', ?, 10.00, ?, ?, ?)", oid, status, ms_ts(ts), by, version)


def ms_update(oid, status, version, by=MSSQL_NODE, ts=None):
    ts = ts or utc_now()
    ms.cursor().execute(
        "UPDATE dbo.Orders SET OrderStatus=?, version=?, last_updated_by=?, last_updated_at=? WHERE OrderID=?",
        status, version, by, ms_ts(ts), oid)


def pg_insert(oid, status="NEW", version=1, by=PG_NODE, ts=None):
    ts = ts or utc_now()
    with pg.cursor() as c:
        c.execute("INSERT INTO public.orders (orderid, customerid, orderstatus, ordertotal, last_updated_at, last_updated_by, version) "
                  "VALUES (%s, 'CUST-1', %s, 10.00, %s, %s, %s)", (oid, status, ts, by, version))


def pg_update(oid, status, version, by=PG_NODE, ts=None):
    ts = ts or utc_now()
    with pg.cursor() as c:
        c.execute("UPDATE public.orders SET orderstatus=%s, version=%s, last_updated_by=%s, last_updated_at=%s WHERE orderid=%s",
                  (status, version, by, ts, oid))
        return c.rowcount


def ms_get(oid):
    r = ms.cursor().execute(
        "SELECT OrderStatus, version, last_updated_by FROM dbo.Orders WHERE OrderID=?", oid).fetchone()
    return None if r is None else {"status": r[0], "version": int(r[1]), "by": r[2]}


def pg_get(oid):
    with pg.cursor() as c:
        c.execute("SELECT orderstatus, version, last_updated_by FROM public.orders WHERE orderid=%s", (oid,))
        r = c.fetchone()
    return None if r is None else {"status": r[0], "version": int(r[1]), "by": r[2]}


def wait_for(fn, what, timeout=TIMEOUT):
    end = time.time() + timeout
    while time.time() < end:
        v = fn()
        if v:
            return v
        time.sleep(1)
    raise AssertionError(f"timeout waiting for: {what}")


def converged(oid, status=None, version=None):
    a, b = ms_get(oid), pg_get(oid)
    if not a or not b:
        return False
    if (a["status"], a["version"]) != (b["status"], b["version"]):
        return False
    return (status is None or a["status"] == status) and (version is None or a["version"] == version)


def wait_converged(oid, status, version, what, hold=CONVERGE_HOLD):
    """Wait for both nodes to match, then require that they keep matching.

    Without LWW the nodes swap values, so they can match briefly mid-swap.
    The hold outlasts in-flight replication, so that doesn't count as a pass.
    """
    wait_for(lambda: converged(oid, status, version), what)
    end = time.time() + hold
    while time.time() < end:
        assert converged(oid, status, version), (
            f"{what}: nodes diverged during the {hold}s hold: "
            f"mssql={ms_get(oid)} pg={pg_get(oid)}")
        time.sleep(1)


def count_events(topic, oid):
    if KafkaConsumer is None:
        raise Skip("kafka-python not installed")
    cons = KafkaConsumer(topic, bootstrap_servers=KAFKA, auto_offset_reset="earliest",
                         enable_auto_commit=False, group_id=None, consumer_timeout_ms=8000)
    n = sum(1 for m in cons if oid.lower() in (m.value or b"").decode("utf-8", "ignore").lower())
    cons.close()
    return n


# ---------- test cases ----------
def tc01():
    oid = str(uuid.uuid4())
    ms_insert(oid, "NEW")
    wait_for(lambda: pg_get(oid), "insert reaches PostgreSQL")
    ms_update(oid, "PAID", 2)
    wait_for(lambda: (pg_get(oid) or {}).get("status") == "PAID", "update reaches PostgreSQL")


def tc02():
    oid = str(uuid.uuid4())
    pg_insert(oid, "NEW")
    wait_for(lambda: ms_get(oid), "insert reaches SQL Server")
    pg_update(oid, "PAID", 2)
    wait_for(lambda: (ms_get(oid) or {}).get("status") == "PAID", "update reaches SQL Server")


def tc03():
    oid = str(uuid.uuid4())
    ms_insert(oid, "LOOP")
    wait_for(lambda: pg_get(oid), "row reaches PostgreSQL")
    time.sleep(10)  # give a looping event time to come back
    n = count_events(MSSQL_TOPIC, oid)
    assert n == 1, f"{n} events for this order in {MSSQL_TOPIC}; expected 1 (echo came back)"


def tc04():
    oid = str(uuid.uuid4())
    ms_insert(oid, "BASE")
    wait_for(lambda: pg_get(oid), "baseline replicated")
    ms_update(oid, "WINNER_MSSQL", 3)
    pg_update(oid, "LOSER_PG", 2)
    wait_converged(oid, "WINNER_MSSQL", 3, "both nodes at version 3")


def tc05():
    oid = str(uuid.uuid4())
    ms_insert(oid, "BASE")
    wait_for(lambda: pg_get(oid), "baseline replicated")
    t = utc_now()
    ms_update(oid, "OLDER_MSSQL", 2, ts=t - dt.timedelta(seconds=5))
    pg_update(oid, "NEWER_PG", 2, ts=t)
    wait_converged(oid, "NEWER_PG", 2, "newer timestamp wins on both")


def tc06():
    oid = str(uuid.uuid4())
    ms_insert(oid, "BASE")
    wait_for(lambda: pg_get(oid), "baseline replicated")
    t = utc_now()
    ms_update(oid, "FROM_MSSQL", 2, by=MSSQL_NODE, ts=t)
    pg_update(oid, "FROM_PG", 2, by=PG_NODE, ts=t)
    winner = "FROM_PG" if PG_NODE > MSSQL_NODE else "FROM_MSSQL"
    wait_converged(oid, winner, 2, f"higher node ID ({winner}) wins on both")


def tc07():
    oid = str(uuid.uuid4())
    pg_insert(oid, "LATEST", version=5)
    wait_for(lambda: ms_get(oid), "version 5 replicated")
    changed = pg_update(oid, "STALE", 3)
    assert changed == 0, "PostgreSQL trigger accepted a stale write (version 3 over 5)"
    assert pg_get(oid)["version"] == 5, "version regressed"


def tc08():
    a, b = str(uuid.uuid4()), str(uuid.uuid4())
    ms_insert(a)
    wait_for(lambda: pg_get(a), "row reaches PostgreSQL")
    ms.cursor().execute("DELETE FROM dbo.Orders WHERE OrderID=?", a)
    wait_for(lambda: pg_get(a) is None, "SQL Server delete reaches PostgreSQL")
    pg_insert(b)
    wait_for(lambda: ms_get(b), "row reaches SQL Server")
    with pg.cursor() as c:
        c.execute("DELETE FROM public.orders WHERE orderid=%s", (b,))
    wait_for(lambda: ms_get(b) is None, "PostgreSQL delete reaches SQL Server")


def tc09():
    if KafkaProducer is None:
        raise Skip("kafka-python not installed")
    p = KafkaProducer(bootstrap_servers=KAFKA)
    p.send(MSSQL_TOPIC, key=b"bad", value=b"this-is-not-json")
    p.flush()
    time.sleep(10)
    st = requests.get(f"{CONNECT_URL}/connectors/postgres-sink-orders/status").json()
    states = [st["connector"]["state"]] + [t["state"] for t in st["tasks"]]
    assert all(s == "RUNNING" for s in states), f"sink not RUNNING: {states}"
    cons = KafkaConsumer(DLQ_TOPIC, bootstrap_servers=KAFKA, auto_offset_reset="earliest",
                         enable_auto_commit=False, group_id=None, consumer_timeout_ms=8000)
    n = sum(1 for _ in cons)
    cons.close()
    assert n >= 1, f"no message in {DLQ_TOPIC}; check the errors.* settings on the sink"


# ---------- customers, products, transactions ----------
PG_ID_MIN = 1000000001  # SQL Server IDs stay below this; PostgreSQL IDs start here

TABLES = {
    "customers": {
        "ms": "dbo.Customers", "pg": "public.customers", "key": "customer_id", "value": "first_name",
        "extra": lambda: {"last_name": "Test", "email": f"{uuid.uuid4()}@example.test"},
    },
    "products": {
        "ms": "dbo.Products", "pg": "public.products", "key": "product_id", "value": "product_name",
        "extra": lambda: {"price": 19.99, "stock_quantity": 10},
    },
    "transactions": {
        "ms": "dbo.Transactions", "pg": "public.transactions", "key": "transaction_id", "value": "transaction_status",
        "extra": lambda: {"customer_id": 1, "product_id": 1, "quantity": 2, "total_price": 39.98},
    },
}


def row_insert(side, t, value, version=1, ts=None):
    """Insert with a database-generated key and return that key."""
    spec = TABLES[t]
    ts = ts or utc_now()
    data = {spec["value"]: value, **spec["extra"](), "version": version,
            "last_updated_at": ms_ts(ts) if side == "ms" else ts,
            "last_updated_by": MSSQL_NODE if side == "ms" else PG_NODE}
    cols, vals = ", ".join(data), list(data.values())
    if side == "ms":
        # A table with a trigger only allows OUTPUT ... INTO; SCOPE_IDENTITY() returns NULL here.
        return ms.cursor().execute(
            f"SET NOCOUNT ON; DECLARE @o TABLE (id INT); "
            f"INSERT INTO {spec['ms']} ({cols}) OUTPUT inserted.{spec['key']} INTO @o "
            f"VALUES ({', '.join('?' * len(vals))}); SELECT id FROM @o;", *vals).fetchone()[0]
    with pg.cursor() as c:
        c.execute(f"INSERT INTO {spec['pg']} ({cols}) VALUES ({', '.join(['%s'] * len(vals))}) "
                  f"RETURNING {spec['key']}", vals)
        return c.fetchone()[0]


def row_update(side, t, rid, value, version, ts=None):
    spec = TABLES[t]
    ts = ts or utc_now()
    if side == "ms":
        ms.cursor().execute(
            f"UPDATE {spec['ms']} SET {spec['value']}=?, version=?, last_updated_at=?, last_updated_by=? "
            f"WHERE {spec['key']}=?", value, version, ms_ts(ts), MSSQL_NODE, rid)
    else:
        with pg.cursor() as c:
            c.execute(f"UPDATE {spec['pg']} SET {spec['value']}=%s, version=%s, last_updated_at=%s, last_updated_by=%s "
                      f"WHERE {spec['key']}=%s", (value, version, ts, PG_NODE, rid))


def row_get(side, t, rid):
    spec = TABLES[t]
    sql = f"SELECT {spec['value']}, version FROM {spec[side]} WHERE {spec['key']}="
    if side == "ms":
        r = ms.cursor().execute(sql + "?", rid).fetchone()
    else:
        with pg.cursor() as c:
            c.execute(sql + "%s", (rid,))
            r = c.fetchone()
    return None if r is None else (r[0], int(r[1]))


def row_delete(side, t, rid):
    spec = TABLES[t]
    if side == "ms":
        ms.cursor().execute(f"DELETE FROM {spec['ms']} WHERE {spec['key']}=?", rid)
    else:
        with pg.cursor() as c:
            c.execute(f"DELETE FROM {spec['pg']} WHERE {spec['key']}=%s", (rid,))


def row_stays_at(t, rid, value, version, what, hold=CONVERGE_HOLD):
    want = (value, version)
    wait_for(lambda: row_get("ms", t, rid) == want and row_get("pg", t, rid) == want, what)
    end = time.time() + hold
    while time.time() < end:
        a, b = row_get("ms", t, rid), row_get("pg", t, rid)
        assert a == want and b == want, f"{what}: nodes diverged during the {hold}s hold: mssql={a} pg={b}"
        time.sleep(1)


def tc10():
    for t in TABLES:
        a = row_insert("ms", t, "FROM_MS")
        assert a < PG_ID_MIN, f"{t}: SQL Server generated {a}, inside PostgreSQL's range"
        wait_for(lambda: row_get("pg", t, a), f"{t}: insert reaches PostgreSQL")
        row_update("ms", t, a, "MS_V2", 2)
        wait_for(lambda: row_get("pg", t, a) == ("MS_V2", 2), f"{t}: update reaches PostgreSQL")

        b = row_insert("pg", t, "FROM_PG")
        assert b >= PG_ID_MIN, f"{t}: PostgreSQL generated {b}, inside SQL Server's range"
        wait_for(lambda: row_get("ms", t, b), f"{t}: insert reaches SQL Server")
        row_update("pg", t, b, "PG_V2", 2)
        wait_for(lambda: row_get("ms", t, b) == ("PG_V2", 2), f"{t}: update reaches SQL Server")


def tc11():
    for t in TABLES:
        rid = row_insert("ms", t, "BASE")
        wait_for(lambda: row_get("pg", t, rid), f"{t}: baseline replicated")
        row_update("ms", t, rid, "WINNER_MS", 3)
        row_update("pg", t, rid, "LOSER_PG", 2)
        row_stays_at(t, rid, "WINNER_MS", 3, f"{t}: higher version wins")


def tc12():
    for t in TABLES:
        rid = row_insert("ms", t, "BASE")
        wait_for(lambda: row_get("pg", t, rid), f"{t}: baseline replicated")
        now = utc_now()
        row_update("ms", t, rid, "OLDER_MS", 2, ts=now - dt.timedelta(seconds=5))
        row_update("pg", t, rid, "NEWER_PG", 2, ts=now)
        row_stays_at(t, rid, "NEWER_PG", 2, f"{t}: newer timestamp wins")


def tc13():
    for t in TABLES:
        rid = row_insert("ms", t, "BASE")
        wait_for(lambda: row_get("pg", t, rid), f"{t}: baseline replicated")
        now = utc_now()
        row_update("ms", t, rid, "FROM_MS", 2, ts=now)
        row_update("pg", t, rid, "FROM_PG", 2, ts=now)
        row_stays_at(t, rid, "FROM_PG", 2, f"{t}: higher node ID wins")


def tc14():
    for t in TABLES:
        for side in ("ms", "pg"):
            rid = row_insert(side, t, "LATEST", version=5)
            other = "pg" if side == "ms" else "ms"
            wait_for(lambda: row_get(other, t, rid), f"{t}: row reaches {other}")
            row_update(side, t, rid, "STALE", 3)
            assert row_get(side, t, rid) == ("LATEST", 5), f"{t}: {side} trigger accepted a stale write"


def tc15():
    for t in TABLES:
        a = row_insert("ms", t, "DEL_MS")
        wait_for(lambda: row_get("pg", t, a), f"{t}: row reaches PostgreSQL")
        row_delete("ms", t, a)
        wait_for(lambda: row_get("pg", t, a) is None, f"{t}: SQL Server delete reaches PostgreSQL")
        b = row_insert("pg", t, "DEL_PG")
        wait_for(lambda: row_get("ms", t, b), f"{t}: row reaches SQL Server")
        row_delete("pg", t, b)
        wait_for(lambda: row_get("ms", t, b) is None, f"{t}: PostgreSQL delete reaches SQL Server")


def tc16():
    # Foreign keys are dropped so a child can arrive before its parent. This child's
    # parents never exist; replication must carry it without stopping either sink.
    with pg.cursor() as c:
        c.execute("INSERT INTO public.transactions (customer_id, product_id, quantity, total_price, last_updated_by) "
                  "VALUES (2147483647, 2147483647, 1, 1.00, %s) RETURNING transaction_id", (PG_NODE,))
        rid = c.fetchone()[0]
    wait_for(lambda: row_get("ms", "transactions", rid), "orphan transaction reaches SQL Server")
    for name in ("postgres-sink-orders", "mssql-sink-orders"):
        st = requests.get(f"{CONNECT_URL}/connectors/{name}/status").json()
        states = [st["connector"]["state"]] + [x["state"] for x in st["tasks"]]
        assert all(s == "RUNNING" for s in states), f"{name} not RUNNING: {states}"


def tc17():
    # A unique index on email made SQL Server log updates as delete + insert. The
    # delete replicated both ways, the filter dropped the insert, and the row was lost.
    def email_of(side, rid):
        if side == "ms":
            r = ms.cursor().execute("SELECT email FROM dbo.Customers WHERE customer_id=?", rid).fetchone()
        else:
            with pg.cursor() as c:
                c.execute("SELECT email FROM public.customers WHERE customer_id=%s", (rid,))
                r = c.fetchone()
        return r[0] if r else None

    rid = row_insert("ms", "customers", "BASE")
    wait_for(lambda: row_get("pg", "customers", rid), "baseline replicated")

    row_update("ms", "customers", rid, "KEEP_EMAIL", 2)
    row_stays_at("customers", rid, "KEEP_EMAIL", 2, "update with same email survives")

    a = f"{uuid.uuid4()}@example.test"
    ms.cursor().execute("UPDATE dbo.Customers SET email=?, first_name='MS_EMAIL', version=3, last_updated_at=?, "
                        "last_updated_by=? WHERE customer_id=?", a, ms_ts(utc_now()), MSSQL_NODE, rid)
    row_stays_at("customers", rid, "MS_EMAIL", 3, "SQL Server email change survives")
    assert email_of("pg", rid) == a, "new email did not reach PostgreSQL"

    b = f"{uuid.uuid4()}@example.test"
    with pg.cursor() as c:
        c.execute("UPDATE public.customers SET email=%s, first_name='PG_EMAIL', version=4, last_updated_at=%s, "
                  "last_updated_by=%s WHERE customer_id=%s", (b, utc_now(), PG_NODE, rid))
    row_stays_at("customers", rid, "PG_EMAIL", 4, "PostgreSQL email change survives")
    assert email_of("ms", rid) == b, "new email did not reach SQL Server"


TESTS = [
    ("TC-01 SQL Server -> PostgreSQL", tc01),
    ("TC-02 PostgreSQL -> SQL Server", tc02),
    ("TC-03 Loop prevention", tc03),
    ("TC-04 LWW higher version", tc04),
    ("TC-05 LWW timestamp", tc05),
    ("TC-06 LWW node ID", tc06),
    ("TC-07 Stale event suppressed", tc07),
    ("TC-08 Hard delete both ways", tc08),
    ("TC-09 DLQ routing", tc09),
    ("TC-10 New tables both ways", tc10),
    ("TC-11 New tables LWW version", tc11),
    ("TC-12 New tables LWW timestamp", tc12),
    ("TC-13 New tables LWW node ID", tc13),
    ("TC-14 New tables stale write", tc14),
    ("TC-15 New tables hard delete", tc15),
    ("TC-16 Child without parent", tc16),
    ("TC-17 Customer email updates", tc17),
]

if __name__ == "__main__":
    only = set(sys.argv[1:])  # e.g. python tests/test_cdc.py TC-01 TC-02
    results = {}
    for name, fn in TESTS:
        if only and name.split()[0] not in only:
            continue
        try:
            fn()
            results[name] = "PASS"
        except Skip as e:
            results[name] = f"SKIP ({e})"
        except Exception as e:
            results[name] = f"FAIL: {e}"
        print(f"{name:<34} {results[name]}")
    ok = sum(v == "PASS" for v in results.values())
    print(f"\n{ok}/{len(results)} passed")
    sys.exit(0 if all(v.startswith(("PASS", "SKIP")) for v in results.values()) else 1)