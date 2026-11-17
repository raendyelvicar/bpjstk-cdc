#!/usr/bin/env python3
"""CDC active-active test suite: SQL Server <-> PostgreSQL, BPJS TK tables.

    python tests/test_cdc.py                 # all tests, then delete the test rows
    python tests/test_cdc.py TC-01 TC-05     # selected tests
    python tests/test_cdc.py --keep          # leave the test rows in place
    python tests/test_cdc.py --allow-remote  # required when the env file points at non-local databases

Settings come from .env.$APP_ENV (default .env.local; or $ENV_FILE), with the same names and defaults as scripts/env.sh.

Test rows are marked by BPJSNumber 'CDCT...' and NIK '99...' (99 is not a
valid province code), which is what the cleanup deletes.
"""
import datetime as dt
import os
import random
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


def load_env(path=os.getenv("ENV_FILE", ".env." + os.getenv("APP_ENV", "local"))):
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))


load_env()

LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_remote = sorted({h for h in (os.getenv("MSSQL_HOST", "localhost"), os.getenv("PG_HOST", "localhost"))
                  if h not in LOCAL_HOSTS})
if _remote and "--allow-remote" not in sys.argv[1:]:
    sys.exit(f"Refusing to run against {', '.join(_remote)}: the tests insert and delete CDCT* rows. "
             "Pass --allow-remote if that is intended.")

MSSQL_PASSWORD = os.environ["MSSQL_PASSWORD"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
# Same defaults as scripts/setup-db.sh: the test runs on the host, so it reaches the
# containers through their published ports. (The connector configs use the Docker
# service names sqlserver/postgres, which only resolve inside the compose network.)
MSSQL_HOST = os.getenv("MSSQL_HOST", "localhost")
MSSQL_PORT = int(os.getenv("MSSQL_PORT", "1433"))
MSSQL_DB = os.getenv("MSSQL_DB", "bpjsk_poc")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "bpjsk_poc")
# The tests write as the admin logins (never as a sink login: the loop filter
# would drop those writes).
MSSQL_ADMIN_USER = os.getenv("MSSQL_ADMIN_USER", "sa")
PG_USER = os.getenv("PG_ADMIN_USER", "postgres")
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
KSQLDB_URL = os.getenv("KSQLDB_URL", "http://localhost:8088")
# TODO(remote-kafka): set KAFKA_BOOTSTRAP to the remote broker's externally advertised listener
# (plus security_protocol/sasl_* kwargs on KafkaConsumer/KafkaProducer if it uses SASL/SSL).
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq.bpjsk-replication")

CONNECTORS = ["bpjsk-mssql-source", "bpjsk-postgres-source", "bpjsk-postgres-sink", "bpjsk-mssql-sink"]
KSQL_OUTPUTS = ["PG_CLAIM_TO_MSSQL", "PG_MEMBERSHIP_TO_MSSQL", "PG_CONTRIBUTION_TO_MSSQL",
                "MSSQL_CLAIM_TO_PG", "MSSQL_MEMBERSHIP_TO_PG", "MSSQL_CONTRIBUTION_TO_PG"]

# Audit user IDs. The loop filter no longer looks at them (it uses db_login); they
# only break LWW ties, so the higher one (PostgreSQL's here) wins a tie.
MS_USER, PG_USER_ID = 101, 1000101
# Logins the sinks write with (same defaults as docker-compose.yaml). The triggers
# stamp the connecting login into db_login and the ksqlDB filter drops these.
MSSQL_SINK_USER = os.getenv("MSSQL_SINK_USER", "cdc_sink")
PG_SINK_USER = os.getenv("PG_SINK_USER", "cdc_sink")
PG_ID_MIN = 1000000000  # SQL Server keys stay below this; PostgreSQL sequences start here

TIMEOUT = 30  # seconds to wait for replication
CONVERGE_HOLD = 10  # seconds both nodes must keep matching after converging


class MSCursor:
    """Keeps the pyodbc-style calls (? placeholders, chained fetchone) working on pymssql."""
    def __init__(self, c):
        self.c = c

    def execute(self, sql, *args):
        self.c.execute(sql.replace("?", "%s"), args or None)
        return self

    def fetchone(self):
        return self.c.fetchone()


class MSConn:
    def __init__(self, conn):
        self.conn = conn

    def cursor(self):
        return MSCursor(self.conn.cursor())


ms = MSConn(pymssql.connect(
    server=MSSQL_HOST, port=MSSQL_PORT, user=MSSQL_ADMIN_USER, password=MSSQL_PASSWORD,
    database=MSSQL_DB, autocommit=True, login_timeout=15,
))

pg = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER,
                      password=PG_PASSWORD, connect_timeout=15)
pg.autocommit = True


class Skip(Exception):
    pass


# ---------- tables ----------
# Column names are PostgreSQL's; MS_COL maps them to SQL Server's (same renames as the sinks).
TABLES = {
    "membership": {"ms": "dbo.BPJSMembership", "pg": "public.bpjs_membership",
                   "key": "membership_id", "value": "full_name",
                   "topic_ms": "mssql-bpjsk-cdc.bpjsk_poc.dbo.BPJSMembership",
                   "topic_pg": "pg-bpjsk-cdc.public.bpjs_membership"},
    "contribution": {"ms": "dbo.BPJSMonthlyContribution", "pg": "public.bpjs_monthly_contribution",
                     "key": "contribution_id", "value": "payment_status"},
    "claim": {"ms": "dbo.BPJSClaim", "pg": "public.bpjs_claim",
              "key": "claim_id", "value": "claim_status"},
}

MS_COL = {
    "membership_id": "MembershipID", "employee_id": "EmployeeID", "bpjs_number": "BPJSNumber",
    "nik": "NIK", "full_name": "FullName", "enrollment_date": "EnrollmentDate",
    "work_risk_group": "WorkRiskGroup", "is_active": "IsActive", "created_at": "CreatedAt",
    "contribution_id": "ContributionID", "period_month": "PeriodMonth", "period_year": "PeriodYear",
    "base_wage": "BaseWage", "jht_employee": "JHTEmployee", "jht_employer": "JHTEmployer",
    "jkk_employer": "JKKEmployer", "jkm_employer": "JKMEmployer", "jp_employee": "JPEmployee",
    "jp_employer": "JPEmployer", "total_amount": "TotalAmount", "payment_status": "PaymentStatus",
    "paid_at": "PaidAt", "claim_id": "ClaimID", "claim_number": "ClaimNumber",
    "program_type": "ProgramType", "claim_date": "ClaimDate", "reason_description": "ReasonDescription",
    "disbursed_amount": "DisbursedAmount", "claim_status": "ClaimStatus", "approved_at": "ApprovedAt",
}


def col(side, c):
    return MS_COL.get(c, c) if side == "ms" else c


def user(side):
    return MS_USER if side == "ms" else PG_USER_ID


def other(side):
    return "pg" if side == "ms" else "ms"


# ---------- helpers ----------
def utc_now():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def db_ts(side, t):  # SQL Server DATETIME2 takes a naive UTC value
    return t.replace(tzinfo=None) if side == "ms" and t is not None else t


def new_nik():
    return "99" + "".join(random.choices("0123456789", k=14))


def row_data(t, value, parent=None):
    """Column values for a new test row (PostgreSQL names)."""
    if t == "membership":
        return {"employee_id": random.randint(1, 2_000_000_000), "bpjs_number": f"CDCT{random.randrange(10**15):015d}",
                "nik": new_nik(), "full_name": value, "enrollment_date": dt.date(2024, 1, 1)}
    if t == "contribution":
        return {"membership_id": parent, "period_month": random.randint(1, 12),
                "period_year": random.randint(2000, 2099), "base_wage": 5000000.00,
                "jht_employee": 100000.00, "jht_employer": 185000.00, "jkk_employer": 12000.00,
                "jkm_employer": 15000.00, "jp_employee": 50000.00, "jp_employer": 100000.00,
                "payment_status": value}
    if t == "claim":
        return {"membership_id": parent, "claim_number": f"CDCT-{uuid.uuid4().hex[:20]}",
                "program_type": "JHT", "claim_date": dt.date(2025, 6, 1),
                "reason_description": "CDC test", "claim_status": value}
    raise KeyError(t)


# Columns that identify a new SQL Server row, since the INSTEAD OF trigger hides the IDENTITY value.
LOOKUP = {"membership": ["nik"], "contribution": ["membership_id", "period_month", "period_year"],
          "claim": ["claim_number"]}


def row_insert(side, t, value, parent=None, ts=None, data=None, by=None):
    """Insert as an application would (key from IDENTITY/sequence) and return the key."""
    spec = TABLES[t]
    ts = ts or utc_now() - dt.timedelta(seconds=60)
    data = {**(data or row_data(t, value, parent)), "created_at": db_ts(side, ts), "created_by": by or user(side)}
    cols = [col(side, c) for c in data]
    vals = list(data.values())
    if side == "ms":
        ms.cursor().execute(f"INSERT INTO {spec['ms']} ({', '.join(cols)}) VALUES ({', '.join('?' * len(vals))})", *vals)
        where = " AND ".join(f"{col('ms', c)}=?" for c in LOOKUP[t])
        r = ms.cursor().execute(f"SELECT {col('ms', spec['key'])} FROM {spec['ms']} WHERE {where}",
                                *[data[c] for c in LOOKUP[t]]).fetchone()
        assert r, f"{t}: inserted SQL Server row not found"
        assert r[0] < PG_ID_MIN, f"{t}: SQL Server key {r[0]} is in the PostgreSQL range"
        return r[0]
    with pg.cursor() as c:
        c.execute(f"INSERT INTO {spec['pg']} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(vals))}) "
                  f"RETURNING {spec['key']}", vals)
        rid = c.fetchone()[0]
    assert rid >= PG_ID_MIN, f"{t}: PostgreSQL key {rid} is in the SQL Server range (sequence not moved?)"
    return rid


def row_set(side, t, rid, **cols):
    """UPDATE the given columns (PostgreSQL names); returns rowcount on PostgreSQL, None on SQL Server."""
    spec = TABLES[t]
    sets = ", ".join(f"{col(side, c)}=?" for c in cols)
    vals = [db_ts(side, v) if isinstance(v, dt.datetime) else v for v in cols.values()]
    sql = f"UPDATE {spec[side]} SET {sets} WHERE {col(side, spec['key'])}=?"
    if side == "ms":
        ms.cursor().execute(sql, *vals, rid)
        return None
    with pg.cursor() as c:
        c.execute(sql.replace("?", "%s"), vals + [rid])
        return c.rowcount


def row_update(side, t, rid, value, ts=None, by=None):
    """An application update: new value plus a newer updated_at and updated_by."""
    return row_set(side, t, rid, **{TABLES[t]["value"]: value, "updated_at": ts or utc_now(),
                                    "updated_by": by or user(side)})


def row_fetch(side, t, rid, *cols):
    spec = TABLES[t]
    sql = f"SELECT {', '.join(col(side, c) for c in cols)} FROM {spec[side]} WHERE {col(side, spec['key'])}="
    if side == "ms":
        return ms.cursor().execute(sql + "?", rid).fetchone()
    with pg.cursor() as c:
        c.execute(sql + "%s", (rid,))
        return c.fetchone()


def row_get(side, t, rid):
    """(value, last writer) or None. Last writer = COALESCE(updated_by, created_by)."""
    r = row_fetch(side, t, rid, TABLES[t]["value"], "updated_by", "created_by")
    return None if r is None else (r[0], r[1] if r[1] is not None else r[2])


def row_delete(side, t, rid):
    spec = TABLES[t]
    sql = f"DELETE FROM {spec[side]} WHERE {col(side, spec['key'])}="
    if side == "ms":
        ms.cursor().execute(sql + "?", rid)
    else:
        with pg.cursor() as c:
            c.execute(sql + "%s", (rid,))


def wait_for(fn, what, timeout=TIMEOUT):
    end = time.time() + timeout
    while time.time() < end:
        v = fn()
        if v:
            return v
        time.sleep(1)
    raise AssertionError(f"timeout waiting for: {what}")


def replicated(side, t, rid):
    """Wait until a row written on `side` shows up on the other node."""
    wait_for(lambda: row_get(other(side), t, rid), f"{t} {rid} reaches {other(side)}")


def new_member(side="ms", value="BASE"):
    rid = row_insert(side, "membership", value)
    replicated(side, "membership", rid)
    return rid


def converged(t, rid, value=None, by=None):
    a, b = row_get("ms", t, rid), row_get("pg", t, rid)
    if not a or not b or a != b:
        return False
    return (value is None or a[0] == value) and (by is None or a[1] == by)


def wait_converged(t, rid, value, by, what, hold=CONVERGE_HOLD):
    """Wait for both nodes to match, then require that they keep matching.

    Without LWW the nodes swap values, so they can match briefly mid-swap.
    The hold outlasts in-flight replication, so that doesn't count as a pass.
    """
    wait_for(lambda: converged(t, rid, value, by), what)
    end = time.time() + hold
    while time.time() < end:
        assert converged(t, rid, value, by), (
            f"{what}: nodes diverged during the {hold}s hold: "
            f"mssql={row_get('ms', t, rid)} pg={row_get('pg', t, rid)}")
        time.sleep(1)


def stays(t, rid, value, by, what):
    wait_converged(t, rid, value, by, what)


def count_events(topic, marker):
    """Messages in `topic` whose value contains `marker` (read from the beginning)."""
    if KafkaConsumer is None:
        raise Skip("kafka-python not installed")
    cons = KafkaConsumer(topic, bootstrap_servers=KAFKA, auto_offset_reset="earliest",
                         enable_auto_commit=False, group_id=None, consumer_timeout_ms=8000)
    n = sum(1 for m in cons if marker in (m.value or b"").decode("utf-8", "ignore"))
    cons.close()
    return n


def assert_connectors_running():
    for name in CONNECTORS:
        st = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10).json()
        assert "connector" in st, f"{name}: {st}"
        states = [st["connector"]["state"]] + [x["state"] for x in st["tasks"]]
        assert states[1:] and all(s == "RUNNING" for s in states), f"{name} not RUNNING: {states}"


# ---------- test cases ----------
def tc01():
    rid = row_insert("ms", "membership", "NEW")
    replicated("ms", "membership", rid)
    row_update("ms", "membership", rid, "UPDATED_MS")
    stays("membership", rid, "UPDATED_MS", MS_USER, "SQL Server update reaches PostgreSQL")


def tc02():
    rid = row_insert("pg", "membership", "NEW")
    replicated("pg", "membership", rid)
    row_update("pg", "membership", rid, "UPDATED_PG")
    stays("membership", rid, "UPDATED_PG", PG_USER_ID, "PostgreSQL update reaches SQL Server")


def loop_check(side):
    """A change goes through the ksqlDB filter once and is not echoed back."""
    t = TABLES["membership"]
    rid = row_insert(side, "membership", "LOOP")
    replicated(side, "membership", rid)
    row_update(side, "membership", rid, "LOOP2")
    stays("membership", rid, "LOOP2", user(side), "update replicated")
    nik = row_fetch(side, "membership", rid, "nik")[0]
    src, dst = (t["topic_ms"], t["topic_pg"]) if side == "ms" else (t["topic_pg"], t["topic_ms"])
    fwd, back = (".to-pg", ".to-mssql") if side == "ms" else (".to-mssql", ".to-pg")
    counts = {
        src: (count_events(src, nik), 2),             # insert + update on the origin
        src + fwd: (count_events(src + fwd, nik), 2),  # both pass the filter
        dst: (count_events(dst, nik), 2),             # the sink's writes on the other node
        dst + back: (count_events(dst + back, nik), 0),  # filtered: nothing goes back
    }
    bad = {k: v for k, v in counts.items() if v[0] != v[1]}
    assert not bad, "event counts (got, expected): " + ", ".join(f"{k}={v}" for k, v in bad.items())


def tc03():
    loop_check("ms")


def tc04():
    loop_check("pg")


def tc05():
    # Concurrent updates: the newer updated_at wins on both nodes, in either direction.
    for newer in ("pg", "ms"):
        rid = new_member()
        t = utc_now()
        row_update(other(newer), "membership", rid, f"OLDER_{other(newer)}", ts=t - dt.timedelta(seconds=5))
        row_update(newer, "membership", rid, f"NEWER_{newer}", ts=t)
        stays("membership", rid, f"NEWER_{newer}", user(newer), f"newer timestamp ({newer}) wins on both")


def tc06():
    # Same updated_at: the higher updated_by wins, i.e. PostgreSQL users beat SQL Server users.
    rid = new_member()
    t = utc_now()
    row_update("ms", "membership", rid, "TIE_MS", ts=t)
    row_update("pg", "membership", rid, "TIE_PG", ts=t)
    stays("membership", rid, "TIE_PG", PG_USER_ID, "higher updated_by wins on both")


def tc07():
    # A write older than the current row is rejected by the local trigger.
    for side in ("ms", "pg"):
        rid = row_insert(side, "membership", "LATEST")
        replicated(side, "membership", rid)
        t = utc_now()
        row_update(side, "membership", rid, "LATEST", ts=t)
        stays("membership", rid, "LATEST", user(side), "baseline")
        n = row_update(side, "membership", rid, "STALE", ts=t - dt.timedelta(minutes=5))
        assert n in (None, 0), f"{side}: trigger reported {n} rows for a stale write"
        assert row_get(side, "membership", rid)[0] == "LATEST", f"{side}: trigger accepted a stale write"
        stays("membership", rid, "LATEST", user(side), f"{side}: stale write did not replicate")


def tc08():
    # An update that does not move updated_at forward is dropped (documented writer rule).
    for side in ("ms", "pg"):
        rid = row_insert(side, "membership", "KEEP")
        replicated(side, "membership", rid)
        n = row_set(side, "membership", rid, full_name="NO_TS")
        assert n in (None, 0), f"{side}: trigger reported {n} rows for an update without updated_at"
        assert row_get(side, "membership", rid)[0] == "KEEP", f"{side}: update without newer updated_at applied"


def tc09():
    for side in ("ms", "pg"):
        rid = new_member(side, "DEL")
        row_delete(side, "membership", rid)
        wait_for(lambda: row_get(other(side), "membership", rid) is None,
                 f"{side} hard delete reaches {other(side)}")
        time.sleep(5)
        assert row_get(side, "membership", rid) is None, f"{side}: deleted row came back"


def tc10():
    # Soft delete = an ordinary update of deleted_at/deleted_by (plus updated_at/updated_by).
    for side in ("ms", "pg"):
        rid = new_member(side, "SOFT")
        t = utc_now()
        row_set(side, "membership", rid, full_name="SOFT_DELETED", is_active=False, deleted_at=t,
                deleted_by=user(side), updated_at=t, updated_by=user(side))
        stays("membership", rid, "SOFT_DELETED", user(side), f"{side} soft delete replicates")
        r = row_fetch(other(side), "membership", rid, "deleted_by", "is_active")
        assert r[0] == user(side) and not r[1], f"{side}: soft delete columns on {other(side)}: {r}"


def tc11():
    # Contributions and claims both ways, each referencing a membership that replicated first
    # (both nodes enforce the foreign keys).
    for t in ("contribution", "claim"):
        for side in ("ms", "pg"):
            parent = new_member("pg" if side == "ms" else "ms")  # parent from the other node
            rid = row_insert(side, t, "NEW", parent=parent)
            replicated(side, t, rid)
            row_update(side, t, rid, "PAID" if t == "contribution" else "APPROVED")
            stays(t, rid, "PAID" if t == "contribution" else "APPROVED", user(side), f"{t} {side} update")
            if t == "contribution":
                a = row_fetch("ms", t, rid, "total_amount")[0]
                b = row_fetch("pg", t, rid, "total_amount")[0]
                assert a is not None and float(a) == float(b), f"TotalAmount differs: mssql={a} pg={b}"
    assert_connectors_running()


def tc12():
    # Non-JSON in a CDC topic passes the ksqlDB filter and the sink sends it to the DLQ.
    if KafkaProducer is None:
        raise Skip("kafka-python not installed")
    marker = f"cdc-test-not-json-{uuid.uuid4()}"
    p = KafkaProducer(bootstrap_servers=KAFKA)
    p.send(TABLES["membership"]["topic_ms"], key=b"bad", value=marker.encode())
    p.flush()
    p.close()
    try:
        wait_for(lambda: count_events(DLQ_TOPIC, marker) >= 1, f"marker in {DLQ_TOPIC}", timeout=60)
    except AssertionError:
        filtered = TABLES["membership"]["topic_ms"] + ".to-pg"
        if count_events(filtered, marker) == 0:
            raise AssertionError(f"ksqlDB dropped the non-JSON record (not in {filtered})")
        raise AssertionError(f"record reached {filtered} but not {DLQ_TOPIC}; check the sink's errors.* settings")
    assert_connectors_running()


def tc13():
    # The six ksqlDB filter queries exist and run; all connectors run.
    r = requests.post(f"{KSQLDB_URL}/ksql", json={"ksql": "SHOW QUERIES;", "streamsProperties": {}},
                      headers={"Content-Type": "application/vnd.ksql.v1+json"}, timeout=10).json()
    queries = r[0]["queries"]
    running = {s.upper() for q in queries if q.get("state", "RUNNING") == "RUNNING"
               and all(k == "RUNNING" for k in (q.get("statusCount") or {"RUNNING": 1}))
               for s in q["sinks"]}
    missing = [s for s in KSQL_OUTPUTS if s not in running]
    assert not missing, f"ksqlDB queries not RUNNING: {missing}"
    assert_connectors_running()


def tc14():
    # Changing a unique column (NIK) can make SQL Server CDC log the update as
    # delete + insert. The delete passes the filter; the row must not be lost.
    for side in ("ms", "pg"):
        rid = new_member(side, "NIK")
        nik = new_nik()
        row_set(side, "membership", rid, nik=nik, full_name=f"NIK_{side}", updated_at=utc_now(),
                updated_by=user(side))
        stays("membership", rid, f"NIK_{side}", user(side), f"{side} NIK change survives on both")
        for s in ("ms", "pg"):
            got = row_fetch(s, "membership", rid, "nik")[0]
            assert got == nik, f"{side} NIK change: {s} has {got}, expected {nik}"


def tc15():
    # The loop filter goes by the login that wrote the row, not by the audit user
    # IDs: a row whose created_by/updated_by is in the other node's range still
    # replicates, db_login is the test's own login on the origin and the sink login
    # on the replica (a value the writer sends is overwritten), and nothing echoes back.
    t = TABLES["membership"]
    me = {"ms": ms.cursor().execute("SELECT ORIGINAL_LOGIN()").fetchone()[0], "pg": PG_USER}
    sink = {"ms": MSSQL_SINK_USER, "pg": PG_SINK_USER}
    for side in ("ms", "pg"):
        wrong = user(other(side))
        data = {**row_data("membership", "LOGIN"), "db_login": "spoofed"}
        rid = row_insert(side, "membership", "LOGIN", data=data, by=wrong)
        replicated(side, "membership", rid)
        row_update(side, "membership", rid, "LOGIN2", by=wrong)
        stays("membership", rid, "LOGIN2", wrong, f"{side} row with {other(side)} user IDs replicates")
        got = {s: row_fetch(s, "membership", rid, "db_login")[0] for s in ("ms", "pg")}
        want = {side: me[side], other(side): sink[other(side)]}
        assert {k: (v or "").lower() for k, v in got.items()} == {k: v.lower() for k, v in want.items()}, \
            f"{side}: db_login {got}, expected {want}"
        nik = row_fetch(side, "membership", rid, "nik")[0]
        dst = t["topic_pg"] if side == "ms" else t["topic_ms"]
        back = dst + (".to-mssql" if side == "ms" else ".to-pg")
        n = count_events(back, nik)
        assert n == 0, f"{side}: {n} echo event(s) in {back}"



def tc16():
    # A replicated insert (explicit key, as the sink sends it) that fails, here on
    # the foreign key, must leave SQL Server's IDENTITY where it was: the failed
    # insert moves it into the PostgreSQL range and a rollback doesn't undo that,
    # so the trigger reseeds it before re-raising the error.
    cur = pymssql.connect(server=MSSQL_HOST, port=MSSQL_PORT, user=MSSQL_ADMIN_USER, password=MSSQL_PASSWORD,
                          database=MSSQL_DB, login_timeout=15)
    try:
        c = cur.cursor()
        c.execute("SELECT IDENT_CURRENT('dbo.BPJSClaim')")
        before = c.fetchone()[0]
        try:
            c.execute("SET IDENTITY_INSERT dbo.BPJSClaim ON; "
                      "INSERT INTO dbo.BPJSClaim (ClaimID, MembershipID, ClaimNumber, ProgramType, ClaimDate, "
                      "ReasonDescription, ClaimStatus, CreatedAt, created_by) "
                      "VALUES (%s, -1, %s, 'JHT', '2025-06-01', 'CDC test', 'NEW', SYSUTCDATETIME(), %s)",
                      (PG_ID_MIN + 999999, f"CDCT-{uuid.uuid4().hex[:20]}", PG_USER_ID))
            cur.commit()
            c.execute("DELETE FROM dbo.BPJSClaim WHERE ClaimID = %s", (PG_ID_MIN + 999999,))
            cur.commit()
            raise Skip("foreign keys are disabled, so the insert can't be made to fail")
        except pymssql.Error as e:
            assert "FOREIGN KEY" in str(e), f"unexpected error: {e}"
        try:
            cur.rollback()
        except pymssql.Error:
            pass
        c = cur.cursor()
        c.execute("SELECT IDENT_CURRENT('dbo.BPJSClaim')")
        after = c.fetchone()[0]
        assert after == before, f"IDENTITY moved from {before} to {after} after a failed replicated insert"
    finally:
        cur.close()


# ---------- cleanup ----------
def cleanup():
    """Delete the test rows on both nodes (children first)."""
    for side in ("ms", "pg"):
        m = TABLES["membership"][side]
        mk, bn = col(side, "membership_id"), col(side, "bpjs_number")
        sqls = [f"DELETE FROM {TABLES[t][side]} WHERE {mk} IN (SELECT {mk} FROM {m} WHERE {bn} LIKE 'CDCT%')"
                for t in ("claim", "contribution")]
        sqls.append(f"DELETE FROM {m} WHERE {bn} LIKE 'CDCT%'")
        for sql in sqls:
            try:
                if side == "ms":
                    ms.cursor().execute(sql)
                else:
                    with pg.cursor() as c:
                        c.execute(sql)
            except Exception as e:
                print(f"cleanup ({side}) failed: {e}")


TESTS = [
    ("TC-01 SQL Server -> PostgreSQL", tc01),
    ("TC-02 PostgreSQL -> SQL Server", tc02),
    ("TC-03 Loop filter (SQL Server origin)", tc03),
    ("TC-04 Loop filter (PostgreSQL origin)", tc04),
    ("TC-05 LWW newer timestamp", tc05),
    ("TC-06 LWW tie -> higher user", tc06),
    ("TC-07 Stale write rejected", tc07),
    ("TC-08 Update w/o newer ts dropped", tc08),
    ("TC-09 Hard delete both ways", tc09),
    ("TC-10 Soft delete both ways", tc10),
    ("TC-11 Contribution/claim both ways", tc11),
    ("TC-12 DLQ routing", tc12),
    ("TC-13 ksqlDB queries + connectors", tc13),
    ("TC-14 NIK (unique) change", tc14),
    ("TC-15 Loop filter by db_login", tc15),
    ("TC-16 Failed insert keeps IDENTITY", tc16),
]

if __name__ == "__main__":
    args = sys.argv[1:]
    keep = "--keep" in args
    only = {a for a in args if a not in ("--keep", "--allow-remote")}  # e.g. python tests/test_cdc.py TC-01 TC-02
    results = {}
    try:
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
            print(f"{name:<40} {results[name]}", flush=True)
    finally:
        if not keep:
            cleanup()
    ok = sum(v == "PASS" for v in results.values())
    print(f"\n{ok}/{len(results)} passed")
    sys.exit(0 if all(v.startswith(("PASS", "SKIP")) for v in results.values()) else 1)
