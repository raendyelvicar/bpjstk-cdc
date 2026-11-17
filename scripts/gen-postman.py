#!/usr/bin/env python3
"""Generates the "BPJS CDC Ops" Postman collection (v3, local files) and its
local / development / staging environments.

    python3 scripts/gen-postman.py

Sources, so the collection never drifts from what the shell scripts send:
  configs/source/*.json, configs/sink/*.json  -> "3 Update connectors" bodies
                                                 (${env:VAR} becomes {{VAR}})
  sql/ksqldb-01-loop-filter.sql               -> "4 Loop filter" statements
  ENVIRONMENTS below (mirrors .env.example)   -> postman/environments/*.yaml

Everything under postman/collections/bpjsk-cdc-ops/ is rewritten on each run;
edit this script, not the generated files. Passwords are never written: the
environments hold empty secret entries, and scripts/postman-run.sh fills them
from .env.$APP_ENV at run time.
"""
import json
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COLL = ROOT / "postman" / "collections" / "bpjsk-cdc-ops"
ENVS = ROOT / "postman" / "environments"

SOURCES = ["configs/source/bpjsk-mssql-source.json", "configs/source/bpjsk-postgres-source.json"]
SINKS = ["configs/sink/bpjsk-postgres-sink.json", "configs/sink/bpjsk-mssql-sink.json"]
CONNECTORS = ["bpjsk-mssql-source", "bpjsk-postgres-source", "bpjsk-postgres-sink", "bpjsk-mssql-sink"]
KSQL_OUTPUTS = ["PG_CLAIM_TO_MSSQL", "PG_MEMBERSHIP_TO_MSSQL", "PG_CONTRIBUTION_TO_MSSQL",
                "MSSQL_CLAIM_TO_PG", "MSSQL_MEMBERSHIP_TO_PG", "MSSQL_CONTRIBUTION_TO_PG"]
SQL_FILE = ROOT / "sql" / "ksqldb-01-loop-filter.sql"

SECRETS = ["CONNECT_PWD", "KSQLDB_PASSWORD", "MSSQL_USER_PASSWORD", "PG_USER_PASSWORD",
           "MSSQL_SINK_PASSWORD", "PG_SINK_PASSWORD"]

# Non-secret values per environment; same defaults as scripts/env.sh and the
# three sections of .env.example. scripts/postman-run.sh overrides all of them
# from .env.$APP_ENV, so these matter only when running from the Postman app.
_common = {"MSSQL_DB": "bpjsk_poc", "PG_DB": "bpjsk_poc", "MSSQL_SINK_DB": "bpjsk_poc",
           "PG_SINK_DB": "bpjsk_poc", "MSSQL_SINK_USER": "cdc_sink", "PG_SINK_USER": "cdc_sink",
           "CONNECT_USER": "", "KSQLDB_USER": ""}
ENVIRONMENTS = {
    "local": {**_common,
              "CONNECT_URL": "http://localhost:8083", "KSQLDB_URL": "http://localhost:8088",
              "CONNECT_MSSQL_HOST": "sqlserver", "CONNECT_MSSQL_PORT": "1433",
              "CONNECT_PG_HOST": "postgres", "CONNECT_PG_PORT": "5432",
              "CONNECT_KAFKA_BOOTSTRAP": "kafka:29092",
              "MSSQL_USER": "sa", "PG_USER": "debezium_replication"},
    "development": {**_common,
                    "CONNECT_URL": "https://connect.kafka.kodehive.id",
                    "KSQLDB_URL": "https://ksqldb.kafka.kodehive.id",
                    "CONNECT_MSSQL_HOST": "sqlserver.kodehive.id", "CONNECT_MSSQL_PORT": "6978",
                    "CONNECT_PG_HOST": "postgres.kodehive.id", "CONNECT_PG_PORT": "6969",
                    "CONNECT_KAFKA_BOOTSTRAP": "kafka:29092",
                    "MSSQL_USER": "sa", "PG_USER": "debezium_replication"},
    "staging": {**_common,
                "CONNECT_URL": "https://<staging connect host>",
                "KSQLDB_URL": "https://<staging ksqldb host>",
                "CONNECT_MSSQL_HOST": "<staging SQL Server host>", "CONNECT_MSSQL_PORT": "1433",
                "CONNECT_PG_HOST": "<staging PostgreSQL host>", "CONNECT_PG_PORT": "5432",
                "CONNECT_KAFKA_BOOTSTRAP": "<broker as the Connect worker reaches it>",
                "MSSQL_USER": "sa", "PG_USER": "debezium_replication"},
}
for env in ENVIRONMENTS.values():  # sinks write to the source databases unless overridden
    env.update({"MSSQL_SINK_HOST": env["CONNECT_MSSQL_HOST"], "MSSQL_SINK_PORT": env["CONNECT_MSSQL_PORT"],
                "PG_SINK_HOST": env["CONNECT_PG_HOST"], "PG_SINK_PORT": env["CONNECT_PG_PORT"]})


# ---------------------------------------------------------------- YAML helpers
def q(s):
    return "'" + str(s).replace("'", "''") + "'"


def block(text, indent):
    pad = " " * indent
    return "|-\n" + "\n".join(pad + line if line else "" for line in text.splitlines())


def script(kind, code, indent=0):
    p = " " * indent
    return f"{p}  - type: {kind}\n{p}    language: text/javascript\n{p}    code: {block(code, indent + 6)}\n"


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip("\n") + "\n")


def request(folder, fname, order, method, url, *, name=None, body=None, tests=None, auth=None,
            json_body=True, description=None):
    out = ["$kind: http-request"]
    if name:
        out.append(f"name: {q(name)}")
    if description:
        out.append(f"description: {block(description, 2)}")
    out += [f"method: {method}", f"url: {q(url)}", f"order: {order}"]
    if body is not None and json_body:
        out += ["headers:", "  - key: Content-Type", "    value: application/json"]
    if auth:
        out.append(auth)
    if body is not None:
        out += ["body:", "  type: json", f"  content: {block(body, 4)}"]
    text = "\n".join(out) + "\n"
    if tests:
        text += "scripts:\n" + script("afterResponse", tests)
    write(COLL / folder / f"{fname}.request.yaml", text)


def definition(folder, order=None, description=None, auth=None, scripts=()):
    out = ["$kind: collection"]
    if folder is None:
        out.append(f"name: {q('BPJS CDC Ops')}")
    if description:
        out.append(f"description: {block(description, 2)}")
    if order is not None:
        out.append(f"order: {order}")
    if auth:
        out.append(auth)
    text = "\n".join(out) + "\n"
    if scripts:
        text += "scripts:\n" + "".join(script(k, c) for k, c in scripts)
    write((COLL / folder if folder else COLL) / ".resources" / "definition.yaml", text)


def basic(user, pw):
    return (f"auth:\n  type: basic\n  credentials:\n    - key: username\n      value: {q(user)}\n"
            f"    - key: password\n      value: {q(pw)}")


CONNECT_AUTH = basic("{{CONNECT_USER}}", "{{CONNECT_PWD}}")
KSQL_AUTH = basic("{{KSQLDB_USER}}", "{{KSQLDB_PASSWORD}}")

# ---------------------------------------------------------------- scripts
FILL_BODY = r"""
// Fills {{VAR}} placeholders in a raw body with JSON-escaped values, so a
// password containing " or \ cannot break the JSON, and stops the request when
// a value is missing (instead of sending an empty host or password).
const body = pm.request.body;
if (body && body.mode === 'raw' && /\{\{\w+\}\}/.test(body.raw)) {
  const missing = [];
  const raw = body.raw.replace(/\{\{(\w+)\}\}/g, (m, k) => {
    const v = pm.variables.get(k);
    if (v === undefined || v === null || v === '') { missing.push(k); return m; }
    return JSON.stringify(String(v)).slice(1, -1);
  });
  if (missing.length) {
    throw new Error('Not set: ' + missing.join(', ') + '. Run through scripts/postman-run.sh, or fill the environment.');
  }
  pm.request.body.update(raw);
}
""".strip()

KSQL_WAIT = r"""
// Every statement must be accepted, and ksqlDB must finish executing it before
// the next one is sent: an in-place upgrade can take a minute, and statements
// queued behind it would otherwise time out (see scripts/register-ksqldb.sh).
let res;
try { res = pm.response.json(); } catch (e) { res = pm.response.text(); }
pm.test('statement accepted', () => {
  pm.expect(Array.isArray(res), JSON.stringify(res).slice(0, 800)).to.equal(true);
  pm.expect(res.filter(e => e['@type'] === 'statement_error').map(e => e.message)).to.be.empty;
});
const seqs = (Array.isArray(res) ? res : [])
  .map(e => e.commandSequenceNumber).filter(n => typeof n === 'number' && n >= 0);
if (seqs.length) {
  const seq = Math.max(...seqs);
  const headers = [{ key: 'Content-Type', value: 'application/vnd.ksql.v1+json' }];
  const user = pm.variables.get('KSQLDB_USER');
  if (user) {
    const token = CryptoJS.enc.Base64.stringify(CryptoJS.enc.Utf8.parse(user + ':' + (pm.variables.get('KSQLDB_PASSWORD') || '')));
    headers.push({ key: 'Authorization', value: 'Basic ' + token });
  }
  const deadline = Date.now() + 5 * 60 * 1000;
  let done = false;
  while (!done && Date.now() < deadline) {
    // Waits server-side until every command up to seq has run (gives up after a few seconds; ask again).
    const r = await pm.sendRequest({
      url: pm.variables.replaceIn('{{KSQLDB_URL}}') + '/ksql', method: 'POST', header: headers,
      body: { mode: 'raw', raw: JSON.stringify({ ksql: 'LIST STREAMS;', commandSequenceNumber: seq }) }
    });
    try { done = Array.isArray(r.json()); } catch (e) { done = false; }
    if (!done) await new Promise(resolve => setTimeout(resolve, 2000));
  }
  pm.test(`ksqlDB executed command ${seq}`, () => pm.expect(done, 'not done after 5 minutes').to.equal(true));
}
""".strip()


def status_code(*codes):
    return f"pm.test('HTTP {'/'.join(map(str, codes))}', () => pm.expect(pm.response.code, pm.response.text().slice(0, 800)).to.be.oneOf({list(codes)}));"


# ---------------------------------------------------------------- ksql statements
def ksql_statements():
    lines = [l for l in SQL_FILE.read_text().splitlines() if not l.lstrip().startswith("--")]
    stmts, buf = [], []
    for l in lines:
        buf.append(l)
        if re.search(r";\s*$", l):
            stmts.append("\n".join(buf).strip())
            buf = []
    raw = [s for s in stmts if re.search(r"_raw\s*\(", s)]
    out = []
    for s in stmts:
        m = re.match(r"CREATE STREAM IF NOT EXISTS (\w+_to_(?:pg|mssql))\b", s)
        if m:
            out.append((m.group(1).upper(), re.sub(r"^CREATE STREAM IF NOT EXISTS ", "CREATE OR REPLACE STREAM ", s)))
    assert len(raw) == 6 and [n for n, _ in out] == KSQL_OUTPUTS, (len(raw), [n for n, _ in out])
    return raw, out


def ksql_body(stmt):
    return json.dumps({"ksql": stmt, "streamsProperties": {"auto.offset.reset": "earliest"},
                       "sessionVariables": {"PG_SINK_USER": "{{PG_SINK_USER}}",
                                            "MSSQL_SINK_USER": "{{MSSQL_SINK_USER}}"}}, indent=2)


def connector_body(path):
    cfg = json.loads((ROOT / path).read_text())["config"]
    cfg = {k: v for k, v in cfg.items() if not (k.startswith("TODO(") or k.startswith("NOTE "))}
    text = json.dumps(cfg, indent=4, ensure_ascii=False)
    return re.sub(r"\$\{env:([A-Za-z0-9_]+)\}", r"{{\1}}", text), json.loads((ROOT / path).read_text())["name"]


# ---------------------------------------------------------------- build
def build():
    if COLL.exists():
        shutil.rmtree(COLL)
    definition(None, description=(
        "Kafka Connect and ksqlDB operations for the BPJS CDC replication (see README.md).\n\n"
        "GENERATED by scripts/gen-postman.py from configs/*.json and sql/ksqldb-01-loop-filter.sql; "
        "edit those and re-run it, not these files.\n\n"
        "Run a folder: APP_ENV=<local|development|staging> bash scripts/postman-run.sh \"1 Check\"\n\n"
        "Database setup (schema, db_login, triggers, CDC, sink logins) is not an API: run scripts/setup-db.sh."),
        auth=CONNECT_AUTH, scripts=[("http:beforeRequest", FILL_BODY)])

    # 1 Check (read only)
    f = "1 Check"
    definition(f, 1000, "Read only. Connectors, sink logins, ksqlDB health and filter queries.")
    request(f, "Connect worker", 1000, "GET", "{{CONNECT_URL}}/",
            tests=status_code(200) + "\npm.test('Connect version', () => pm.expect(pm.response.json().version).to.be.a('string'));")
    request(f, "Connector status", 2000, "GET", "{{CONNECT_URL}}/connectors?expand=status", tests=(
        status_code(200) + "\nconst all = pm.response.json();\n"
        f"for (const name of {json.dumps(CONNECTORS)}) {{\n"
        "  pm.test(`${name} RUNNING`, () => {\n"
        "    pm.expect(all, 'not registered').to.have.property(name);\n"
        "    const s = all[name].status;\n"
        "    const states = [s.connector.state, ...s.tasks.map(t => t.state)];\n"
        "    pm.expect(s.tasks, 'no tasks').to.not.be.empty;\n"
        "    pm.expect(states.every(x => x === 'RUNNING'), states.join(',')).to.equal(true);\n"
        "  });\n}"))
    request(f, "Connector configs", 3000, "GET", "{{CONNECT_URL}}/connectors?expand=info", description=(
        "Each connector targets the databases and logins of this environment. The sinks must connect as the "
        "dedicated sink logins and drop db_login; otherwise the loop filter lets echoes through, or drops "
        "real writes."), tests=(
        status_code(200) + "\nconst all = pm.response.json();\n"
        "const v = k => String(pm.variables.get(k) || '');\n"
        "const lc = s => String(s || '').toLowerCase();\n"
        "const cfg = name => { const c = all[name] && all[name].info.config; pm.expect(c, name + ' not registered').to.be.an('object'); return c; };\n"
        "const sources = { 'bpjsk-mssql-source': ['CONNECT_MSSQL_HOST', 'CONNECT_MSSQL_PORT', 'MSSQL_USER'],\n"
        "                  'bpjsk-postgres-source': ['CONNECT_PG_HOST', 'CONNECT_PG_PORT', 'PG_USER'] };\n"
        "for (const [name, [h, p, u]] of Object.entries(sources)) {\n"
        "  pm.test(`${name} reads ${v(h)}:${v(p)} as ${v(u)}`, () => {\n"
        "    const c = cfg(name);\n"
        "    pm.expect(`${c['database.hostname']}:${c['database.port']}`).to.equal(`${v(h)}:${v(p)}`);\n"
        "    pm.expect(lc(c['database.user'])).to.equal(lc(v(u)));\n"
        "  });\n}\n"
        "const sinks = { 'bpjsk-mssql-sink': ['MSSQL_SINK_HOST', 'MSSQL_SINK_PORT', 'MSSQL_SINK_USER'],\n"
        "                'bpjsk-postgres-sink': ['PG_SINK_HOST', 'PG_SINK_PORT', 'PG_SINK_USER'] };\n"
        "for (const [name, [h, p, u]] of Object.entries(sinks)) {\n"
        "  pm.test(`${name} writes ${v(h)}:${v(p)} as ${v(u)} and drops db_login`, () => {\n"
        "    const c = cfg(name);\n"
        "    pm.expect(c['connection.url']).to.include(`//${v(h)}:${v(p)}`);\n"
        "    pm.expect(lc(c['connection.username'])).to.equal(lc(v(u)));\n"
        "    pm.expect(c['transforms.dropLogin.exclude']).to.equal('db_login');\n"
        "  });\n}"))
    request(f, "ksqlDB health", 4000, "GET", "{{KSQLDB_URL}}/healthcheck", auth=KSQL_AUTH,
            tests=status_code(200) + "\npm.test('healthy', () => pm.expect(pm.response.json().isHealthy, JSON.stringify(pm.response.json().details)).to.equal(true));")
    request(f, "ksqlDB filter queries", 5000, "POST", "{{KSQLDB_URL}}/ksql", auth=KSQL_AUTH,
            body=json.dumps({"ksql": "SHOW QUERIES;"}, indent=2), tests=(
        status_code(200) + "\nconst queries = pm.response.json()[0].queries;\n"
        f"for (const out of {json.dumps(KSQL_OUTPUTS)}) {{\n"
        "  pm.test(`${out} RUNNING`, () => {\n"
        "    const q = queries.find(x => x.sinks.map(s => s.toUpperCase()).includes(out));\n"
        "    pm.expect(q, 'no query writes ' + out).to.be.an('object');\n"
        "    pm.expect(q.state).to.equal('RUNNING');\n"
        "  });\n}"))

    # 2 Pause
    f = "2 Pause"
    definition(f, 2000, "Pause all four connectors (sinks first). Offsets are kept; resume with 5 Resume.")
    for i, name in enumerate(["bpjsk-postgres-sink", "bpjsk-mssql-sink", "bpjsk-mssql-source", "bpjsk-postgres-source"]):
        request(f, f"Pause {name}", 1000 * (i + 1), "PUT", f"{{{{CONNECT_URL}}}}/connectors/{name}/pause", tests=status_code(202))

    # 3 Update connectors
    f = "3 Update connectors"
    definition(f, 3000, (
        "PUT /connectors/<name>/config: creates a missing connector, updates an existing one in place "
        "(offsets kept, a paused connector stays paused). Bodies come from configs/*.json."))
    for i, path in enumerate(SOURCES + SINKS):
        body, name = connector_body(path)
        request(f, f"Put {name}", 1000 * (i + 1), "PUT", f"{{{{CONNECT_URL}}}}/connectors/{name}/config",
                body=body, description=f"From {path}.", tests=status_code(200, 201))

    # 4 Loop filter
    f = "4 Loop filter"
    raw, outs = ksql_statements()
    definition(f, 4000, (
        "Creates the raw streams if missing, then applies each filter with CREATE OR REPLACE (an in-place "
        "upgrade: same query, offsets kept). Each request waits until ksqlDB has executed it. Afterwards ksqlDB "
        "rebalances for a minute or more; run 1 Check until it is healthy. The CDC source topics must exist "
        "(scripts/register-ksqldb.sh creates missing ones; this folder does not)."),
        auth=KSQL_AUTH, scripts=[("http:afterResponse", KSQL_WAIT)])
    request(f, "Raw streams", 1000, "POST", "{{KSQLDB_URL}}/ksql", body=ksql_body("\n\n".join(raw)))
    for i, (name, stmt) in enumerate(outs):
        request(f, f"Filter {name}", 2000 + 1000 * i, "POST", "{{KSQLDB_URL}}/ksql", body=ksql_body(stmt))

    # 5 Resume
    f = "5 Resume"
    definition(f, 5000, "Resume all four connectors, sources first.")
    for i, name in enumerate(CONNECTORS):
        request(f, f"Resume {name}", 1000 * (i + 1), "PUT", f"{{{{CONNECT_URL}}}}/connectors/{name}/resume", tests=status_code(202))

    # 6 Restart failed tasks
    f = "6 Restart failed tasks"
    definition(f, 6000, "Restart only the failed connector instances and tasks of the four connectors.")
    for i, name in enumerate(CONNECTORS):
        request(f, f"Restart {name}", 1000 * (i + 1), "POST",
                f"{{{{CONNECT_URL}}}}/connectors/{name}/restart?includeTasks=true&onlyFailed=true",
                tests=status_code(200, 202, 204))
    # queryParams must mirror the URL (lint FMT209)
    for p in (COLL / f).glob("*.request.yaml"):
        p.write_text(p.read_text().replace("order:", "queryParams:\n  - key: includeTasks\n    value: 'true'\n"
                                           "  - key: onlyFailed\n    value: 'true'\norder:", 1))
    for p in (COLL / "1 Check").glob("Connector status.request.yaml"):
        p.write_text(p.read_text().replace("order:", "queryParams:\n  - key: expand\n    value: status\norder:", 1))
    for p in (COLL / "1 Check").glob("Connector configs.request.yaml"):
        p.write_text(p.read_text().replace("order:", "queryParams:\n  - key: expand\n    value: info\norder:", 1))


def environments():
    ENVS.mkdir(parents=True, exist_ok=True)
    for env, values in ENVIRONMENTS.items():
        out = [f"name: {q('BPJS CDC ' + env)}", "values:"]
        for k in sorted(values):
            out += [f"  - key: {k}", f"    value: {q(values[k])}", "    enabled: true", "    type: default"]
        for k in SECRETS:
            out += [f"  - key: {k}", "    value: ''", "    enabled: true", "    type: secret",
                    "    description: Filled from .env.$APP_ENV by scripts/postman-run.sh; never commit a value."]
        write(ENVS / f"{env}.environment.yaml", "\n".join(out))


if __name__ == "__main__":
    build()
    environments()
    print(f"wrote {COLL.relative_to(ROOT)} and {ENVS.relative_to(ROOT)}/{{{','.join(ENVIRONMENTS)}}}.environment.yaml")
