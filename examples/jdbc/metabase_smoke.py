"""Exercise a fresh, disposable Metabase through its Flight SQL community plugin.

Requires Metabase with plugin 0.8.0, the Arrow JVM flags, and a disposable
Redis Flight SQL backend. Never point this at an existing Metabase installation:
the script performs first-run setup and creates a local test administrator.
Seed the fixture with BiFixture.java before running. Application metadata stays
until the container is removed; drop the fixture with BiFixture.java afterward.
Uses only the Python standard library.
"""

import argparse
import json
import secrets
import time
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metabase", default="http://127.0.0.1:13030")
    parser.add_argument("--flight-host", default="host.docker.internal")
    parser.add_argument("--flight-port", type=int, default=18825)
    args = parser.parse_args()
    session = None

    def api(path, data=None, method=None):
        headers = {"Content-Type": "application/json"}
        if session:
            headers["X-Metabase-Session"] = session
        request = urllib.request.Request(
            args.metabase + "/api/" + path,
            data=None if data is None else json.dumps(data).encode(),
            headers=headers, method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=90) as response:
                body = response.read()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"{path}: HTTP {error.code}: {error.read().decode()}") from None

    deadline = time.monotonic() + 240
    while True:
        try:
            if api("health").get("status") == "ok":
                break
        except (urllib.error.URLError, OSError, RuntimeError):
            pass
        if time.monotonic() > deadline:
            raise RuntimeError("Metabase did not become healthy")
        time.sleep(2)
    props = api("session/properties")
    token = props.get("setup-token")
    if not token:
        raise RuntimeError("Refusing to use an already configured Metabase")
    result = api("setup", {
        "token": token,
        "user": {"first_name": "Flight", "last_name": "Validation",
                 "email": "flight-validation@example.com",
                 "password": "FlightTest!" + secrets.token_hex(16)},
        "prefs": {"site_name": "Disposable Flight SQL validation",
                  "allow_tracking": False},
    })
    session = result["id"]
    database = api("database", {
        "name": "Redis Flight SQL validation", "engine": "arrow-flight-sql",
        "details": {"host": args.flight_host, "port": args.flight_port,
                    "useEncryption": False, "catalog": "redis", "enable-actions": True},
        "is_full_sync": True, "is_on_demand": False,
    })
    dbid = database["id"]
    print("PASS database connection", flush=True)

    def native(sql):
        return api("dataset", {"database": dbid, "type": "native",
                              "native": {"query": sql}, "parameters": []})

    def checked(label, result, expected):
        if result.get("status") != "completed":
            print(json.dumps({"check": label, "status": result.get("status"),
                              "error": result.get("error")}), flush=True)
            raise AssertionError(label)
        rows = result["data"]["rows"]
        assert rows == expected, (label, rows, expected)
        print(json.dumps({"check": label, "status": "PASS", "rows": rows}), flush=True)

    table = "bi_jdbc_validation"
    checked("native ordered rows and NULL", native(
        f"SELECT id, category, amount FROM {table} ORDER BY id"),
        [[1, "east", 10.5], [2, "west", 20.0], [3, "east", None]])
    checked("native aggregate", native(
        f"SELECT category, COUNT(*), SUM(amount) FROM {table} GROUP BY category ORDER BY category"),
        [["east", 2, 10.5], ["west", 1, 20.0]])
    checked("native empty result", native(f"SELECT id FROM {table} WHERE id < 0"), [])
    api(f"database/{dbid}/sync_schema", {})
    deadline = time.monotonic() + 120
    while True:
        metadata = api(f"database/{dbid}/metadata")
        tables = [t for t in metadata.get("tables", []) if t["name"] == table]
        if tables and len(tables[0].get("fields", [])) == 3:
            break
        if time.monotonic() > deadline:
            raise AssertionError("Table/column discovery did not complete")
        time.sleep(2)
    found = tables[0]
    print(json.dumps({"check": "schema sync", "status": "PASS", "schema": found["schema"],
                      "fields": [{k: f.get(k) for k in ("name", "base_type", "database_type")}
                                 for f in found["fields"]]}), flush=True)
    fields = {f["name"]: f["id"] for f in found["fields"]}
    query = {"source-table": found["id"],
             "fields": [["field", fields[n], None] for n in ("id", "category", "amount")],
             "filter": [">", ["field", fields["id"], None], 1],
             "order-by": [["asc", ["field", fields["id"], None]]]}
    checked("Metabase generated filter/order", api("dataset", {
        "database": dbid, "type": "query", "query": query}),
        [[2, "west", 20.0], [3, "east", None]])
    query = {"source-table": found["id"], "aggregation": [["count"]],
             "breakout": [["field", fields["category"], None]],
             "order-by": [["asc", ["field", fields["category"], None]]]}
    checked("Metabase generated grouped count", api("dataset", {
        "database": dbid, "type": "query", "query": query}),
        [["east", 2], ["west", 1]])
    api(f"database/{dbid}", {"settings": {"database-enable-actions": True}}, method="PUT")
    model = api("card", {"name": "Flight validation model", "type": "model",
        "dataset_query": {"database": dbid, "type": "query", "query": {"source-table": found["id"]}},
        "display": "table", "visualization_settings": {}})
    for label, sql, expected in [
        ("INSERT read-back", f"INSERT INTO {table} VALUES (4, 'write-test', 5.0)",
         [[4, "write-test", 5.0]]),
        ("UPDATE read-back", f"UPDATE {table} SET amount = 9.5 WHERE id = 4",
         [[4, "write-test", 9.5]]),
        ("DELETE read-back", f"DELETE FROM {table} WHERE id = 4", []),
    ]:
        action = api("action", {"name": label, "type": "query", "model_id": model["id"],
            "database_id": dbid, "dataset_query": {"database": dbid, "type": "native",
                "native": {"query": sql, "template-tags": {}}}, "parameters": []})
        api(f"action/{action['id']}/execute", {"parameters": {}})
        checked(label, native(f"SELECT id, category, amount FROM {table} WHERE id = 4"), expected)
    print("All Metabase API smoke checks passed; browser rendering was not tested.", flush=True)


if __name__ == "__main__":
    main()
