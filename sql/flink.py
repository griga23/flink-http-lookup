#!/usr/bin/env python3
"""Run Flink SQL on Confluent Cloud via REST (secrets from ../secrets.env).

Usage: flink.py FILE.sql [--snapshot]

A file may hold several ';'-terminated statements. `SET 'k' = 'v';` lines apply to the
statements after them. Bounded statements (DDL, SHOW, snapshot SELECT) print their rows;
INSERT statements are left running and their names are printed.
"""
import json, pathlib, re, sys, time, uuid
import requests

env = dict(l.strip().split("=", 1) for l in (pathlib.Path(__file__).parent.parent / "secrets.env").read_text().splitlines() if "=" in l)
base = f"{env['FLINK_REST']}/sql/v1/organizations/{env['FLINK_ORG_ID']}/environments/{env['FLINK_ENV_ID']}/statements"
auth = (env["FLINK_API_KEY"], env["FLINK_API_SECRET"])


def run(sql, name=None, props=None):
    name = name or f"demo-{uuid.uuid4().hex[:12]}"
    props = props or {}
    body = {"name": name, "spec": {
        "statement": sql, "compute_pool_id": env["FLINK_POOL_ID"],
        "properties": {"sql.current-catalog": env["FLINK_CATALOG"], "sql.current-database": env["FLINK_DATABASE"], **props}}}
    r = requests.post(base, json=body, auth=auth)
    if not r.ok:
        sys.exit(f"{r.status_code} {r.text}")
    is_insert = sql.lstrip().upper().startswith("INSERT")
    is_select = sql.lstrip().upper().startswith("SELECT")
    for _ in range(150):
        st = requests.get(f"{base}/{name}", auth=auth).json()
        phase = st["status"]["phase"]
        if phase in ("COMPLETED", "FAILED", "STOPPED"):
            break
        if phase == "RUNNING" and (is_insert or (is_select and not props.get("sql.snapshot.mode"))):
            break
        time.sleep(2)
    print(f"[{name}] {phase} {st['status'].get('detail', '')}", file=sys.stderr)
    return name, phase


def results(name, max_pages=20):
    url = f"{base}/{name}/results"
    for _ in range(max_pages):
        d = requests.get(url, auth=auth).json()
        for row in (d.get("results") or {}).get("data") or []:
            print(json.dumps(row.get("row")))
        url = d.get("metadata", {}).get("next")
        if not url:
            break
        time.sleep(0.5)


def statements(text):
    props = {}
    for stmt in re.split(r";\s*(?:\n|$)", re.sub(r"^--.*$", "", text, flags=re.M)):
        stmt = stmt.strip()
        if not stmt:
            continue
        m = re.fullmatch(r"SET\s+'([^']+)'\s*=\s*'([^']*)'", stmt, flags=re.I)
        if m:
            props[m.group(1)] = m.group(2)
        else:
            yield stmt, dict(props)


if __name__ == "__main__":
    if sys.argv[1] == "--stop":  # flink.py --stop NAME...
        for n in sys.argv[2:]:
            print(n, requests.delete(f"{base}/{n}", auth=auth).status_code)
        sys.exit(0)
    extra = {"sql.snapshot.mode": "now"} if "--snapshot" in sys.argv else {}
    for stmt, props in statements(pathlib.Path(sys.argv[1]).read_text()):
        n, phase = run(stmt, props={**props, **extra})
        if phase == "FAILED":
            sys.exit(1)
        if phase in ("COMPLETED", "RUNNING") and not stmt.upper().startswith("INSERT"):
            results(n)
