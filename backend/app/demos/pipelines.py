"""Lakeflow Declarative Pipelines, through their open-source core: Spark Declarative Pipelines
(donated by Databricks to Apache Spark, shipped in Spark 4.1+). The backend runs the real
`spark-pipelines` CLI against the Spark Connect server and parses its event log."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from fastapi import APIRouter

from .. import core
from ..lab import Demo, clamp

router = APIRouter(prefix="/api/demos/pipelines")
PROJECTS = Path(__file__).resolve().parent.parent / "pipelines"
LANDING = core.LAKEHOUSE / "landing" / "sdp_orders"
EVENT = re.compile(r"^(\S+ \S+): Flow '?([\w.]+)'? (?:is|has) ([A-Z_]+)")
STORES = ["north", "south", "east"]


def run_pipeline(d: Demo, project: str, command: str = "run", extra: list[str] | None = None, timeout: int = 85) -> dict:
    """Run `spark-pipelines <command>` (same CLI, as a module) and collect its flow events."""
    spec = PROJECTS / project / "spark-pipeline.yml"
    args = [sys.executable, "-m", "pyspark.pipelines.cli", command, "--spec", str(spec), *(extra or [])]
    d.command(f"spark-pipelines {command} --spec {project}/spark-pipeline.yml {' '.join(extra or [])} --remote {core.SPARK_REMOTE}".replace("  ", " "))
    started = time.perf_counter()
    proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout, env={**os.environ, "SPARK_REMOTE": core.SPARK_REMOTE})
    ms = round((time.perf_counter() - started) * 1000, 1)
    events, log = [], []
    for line in proc.stdout.splitlines():
        log.append(line)
        m = EVENT.match(line)
        if m:
            events.append({"time": m.group(1)[11:], "flow": m.group(2).split(".")[-1], "state": m.group(3)})
    error = None
    if proc.returncode != 0:
        # The flow-level "Error: [...]" line is the useful one; fall back to the exception text
        flow_errors = [line[7:] for line in log if line.startswith("Error: ")]
        lines = [line for line in proc.stderr.splitlines() if line.strip() and not line.startswith((" ", "\t", "Traceback"))]
        exc = next((i for i, line in enumerate(lines) if "Exception" in line), None)
        fallback = " ".join(lines[exc:exc + 3]) if exc is not None else (lines[-1] if lines else "failed")
        error = (flow_errors[0] if flow_errors else fallback)[:500]
    for line in log[-12:]:
        d.command(f"  {line[:200]}")
    d.command(f"  exit code {proc.returncode} in {ms:g} ms")
    if error:
        d.command(f"  -> {error}")
    return {"returncode": proc.returncode, "events": events, "error": error, "ms": ms}


def write_orders(start: int, count: int) -> str:
    LANDING.mkdir(parents=True, exist_ok=True)
    path = LANDING / f"orders_{start:05d}.json"
    lines = []
    for i in range(start, start + count):
        amount = -5.0 if i % 9 == 4 else round(5 + (i * 37 % 120) + (i % 4) * 0.25, 2)  # every 9th order is invalid
        lines.append(json.dumps({"order_id": i, "customer": f"c{i % 6}", "store": STORES[i % 3], "amount": amount}))
    path.write_text("\n".join(lines) + "\n")
    return path.name


def count(d: Demo, table: str) -> int:
    try:
        return core.scalar(d, f"SELECT count(*) FROM {table}") or 0
    except Exception:
        return 0


@router.post("/run")
def declarative_pipeline(new_orders: int = 6, reset: bool = False, mistake: bool = False):
    n = clamp(new_orders, 1, 30)
    with Demo("declarative-pipeline") as d:
        core.ensure_schema(d, "sdp")
        tables = ["orders_raw", "orders_clean", "revenue_by_store", "top_customers"]
        if reset or not LANDING.exists():
            for t in tables:
                core.drop_table(d, f"main.sdp.{t}")
            for p in (LANDING, core.LAKEHOUSE / "pipelines" / "orders"):
                core.remove_dir(p)
            d.command("reset: dropped the pipeline's tables, its checkpoints and the landing folder")
        before_raw = count(d, "main.sdp.orders_raw")
        start = 1 + sum(1 for _ in LANDING.glob("*.json")) * 1000 if LANDING.exists() else 1
        name = write_orders(start, n)
        d.command(f"landing/sdp_orders/ <- {name} ({n} orders)")
        files = sorted(p.name for p in LANDING.glob("*.json"))
        expected_total = sum(len(p.read_text().splitlines()) for p in LANDING.glob("*.json"))
        run = run_pipeline(d, "orders")
        counts = {t: count(d, f"main.sdp.{t}") for t in tables}
        revenue = core.sql(d, "SELECT * FROM main.sdp.revenue_by_store ORDER BY store") if run["returncode"] == 0 else []
        flows = {}
        for e in run["events"]:
            flows.setdefault(e["flow"], []).append(e["state"])
        d.data.update(run=run, flows=flows, counts=counts, revenue=revenue, files=files, newOrders=n, processedThisRun=counts["orders_raw"] - before_raw,
                      graph=[["landing JSON", "orders_raw"], ["orders_raw", "orders_clean"], ["orders_clean", "revenue_by_store"], ["orders_clean", "top_customers"]])
        d.check("The pipeline run succeeded", run["returncode"] == 0, run["error"] or f"{run['ms']:.0f} ms")
        d.check("Every flow reached COMPLETED", flows and all(states[-1] == "COMPLETED" for states in flows.values()),
                {f: s[-1] for f, s in flows.items()})
        d.check("The streaming table read only the new file this run", counts["orders_raw"] - before_raw == n and counts["orders_raw"] == expected_total,
                f"+{counts['orders_raw'] - before_raw} rows, {counts['orders_raw']} total")
        valid = sum(1 for p in LANDING.glob("*.json") for line in p.read_text().splitlines() if json.loads(line)["amount"] > 0)
        d.check("orders_clean dropped invalid amounts", counts["orders_clean"] == valid, f"{counts['orders_clean']} of {counts['orders_raw']}")
        d.check("The materialized view agrees with its input", sum(r["orders"] for r in revenue) == counts["orders_clean"], f"{sum(r['orders'] for r in revenue)} orders")
        if mistake:
            core.sql(d, "DROP SCHEMA IF EXISTS main.sdp_broken CASCADE")
            no_schema = run_pipeline(d, "broken", "dry-run")
            d.check("The target schema must exist before the pipeline runs", no_schema["error"] and "SCHEMA_NOT_FOUND" in no_schema["error"], no_schema["error"])
            core.ensure_schema(d, "sdp_broken")
            broken = run_pipeline(d, "broken", "dry-run")
            d.check("dry-run catches a read of a dataset nobody defines, before any data moves",
                    broken["returncode"] != 0 and broken["error"] is not None and "orders_cleaned" in broken["error"], broken["error"])
            d.data["mistake"] = {"noSchema": no_schema, "undefined": broken}
    return d.result()


@router.post("/cdc")
def change_data_capture(mistake: bool = False):
    s = core.spark()
    with Demo("cdc-scd1") as d:
        core.ensure_schema(d, "sdp_cdc")
        feed, target = "main.sdp_cdc.cdc_feed", "main.sdp_cdc.customers_scd1"
        for t in (feed, target):
            core.drop_table(d, t)
        core.sql(d, f"CREATE TABLE {feed} (id INT, name STRING, city STRING, op STRING, seq INT) USING DELTA")
        core.sql(d, f"CREATE TABLE {target} (id INT, name STRING, city STRING, seq INT) USING DELTA")
        batches = [
            [(1, "Ana", "Lisbon", "UPSERT", 1), (2, "Ben", "Porto", "UPSERT", 2), (3, "Cy", "Faro", "UPSERT", 3), (1, "Ana", "Braga", "UPSERT", 4)],
            [(2, None, None, "DELETE", 6), (4, "Dee", "Evora", "UPSERT", 7), (1, "Ana", "Lisbon (stale)", "UPSERT", 2)],
        ]
        apply = f"""MERGE INTO {target} AS t
            USING (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY seq DESC) AS rn FROM batch) WHERE rn = 1) AS s
            ON t.id = s.id
            WHEN MATCHED AND s.seq > t.seq AND s.op = 'DELETE' THEN DELETE
            WHEN MATCHED AND s.seq > t.seq THEN UPDATE SET t.name = s.name, t.city = s.city, t.seq = s.seq
            WHEN NOT MATCHED AND s.op != 'DELETE' THEN INSERT (id, name, city, seq) VALUES (s.id, s.name, s.city, s.seq)"""
        states = []
        for i, events in enumerate(batches, 1):
            df = s.createDataFrame(events, "id INT, name STRING, city STRING, op STRING, seq INT")
            df.write.mode("append").saveAsTable(feed)
            df.createOrReplaceTempView("batch")
            d.command(f"batch {i}: {len(events)} change events appended to {feed}")
            metrics = core.sql(d, apply, show="MERGE INTO customers_scd1 USING (latest event per id) ON id WHEN MATCHED AND newer: DELETE / UPDATE WHEN NOT MATCHED: INSERT")[0]
            states.append({"batch": i, "events": [dict(zip(["id", "name", "city", "op", "seq"], e)) for e in events],
                           "metrics": metrics, "table": core.sql(d, f"SELECT * FROM {target} ORDER BY id")})
        final = {r["id"]: r for r in states[-1]["table"]}
        d.data.update(states=states)
        d.check("Within a batch only the newest event per key is applied (Ana -> Braga)", states[0]["table"][0]["city"] == "Braga", states[0]["table"][0])
        d.check("A late, out-of-order event (seq 2) does not overwrite newer data", final[1]["city"] == "Braga", final[1])
        d.check("DELETE events remove the row", 2 not in final, sorted(final))
        d.check("New keys are inserted", 4 in final and final[4]["city"] == "Evora", sorted(final))
        if mistake:
            core.drop_table(d, "main.sdp_cdc.customers")
            core.remove_dir(core.LAKEHOUSE / "pipelines" / "autocdc")
            attempt = run_pipeline(d, "autocdc")
            d.check("create_auto_cdc_flow fails on Spark 4.2 + Delta 4.4 (it needs DSv2 row-level operations)",
                    attempt["returncode"] != 0 and attempt["error"] and "AUTOCDC" in attempt["error"], attempt["error"])
            d.data["mistake"] = attempt
    return d.result()
