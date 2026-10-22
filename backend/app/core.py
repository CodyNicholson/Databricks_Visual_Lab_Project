"""Connections to the lab's lakehouse: the Spark Connect server, Unity Catalog, MLflow and the
shared /lakehouse volume, plus small helpers the demos share (SQL with timings, resets, the
Delta transaction log reader, Spark UI metrics)."""

from __future__ import annotations

import json
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any

import requests
from pyspark.sql import DataFrame, SparkSession

from .lab import Demo

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark:15002")
SPARK_UI = os.environ.get("SPARK_UI_URL", "http://spark:4040")
UC_URL = os.environ.get("UC_URL", "http://unitycatalog:8080")
UC_API = f"{UC_URL}/api/2.1/unity-catalog"
MLFLOW_URL = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
LAKEHOUSE = Path(os.environ.get("LAKEHOUSE_DIR", "/lakehouse"))
EXT = LAKEHOUSE / "ext"  # external table locations
CATALOG = "main"

_session: SparkSession | None = None
_session_lock = threading.Lock()

# One demo at a time: demos reset and rewrite shared tables, and the single-node "cluster" is
# the bottleneck anyway. Visitors queue for up to LOCK_WAIT_S seconds.
DEMO_LOCK = threading.Lock()
LOCK_WAIT_S = 75


def spark() -> SparkSession:
    """The shared Spark Connect session (Databricks Connect works the same way: a thin client
    that sends query plans to a remote cluster)."""
    global _session
    with _session_lock:
        if _session is None or _session.is_stopped:
            _session = SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
            try:  # OSS Spark starts a UC catalog with no current schema; Databricks starts in `default`
                _session.sql("USE main.default").collect()
            except Exception:
                pass  # before seeding the schema may not exist yet
        return _session


def reset_session() -> None:
    """Drop the session (after a server restart the old one is gone)."""
    global _session
    with _session_lock:
        _session = None


def sql(d: Demo, statement: str, args: dict[str, Any] | None = None, show: str | None = None, **values: Any) -> list[dict[str, Any]]:
    """Run SQL on the cluster, record it with its timing, and return the rows as dicts.

    `values` fill `{name}` placeholders through PySpark's own formatter, which renders each value
    as a properly quoted SQL literal (safe against injection). Named `:markers` via `args` are
    broken with the Delta 4.4.1 extension on Spark 4.2.0 (UNBOUND_SQL_PARAMETER); see the SQL demo.
    """
    started = time.perf_counter()
    if args:
        rows = spark().sql(statement, args=args).collect()
    else:
        rows = spark().sql(statement, **values).collect()
    ms = round((time.perf_counter() - started) * 1000, 1)
    text = " ".join(statement.split())
    d.command(f"SQL> {show or text}  -- {ms:g} ms, {len(rows)} row(s)")
    return [row.asDict(recursive=True) for row in rows]


def collect(d: Demo, label: str, df: DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    """Run a DataFrame action and record it."""
    started = time.perf_counter()
    rows = (df.limit(limit) if limit else df).collect()
    ms = round((time.perf_counter() - started) * 1000, 1)
    d.command(f"{label}.collect()  -- {ms:g} ms, {len(rows)} row(s)")
    return [row.asDict(recursive=True) for row in rows]


def scalar(d: Demo, statement: str) -> Any:
    rows = sql(d, statement)
    return next(iter(rows[0].values())) if rows else None


def ext_path(name: str) -> Path:
    return EXT / name


def drop_table(d: Demo, full_name: str, location: str | None = None) -> None:
    """DROP TABLE and, for external tables, delete the files too (DROP keeps them on purpose)."""
    sql(d, f"DROP TABLE IF EXISTS {full_name}")
    if location:
        remove_dir(Path(location))


def remove_dir(path: Path) -> None:
    if path.exists() and LAKEHOUSE in path.parents:
        shutil.rmtree(path, ignore_errors=True)


def ensure_schema(d: Demo, schema: str, catalog: str = CATALOG) -> None:
    sql(d, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


# ---- Unity Catalog REST API ------------------------------------------------------------


def uc(method: str, path: str, d: Demo | None = None, **kwargs: Any) -> requests.Response:
    """Call the Unity Catalog REST API (the same /api/2.1/unity-catalog API Databricks serves)."""
    started = time.perf_counter()
    response = requests.request(method, f"{UC_API}{path}", timeout=20, **kwargs)
    if d is not None:
        ms = round((time.perf_counter() - started) * 1000, 1)
        query = kwargs.get("params")
        suffix = "?" + "&".join(f"{k}={v}" for k, v in query.items()) if query else ""
        d.command(f"REST> {method} /api/2.1/unity-catalog{path}{suffix} -> {response.status_code}  -- {ms:g} ms")
    return response


def uc_json(method: str, path: str, d: Demo | None = None, **kwargs: Any) -> dict[str, Any]:
    response = uc(method, path, d, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(f"Unity Catalog {method} {path} failed with {response.status_code}: {response.text[:300]}")
    return response.json() if response.content else {}


def ensure_catalog(name: str, comment: str) -> None:
    if uc("GET", f"/catalogs/{name}").status_code == 404:
        uc_json("POST", "/catalogs", json={"name": name, "comment": comment, "storage_root": f"file://{LAKEHOUSE}/{name}"})


# ---- Delta transaction log (read straight from the shared volume) ------------------------


def delta_log(location: str | Path) -> list[dict[str, Any]]:
    """Every commit in a table's _delta_log, oldest first, summarised action by action."""
    log_dir = Path(str(location).replace("file:", "")) / "_delta_log"
    commits = []
    for path in sorted(log_dir.glob("*.json")):
        if not path.stem.isdigit():
            continue
        summary: dict[str, Any] = {"version": int(path.stem), "file": path.name, "bytes": path.stat().st_size,
                                   "add": [], "remove": [], "cdc": 0, "metaData": False, "protocol": None, "operation": None}
        for line in path.read_text().splitlines():
            action = json.loads(line)
            if "commitInfo" in action:
                info = action["commitInfo"]
                summary["operation"] = info.get("operation")
                summary["operationMetrics"] = info.get("operationMetrics", {})
                summary["isBlindAppend"] = info.get("isBlindAppend")
            elif "add" in action:
                add = action["add"]
                stats = json.loads(add.get("stats") or "{}")
                summary["add"].append({"path": add["path"].split("/")[-1], "size": add.get("size"),
                                       "numRecords": stats.get("numRecords"), "minValues": stats.get("minValues"),
                                       "maxValues": stats.get("maxValues"), "dv": bool(add.get("deletionVector"))})
            elif "remove" in action:
                summary["remove"].append({"path": action["remove"]["path"].split("/")[-1]})
            elif "cdc" in action:
                summary["cdc"] += 1
            elif "metaData" in action:
                summary["metaData"] = True
            elif "protocol" in action:
                summary["protocol"] = action["protocol"]
        commits.append(summary)
    return commits


def data_files(location: str | Path) -> list[Path]:
    """Parquet data files physically on disk (live and tombstoned), excluding the log."""
    root = Path(str(location).replace("file:", ""))
    return [p for p in root.rglob("*.parquet") if "_delta_log" not in p.parts]


def table_location(d: Demo, full_name: str) -> str:
    detail = sql(d, f"DESCRIBE DETAIL {full_name}")[0]
    return str(detail["location"]).replace("file:", "")


# ---- Spark UI REST API: real job / stage metrics from the cluster ------------------------


def _app_id() -> str:
    return requests.get(f"{SPARK_UI}/api/v1/applications", timeout=5).json()[0]["id"]


def spark_jobs() -> list[dict[str, Any]]:
    return requests.get(f"{SPARK_UI}/api/v1/applications/{_app_id()}/jobs", timeout=5).json()


def max_job_id() -> int:
    jobs = spark_jobs()
    return max((j["jobId"] for j in jobs), default=-1)


def jobs_since(job_id: int) -> list[dict[str, Any]]:
    return sorted((j for j in spark_jobs() if j["jobId"] > job_id), key=lambda j: j["jobId"])


def stages_for(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Stage details (tasks, shuffle bytes) for the given jobs, skipped stages excluded."""
    app = _app_id()
    ids = sorted({s for j in jobs for s in j.get("stageIds", [])})
    stages = []
    for sid in ids:
        attempts = None
        for _ in range(8):  # the UI can answer "unknown stage" (plain text) until its listener catches up
            response = requests.get(f"{SPARK_UI}/api/v1/applications/{app}/stages/{sid}", timeout=5)
            if response.ok:
                attempts = response.json()
                break
            time.sleep(0.25)
        if not attempts or attempts[0].get("status") == "SKIPPED":
            continue
        s = attempts[0]
        stages.append({"stageId": sid, "name": s.get("name", "")[:60], "numTasks": s.get("numTasks"),
                       "shuffleReadBytes": s.get("shuffleReadBytes", 0), "shuffleWriteBytes": s.get("shuffleWriteBytes", 0),
                       "inputRecords": s.get("inputRecords", 0), "status": s.get("status")})
    return stages


def wait_for_jobs(after: int, timeout: float = 6.0) -> list[dict[str, Any]]:
    """The Spark UI listener is asynchronous: wait until the jobs just run are all recorded and
    finished (the job list stops changing for a few polls)."""
    deadline = time.time() + timeout
    jobs, stable = jobs_since(after), 0
    while time.time() < deadline and stable < 4:
        time.sleep(0.25)
        latest = jobs_since(after)
        settled = latest and not any(j.get("status") == "RUNNING" for j in latest)
        stable = stable + 1 if settled and len(latest) == len(jobs) else 0
        jobs = latest
    return jobs
