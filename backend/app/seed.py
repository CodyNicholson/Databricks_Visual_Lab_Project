"""Idempotent startup seeding: the `main` and `samples` catalogs and a NYC-taxi-like sample table,
mirroring the `samples.nyctaxi.trips` dataset every Databricks workspace has."""

from __future__ import annotations

import logging
import threading
import time

from . import core
from .lab import Demo

log = logging.getLogger("seed")
TRIPS = "samples.nyctaxi.trips"
TRIPS_ROWS = 100_000

state = {"ready": False, "error": None, "steps": []}


def seed() -> None:
    d = Demo("seed")
    core.ensure_catalog("main", "Default catalog, like a Databricks workspace's main catalog")
    core.ensure_catalog("samples", "Read-only sample data, like Databricks' samples catalog")
    core.sql(d, "CREATE SCHEMA IF NOT EXISTS main.default")
    core.sql(d, "CREATE SCHEMA IF NOT EXISTS samples.nyctaxi")
    exists = core.uc("GET", f"/tables/{TRIPS}").status_code == 200
    if exists:
        try:
            exists = core.spark().read.table(TRIPS).count() == TRIPS_ROWS
        except Exception:  # registered in UC but its files are gone (e.g. the volume was reset)
            exists = False
        if not exists:
            # DROP TABLE would try to load the missing Delta log and fail; unregister it in UC instead
            core.uc("DELETE", f"/tables/{TRIPS}", d)
    if not exists:
        location = core.LAKEHOUSE / "samples" / "nyctaxi" / "trips"
        core.remove_dir(location)
        # Deterministic synthetic trips: same columns as Databricks' samples.nyctaxi.trips
        core.sql(d, f"""
            CREATE TABLE {TRIPS} USING DELTA LOCATION '{location}' AS
            SELECT
              TIMESTAMP '2016-02-01 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, (id * 37) % 2505600) AS tpep_pickup_datetime,
              TIMESTAMP '2016-02-01 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, (id * 37) % 2505600 + 300 + abs(hash(id, 9)) % 2400) AS tpep_dropoff_datetime,
              CAST(round(0.4 + abs(hash(id, 1)) % 1500 / 100.0, 2) AS DOUBLE) AS trip_distance,
              CAST(round(3.0 + (0.4 + abs(hash(id, 1)) % 1500 / 100.0) * 2.6 + abs(hash(id, 2)) % 400 / 100.0, 2) AS DOUBLE) AS fare_amount,
              CAST(10001 + abs(hash(id, 3)) % 40 AS INT) AS pickup_zip,
              CAST(10001 + abs(hash(id, 4)) % 40 AS INT) AS dropoff_zip
            FROM range({TRIPS_ROWS})
        """)
    core.sql(d, "USE main.default")
    state["steps"] = d.commands


def seed_in_background() -> None:
    def loop() -> None:
        delay = 2.0
        while not state["ready"]:
            try:
                seed()
                state.update(ready=True, error=None)
                log.info("lakehouse seeded")
            except Exception as exc:  # Spark or UC still starting: retry
                state["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
                log.warning("seed failed, retrying: %s", state["error"])
                core.reset_session()
                time.sleep(delay)
                delay = min(delay * 1.5, 15)

    threading.Thread(target=loop, name="seed", daemon=True).start()
