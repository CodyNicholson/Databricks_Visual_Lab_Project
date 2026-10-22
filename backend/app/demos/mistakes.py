"""Common mistakes that cost real time and money on Databricks, run for real: a skewed join,
row-by-row writes to a Delta table, and pulling a whole table to the driver."""

from __future__ import annotations

import requests
from fastapi import APIRouter
from pyspark.sql import functions as F

from .. import core
from ..lab import Demo, clamp
from ..seed import TRIPS

router = APIRouter(prefix="/api/demos/mistakes")
SCHEMA = "main.mistakes"


def task_records(stage: dict) -> list[int]:
    """Shuffle records each task of a stage read, from the Spark UI REST API."""
    app = core._app_id()
    tasks = requests.get(f"{core.SPARK_UI}/api/v1/applications/{app}/stages/{stage['stageId']}/0/taskList",
                         params={"length": 500}, timeout=5).json()
    return sorted((t.get("taskMetrics", {}).get("shuffleReadMetrics", {}).get("recordsRead", 0) for t in tasks), reverse=True)


@router.post("/skew")
def data_skew(hot_share: int = 80):
    share = clamp(hot_share, 10, 95)
    s = core.spark()
    with Demo("data-skew") as d:
        rows = 600_000
        # 16 input partitions: AQE splits a skewed partition along map outputs, so it needs several
        facts = s.range(0, rows, 1, 16).select(F.when(F.col("id") % 100 < share, F.lit(0)).otherwise(F.col("id") % 997 + 1).alias("customer_id"),
                                     (F.col("id") % 50).alias("amount"))
        customers = s.range(998).select(F.col("id").alias("customer_id"), F.concat(F.lit("c"), F.col("id")).alias("name"))
        d.command(f"orders: {rows:,} rows, {share}% of them for customer_id = 0 (one huge customer)")
        joined = facts.join(customers, "customer_id").groupBy("name").agg(F.sum("amount").alias("total"))
        settings = {"spark.sql.autoBroadcastJoinThreshold": "-1",  # force a shuffle join, as with two big tables
                    "spark.sql.shuffle.partitions": "16",
                    "spark.sql.adaptive.coalescePartitions.enabled": "false",
                    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
                    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "64KB",
                    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "16KB"}  # tiny, for lab-sized data
        runs = []
        try:
            for key, value in settings.items():
                s.conf.set(key, value)
            d.command("spark.conf.set(...)  # " + ", ".join(f"{k.replace('spark.sql.', '')}={v}" for k, v in settings.items()))
            for skew_join in ("false", "true"):
                s.conf.set("spark.sql.adaptive.skewJoin.enabled", skew_join)
                d.command(f'spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "{skew_join}")')
                before = core.max_job_id()
                result, ms = d.timed("orders.join(customers, 'customer_id').groupBy('name').agg(sum('amount')).collect()", joined.collect)
                stages = core.stages_for(core.wait_for_jobs(before))
                join_stage = max((st for st in stages if st["shuffleReadBytes"] > 0), key=lambda st: st["numTasks"] or 0)
                records = task_records(join_stage)
                median = records[len(records) // 2] if records else 0
                runs.append({"skewJoin": skew_join == "true", "ms": ms, "tasks": len(records), "records": records[:40],
                             "max": records[0] if records else 0, "median": median, "groups": len(result),
                             "total": sum(r["total"] for r in result)})
        finally:
            for key in (*settings, "spark.sql.adaptive.skewJoin.enabled"):
                s.conf.unset(key)
            d.command("spark.conf.unset(...)  # back to the cluster defaults")
        off, on = runs
        d.data.update(runs=runs, hotShare=share, rows=rows)
        d.check("Without skew handling one task does most of the work", off["max"] > 5 * max(off["median"], 1),
                f"busiest task {off['max']:,} records, median {off['median']:,}")
        d.check("AQE's skew join split the hot partition across more tasks", on["tasks"] > off["tasks"] and on["max"] * 3 < off["max"],
                f"{off['tasks']} -> {on['tasks']} tasks, busiest {off['max']:,} -> {on['max']:,} records")
        d.check("Both plans return the same answer", off["groups"] == on["groups"] and off["total"] == on["total"], f"{on['groups']} customers")
    return d.result()


@router.post("/row-by-row")
def row_by_row_writes(rows: int = 20):
    n = clamp(rows, 5, 40)
    s = core.spark()
    with Demo("row-by-row") as d:
        core.ensure_schema(d, "mistakes")
        results = {}
        for mode in ("one INSERT per row", "one batch"):
            name = "events_loop" if mode.startswith("one INSERT") else "events_batch"
            full, location = f"{SCHEMA}.{name}", core.ext_path(f"mistakes_{name}")
            core.drop_table(d, full, str(location))
            core.sql(d, f"CREATE TABLE {full} (id INT, kind STRING) USING DELTA LOCATION '{location}'")
            if mode.startswith("one INSERT"):
                d.command(f"for i in range({n}): spark.sql(f\"INSERT INTO {name} VALUES ({{i}}, 'click')\")")
                timings = []
                for i in range(n):
                    _, ms = d.timed(f"  INSERT INTO {name} VALUES ({i}, 'click')", lambda i=i: s.sql(f"INSERT INTO {full} VALUES ({i}, 'click')").collect())
                    timings.append(ms)
                d.commands[-n:] = d.commands[-n:][:3] + [f"  ... {n - 3} more INSERTs"] if n > 3 else d.commands[-n:]
            else:
                batch = s.createDataFrame([(i, "click") for i in range(n)], "id INT, kind STRING")
                _, ms = d.timed(f"spark.createDataFrame(rows).write.mode('append').saveAsTable('{name}')  # all {n} rows at once",
                                lambda: batch.write.mode("append").saveAsTable(full))
                timings = [ms]
            commits = core.delta_log(location)
            results[mode] = {"table": full, "ms": round(sum(timings), 1), "timings": timings, "commits": len(commits) - 1,
                             "files": len(core.data_files(location)), "rows": core.scalar(d, f"SELECT count(*) FROM {full}"), "location": location}
        loop, batch = results["one INSERT per row"], results["one batch"]
        metrics = core.sql(d, f"OPTIMIZE {loop['table']}")[0]["metrics"]
        loop["filesAfterOptimize"] = metrics.get("numFilesAdded")
        d.data.update(rows=n, results={k: {kk: vv for kk, vv in v.items() if kk != "location"} for k, v in results.items()})
        d.check("Both tables hold the same rows", loop["rows"] == batch["rows"] == n, f"{n} rows each")
        d.check("The loop made one commit and one tiny file per row", loop["commits"] == n and loop["files"] >= n,
                f"{loop['commits']} commits, {loop['files']} files vs {batch['commits']} commit, {batch['files']} file(s)")
        d.check("The batch write is many times faster", batch["ms"] * 3 < loop["ms"], f"{batch['ms']:.0f} ms vs {loop['ms']:.0f} ms")
        d.check("OPTIMIZE can compact the damage afterwards, but not undo the time spent", loop["filesAfterOptimize"] == 1,
                f"{loop['files']} -> {loop['filesAfterOptimize']} file")
    return d.result()


@router.post("/collect")
def collect_to_driver():
    s = core.spark()
    with Demo("collect-to-driver") as d:
        trips = s.read.table(TRIPS)
        everything, wrong_ms = d.timed(f"pdf = spark.read.table('{TRIPS}').toPandas()  # every row to the driver", trips.toPandas)
        wrong = (everything.groupby("pickup_zip")["fare_amount"].mean().round(2).sort_index())
        d.command("pdf.groupby('pickup_zip')['fare_amount'].mean()  # pandas on the driver, one machine")
        agg = trips.groupBy("pickup_zip").agg(F.round(F.avg("fare_amount"), 2).alias("avg_fare")).orderBy("pickup_zip")
        small, right_ms = d.timed("spark.read.table(...).groupBy('pickup_zip').agg(avg('fare_amount')).toPandas()  # aggregate on the cluster first", agg.toPandas)
        right = small.set_index("pickup_zip")["avg_fare"]
        wrong_bytes, right_bytes = int(everything.memory_usage(deep=True).sum()), int(small.memory_usage(deep=True).sum())
        d.data.update(wrong={"rows": len(everything), "bytes": wrong_bytes, "ms": wrong_ms},
                      right={"rows": len(small), "bytes": right_bytes, "ms": right_ms},
                      sample=[{"zip": int(z), "avg": float(v)} for z, v in right.head(8).items()])
        d.check("Same answer both ways", (wrong.round(2).values == right.values).all() if len(wrong) == len(right) else False, f"{len(right)} zips")
        d.check("Pulling the table moved every row to the driver", len(everything) == 100_000, f"{len(everything):,} rows")
        d.check("Aggregating first moved only the result", len(small) == 40 and right_bytes * 100 < wrong_bytes,
                f"{right_bytes:,} vs {wrong_bytes:,} bytes in driver memory")
    return d.result()
