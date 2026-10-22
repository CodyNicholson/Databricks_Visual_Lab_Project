"""Spark on Databricks compute: Spark Connect, lazy evaluation, SQL vs DataFrames, shuffles and
adaptive query execution, and UDF performance."""

from __future__ import annotations

import contextlib
import io
import time

import numpy as np
import pandas as pd
import pyspark
from fastapi import APIRouter, Query
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import StringType

from .. import core
from ..lab import Demo, clamp
from ..seed import TRIPS

router = APIRouter(prefix="/api/demos/basics")


def explain(df, mode: str = "formatted") -> str:
    """df.explain() prints; capture it so the page can show the plan."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode=mode)
    return buffer.getvalue()


@router.post("/connect")
def connect_session(rows: int = 10_000_000, mistake: bool = False):
    n = clamp(rows, 1_000, 50_000_000)
    s = core.spark()
    with Demo("connect-session") as d:
        d.command(f'spark = SparkSession.builder.remote("{core.SPARK_REMOTE}").getOrCreate()')
        df = s.range(n).selectExpr("sum(id) AS total", "count(*) AS row_count")
        d.command(f'df = spark.range({n:,}).selectExpr("sum(id) AS total", "count(*) AS row_count")  -- builds a plan, nothing runs yet')
        try:
            plan_bytes = df._plan.to_proto(s.client).ByteSize()
        except Exception:  # private API: the demo still works without the number
            plan_bytes = None
        before = core.max_job_id()
        result, ms = d.timed("df.collect()  # the plan goes to the server; only the 1-row answer comes back", df.collect)
        jobs = core.wait_for_jobs(before)
        total, count = result[0]["total"], result[0]["row_count"]
        conf = {k: s.conf.get(k) for k in ("spark.master", "spark.sql.defaultCatalog", "spark.sql.sources.default")}
        d.command(f"spark.version -> {s.version}; spark.conf.get(...) -> {conf}")
        d.data.update(serverVersion=s.version, clientVersion=pyspark.__version__, sessionId=s.session_id,
                      remote=core.SPARK_REMOTE, rows=n, total=total, planBytes=plan_bytes, rawBytes=n * 8,
                      resultBytes=16, serverMs=ms, jobs=[{"jobId": j["jobId"], "tasks": j.get("numTasks")} for j in jobs], conf=conf)
        d.check("The server runs Spark 4.2 (the open-source engine under Databricks Runtime)", s.version.startswith("4.2"), s.version)
        d.check("The cluster computed sum(0..n-1) correctly", total == n * (n - 1) // 2 and count == n, f"{total:,}")
        if plan_bytes:
            d.check("Only the small plan travelled to the cluster, not the data", plan_bytes < n * 8 / 100,
                    f"plan {plan_bytes} bytes vs {n * 8:,} bytes of data")
        d.check("The action ran as Spark job(s) on the server", len(jobs) >= 1, f"{len(jobs)} job(s)")
        if mistake:
            _, err1 = d.attempt("spark.sparkContext  # classic API, needs a local JVM", lambda: s.sparkContext)
            _, err2 = d.attempt("spark.range(3).rdd  # RDDs are not part of Spark Connect", lambda: s.range(3).rdd)
            _, err3 = d.attempt('spark.conf.set("spark.sql.warehouse.dir", "/x")  # a static config',
                                lambda: s.conf.set("spark.sql.warehouse.dir", "/x"))
            d.check("sparkContext is unavailable over Spark Connect", err1 and "JVM_ATTRIBUTE_NOT_SUPPORTED" in err1, err1)
            d.check("RDD APIs are unavailable over Spark Connect", err2 and "NOT_IMPLEMENTED" in err2, err2)
            d.check("Static configs cannot change at runtime", err3 and "CANNOT_MODIFY_STATIC_CONFIG" in err3, err3)
            fixed = s.range(5).select((F.col("id") * 2).alias("doubled")).agg(F.sum("doubled")).collect()[0][0]
            d.command("spark.range(5).select(col('id') * 2).agg(sum(...))  # the DataFrame way instead of rdd.map")
            d.check("The DataFrame API does what the RDD code wanted", fixed == 20, fixed)
            d.data["errors"] = [e for e in (err1, err2, err3) if e]
    return d.result()


@router.post("/lazy")
def lazy_evaluation(min_distance: float = 5.0, mistake: bool = False):
    dist = max(0.0, min(15.0, min_distance))
    s = core.spark()
    with Demo("lazy-evaluation") as d:
        steps = []
        start_job = core.max_job_id()
        builders = [
            ("trips = spark.read.table('samples.nyctaxi.trips')", "transformation", lambda _: s.read.table(TRIPS)),
            (f"long = trips.where(col('trip_distance') > {dist})", "transformation", lambda df: df.where(F.col("trip_distance") > dist)),
            ("cols = long.select('pickup_zip', 'fare_amount')", "transformation", lambda df: df.select("pickup_zip", "fare_amount")),
            ("agg = cols.groupBy('pickup_zip').agg(avg('fare_amount'), count('*'))", "transformation",
             lambda df: df.groupBy("pickup_zip").agg(F.round(F.avg("fare_amount"), 2).alias("avg_fare"), F.count("*").alias("trips"))),
            ("top = agg.orderBy(desc('trips')).limit(5)", "transformation", lambda df: df.orderBy(F.desc("trips")).limit(5)),
        ]
        df = None
        for text, kind, build in builders:
            t0 = time.perf_counter()
            df = build(df)
            ms = round((time.perf_counter() - t0) * 1000, 2)
            d.command(f"{text}  -- {ms:g} ms, no job")
            steps.append({"code": text, "kind": kind, "ms": ms, "jobs": 0})
        jobs_after_transforms = core.jobs_since(start_job)
        plan = explain(df)
        d.command("top.explain(mode='formatted')")
        before = core.max_job_id()
        rows, ms = d.timed("top.collect()  # the action: now Spark plans, optimizes and runs it", df.collect)
        jobs = core.wait_for_jobs(before)
        steps.append({"code": "top.collect()", "kind": "action", "ms": ms, "jobs": len(jobs)})
        scan_line = next((line for line in plan.splitlines() if "ReadSchema" in line), "")
        pushed = next((line for line in plan.splitlines() if "PushedFilters" in line), "")
        d.data.update(steps=steps, plan=plan, readSchema=scan_line.strip(), pushedFilters=pushed.strip(),
                      rows=[r.asDict() for r in rows], minDistance=dist)
        d.check("Transformations ran no Spark jobs (they only build a plan)", len(jobs_after_transforms) == 0, f"{len(jobs_after_transforms)} jobs")
        d.check("The action ran Spark job(s)", len(jobs) >= 1, f"{len(jobs)} job(s): " + ", ".join(str(j["jobId"]) for j in jobs))
        d.check("Column pruning: the scan reads only the 3 columns used", "dropoff_zip" not in scan_line and "pickup_zip" in scan_line, scan_line.strip()[:160])
        d.check("Predicate pushdown: the filter reached the Delta scan", "trip_distance" in pushed, pushed.strip()[:160])
        if mistake:
            # The wrong way: reuse an expensive DataFrame for several actions without caching
            s.catalog.clearCache()
            expensive = s.read.table(TRIPS).where(F.col("trip_distance") > dist).withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance"))
            runs = []
            actions = [("count()", lambda f: f.count()),
                       ("agg(avg('fare_per_mile'))", lambda f: f.agg(F.avg("fare_per_mile")).collect())]
            for label in ("no cache", "cached"):
                frame = expensive if label == "no cache" else expensive.cache()  # cache() only once the uncached runs are done
                for i, (action, run_action) in enumerate(actions):
                    b = core.max_job_id()
                    name = "expensive" if label == "no cache" else "expensive.cache()"
                    _, t = d.timed(f"{name}.{action}", lambda: run_action(frame))
                    stages = core.stages_for(core.wait_for_jobs(b))
                    scanned = sum(st["inputRecords"] for st in stages)
                    runs.append({"label": label, "run": i + 1, "action": action, "ms": t, "inputRecords": scanned})
            cached_plan = explain(expensive, "simple")
            s.catalog.clearCache()
            d.command("expensive.unpersist()  # free the executors' memory when done")
            d.data["cacheRuns"] = runs
            nc = [r for r in runs if r["label"] == "no cache"]
            d.check("Without cache, every action re-reads the source", all(r["inputRecords"] > 0 for r in nc),
                    ", ".join(f"run {r['run']}: {r['inputRecords']:,} rows read" for r in nc))
            cached = [r for r in runs if r["label"] == "cached"]
            d.check("With cache, the plan reads from memory (InMemoryRelation)", "InMemory" in cached_plan, "InMemoryRelation in plan")
            d.check("With cache, the second action no longer scans the table", cached[1]["inputRecords"] < nc[1]["inputRecords"],
                    f"{cached[1]['inputRecords']:,} vs {nc[1]['inputRecords']:,} rows read")
    return d.result()


@router.post("/sql")
def sql_and_dataframes(borough: str = Query("Manhattan", max_length=60), mistake: bool = False):
    s = core.spark()
    with Demo("sql-dataframes") as d:
        core.ensure_schema(d, "basics")
        core.sql(d, "CREATE TABLE IF NOT EXISTS main.basics.zones (zip INT, borough STRING) USING DELTA")
        if core.scalar(d, "SELECT count(*) FROM main.basics.zones") == 0:
            core.sql(d, "INSERT INTO main.basics.zones SELECT CAST(10001 + id AS INT), element_at(array('Manhattan','Brooklyn','Queens','Bronx'), CAST(id % 4 + 1 AS INT)) FROM range(40)")
        query = """SELECT z.borough, count(*) AS trips, round(avg(t.fare_amount), 2) AS avg_fare
                   FROM samples.nyctaxi.trips t JOIN main.basics.zones z ON t.pickup_zip = z.zip
                   WHERE z.borough = {borough} GROUP BY z.borough"""
        via_sql = core.sql(d, query, borough=borough, show=" ".join(query.split()) + f"  (borough={borough!r} passed as a value)")
        trips, zones = s.read.table(TRIPS), s.read.table("main.basics.zones")
        df = (trips.join(zones, trips.pickup_zip == zones.zip).where(zones.borough == borough)
              .groupBy("borough").agg(F.count("*").alias("trips"), F.round(F.avg("fare_amount"), 2).alias("avg_fare")))
        via_df = core.collect(d, "trips.join(zones, ...).where(zones.borough == borough).groupBy('borough').agg(...)", df)
        s.read.table(TRIPS).createOrReplaceTempView("trips_v")
        d.command("spark.read.table('samples.nyctaxi.trips').createOrReplaceTempView('trips_v')")
        view_count = core.scalar(d, "SELECT count(*) FROM trips_v")
        sql_plan, df_plan = explain(s.sql(query, borough=borough), "simple"), explain(df, "simple")
        d.data.update(sql=via_sql, dataframe=via_df, borough=borough, viewRows=view_count, sqlPlan=sql_plan, dfPlan=df_plan)
        d.check("SQL and the DataFrame API return the same answer", via_sql == via_df, via_sql)
        d.check("A DataFrame registered as a temp view is queryable from SQL", view_count == 100_000, f"{view_count:,} rows")
        d.check("Both compile to the same kind of join", ("BroadcastHashJoin" in sql_plan) == ("BroadcastHashJoin" in df_plan), "same physical join")
        if mistake:
            attack = "Queens' OR '1'='1"
            unsafe = f"SELECT count(*) AS n FROM main.basics.zones WHERE borough = '{attack}'"
            wrong = core.sql(d, unsafe, show=f"f\"... WHERE borough = '{{user_input}}'\"  with user_input = {attack!r}")
            right = core.sql(d, "SELECT count(*) AS n FROM main.basics.zones WHERE borough = {b}", b=attack,
                             show=f'spark.sql("... WHERE borough = {{b}}", b=user_input)')
            via_filter = s.read.table("main.basics.zones").where(F.col("borough") == attack).count()
            d.command("zones.where(col('borough') == user_input).count()")
            _, marker_error = d.attempt("spark.sql('... WHERE borough = :b', args={'b': user_input})  # named parameter markers",
                                        lambda: s.sql("SELECT count(*) FROM main.basics.zones WHERE borough = :b", args={"b": attack}).collect())
            d.data["injection"] = {"input": attack, "unsafeRows": wrong[0]["n"], "safeRows": right[0]["n"],
                                   "filterRows": via_filter, "markerError": marker_error}
            d.check("String formatting lets the input rewrite the query (all 40 zones match)", wrong[0]["n"] == 40, f"{wrong[0]['n']} rows")
            d.check("Passing the input as a value treats it as data (0 matches)", right[0]["n"] == 0 and via_filter == 0,
                    f"spark.sql(..., b=...) -> {right[0]['n']}, DataFrame filter -> {via_filter}")
            d.check("Named :markers fail with the Delta 4.4.1 extension on Spark 4.2.0 (known incompatibility)",
                    marker_error is not None and "UNBOUND_SQL_PARAMETER" in marker_error, marker_error)
    return d.result()


@router.post("/shuffle")
def shuffle_and_aqe(partitions: int = 200):
    parts = clamp(partitions, 8, 400)
    s = core.spark()
    with Demo("shuffle-aqe") as d:
        agg = s.read.table(TRIPS).groupBy("pickup_zip").agg(F.sum("fare_amount").alias("revenue"))
        runs = []
        try:
            for aqe in (False, True):
                s.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
                s.conf.set("spark.sql.shuffle.partitions", str(parts))
                d.command(f'spark.conf.set("spark.sql.adaptive.enabled", "{str(aqe).lower()}"); spark.conf.set("spark.sql.shuffle.partitions", "{parts}")')
                before = core.max_job_id()
                rows, ms = d.timed("trips.groupBy('pickup_zip').agg(sum('fare_amount')).collect()", agg.collect)
                stages = core.stages_for(core.wait_for_jobs(before))
                reduce_tasks = [st["numTasks"] for st in stages if st["shuffleReadBytes"] > 0]
                runs.append({"aqe": aqe, "ms": ms, "groups": len(rows), "stages": stages,
                             "reduceTasks": max(reduce_tasks) if reduce_tasks else 0,
                             "shuffleBytes": sum(st["shuffleWriteBytes"] for st in stages)})
            s.conf.set("spark.sql.adaptive.enabled", "true")
            zones = s.read.table("main.basics.zones") if s.catalog.tableExists("main.basics.zones") else s.range(40).select((F.col("id") + 10001).cast("int").alias("zip"))
            join = s.read.table(TRIPS).join(zones, F.col("pickup_zip") == F.col("zip")).groupBy("zip").count()
            joins = []
            for threshold in ("10MB", "-1"):
                s.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
                d.command(f'spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "{threshold}")')
                plan = explain(join, "simple")
                strategy = "BroadcastHashJoin" if "BroadcastHashJoin" in plan else "SortMergeJoin" if "SortMergeJoin" in plan else "other"
                _, ms = d.timed("trips.join(zones, ...).groupBy('zip').count().collect()", join.collect)
                joins.append({"threshold": threshold, "strategy": strategy, "ms": ms})
        finally:
            for key in ("spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions", "spark.sql.autoBroadcastJoinThreshold"):
                s.conf.unset(key)
            d.command("spark.conf.unset(...)  # back to the cluster defaults")
        off, on = runs
        d.data.update(partitions=parts, runs=runs, joins=joins)
        d.check(f"Without AQE the shuffle uses all {parts} partitions", off["reduceTasks"] == parts, f"{off['reduceTasks']} reduce tasks")
        d.check("AQE coalesced the small shuffle into fewer partitions", 0 < on["reduceTasks"] < parts, f"{on['reduceTasks']} reduce tasks")
        d.check("Both runs produced the same 40 groups", off["groups"] == on["groups"] == 40, f"{on['groups']} groups")
        d.check("A small dimension table is broadcast (no shuffle of the big side)", joins[0]["strategy"] == "BroadcastHashJoin", joins[0]["strategy"])
        d.check("Disabling broadcast falls back to a sort-merge join", joins[1]["strategy"] == "SortMergeJoin", joins[1]["strategy"])
    return d.result()


def bucket_udfs():
    """Python UDFs, defined once a Spark Connect session exists (their return types need it)."""

    def bucket(fare):
        if fare is None:
            return None
        return "high" if fare >= 30 else "mid" if fare >= 15 else "low"

    pickled = udf(bucket, StringType(), useArrow=False)
    arrow = udf(bucket, StringType(), useArrow=True)

    @pandas_udf(StringType())
    def bucket_pandas(fare: pd.Series) -> pd.Series:
        return pd.Series(np.where(fare >= 30, "high", np.where(fare >= 15, "mid", "low")))

    return pickled, arrow, bucket_pandas


@router.post("/udfs")
def udf_performance(rows: int = 1_000_000):
    n = clamp(rows, 10_000, 2_000_000)
    s = core.spark()
    pickled, arrow, bucket_pandas = bucket_udfs()
    with Demo("udfs") as d:
        base = s.range(n).select((F.abs(F.hash("id")) % 4500 / 100.0).alias("fare"))
        variants = [
            ("built-in when()", base.select(F.when(F.col("fare") >= 30, "high").when(F.col("fare") >= 15, "mid").otherwise("low").alias("b"))),
            ("pandas_udf (Arrow, vectorized)", base.select(bucket_pandas("fare").alias("b"))),
            ("@udf(useArrow=True)", base.select(arrow("fare").alias("b"))),
            ("@udf(useArrow=False), pickled row by row", base.select(pickled("fare").alias("b"))),
        ]
        results = []
        for label, df in variants:
            counts = df.groupBy("b").count()
            rows_out, ms = d.timed(f"{label}: groupBy('b').count().collect() over {n:,} rows", counts.collect)
            results.append({"label": label, "ms": ms, "counts": {r["b"]: r["count"] for r in rows_out}})
        d.data.update(rows=n, results=results)
        d.check("All four implementations agree", all(r["counts"] == results[0]["counts"] for r in results), results[0]["counts"])
        d.check("Built-in functions beat the pickled row-at-a-time Python UDF", results[0]["ms"] < results[3]["ms"],
                f"{results[0]['ms']:.0f} ms vs {results[3]['ms']:.0f} ms")
    return d.result()
