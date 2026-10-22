"""Ingestion and streaming: incremental file ingestion with Structured Streaming and checkpoints
(the engine under Auto Loader), Delta tables as streaming sources, and a medallion pipeline."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter
from pyspark.sql import functions as F

from .. import core
from ..lab import Demo, clamp

router = APIRouter(prefix="/api/demos/stream")
SCHEMA = "main.stream"
LANDING = core.LAKEHOUSE / "landing"
CHECKPOINTS = core.LAKEHOUSE / "checkpoints"


def write_batch(folder: Path, index: int, rows: int = 5) -> str:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"clicks_{index:03d}.json"
    path.write_text("".join(json.dumps({"event_id": index * 100 + i, "user": f"u{(index + i) % 7}", "page": f"/p/{i % 3}"}) + "\n" for i in range(rows)))
    return path.name


def checkpoint_listing(path: Path) -> dict:
    return {sub: sorted(p.name for p in (path / sub).iterdir() if not p.name.startswith(".")) if (path / sub).exists() else []
            for sub in ("offsets", "commits", "sources")}


def run_available_now(d: Demo, df, table: str, checkpoint: Path, label: str, **options) -> dict:
    """One incremental run: process whatever is new since the checkpoint, then stop."""
    writer = df.writeStream.option("checkpointLocation", str(checkpoint)).trigger(availableNow=True)
    for key, value in options.items():
        writer = writer.option(key, value)
    query, ms = d.timed(f"{label}.writeStream.option('checkpointLocation', ...).trigger(availableNow=True).toTable('{table}').awaitTermination()",
                        lambda: _run(writer, table))
    progress = query.recentProgress or []
    rows = sum(p.numInputRows or 0 for p in progress)  # StreamingQueryProgress objects in Spark 4
    return {"inputRows": rows, "batches": [p.batchId for p in progress if (p.numInputRows or 0) > 0], "ms": ms}


def _run(writer, table):
    query = writer.toTable(table)
    query.awaitTermination()
    return query


@router.post("/files")
def file_ingestion(batches: int = 3, mistake: bool = False):
    n = clamp(batches, 1, 8)
    s = core.spark()
    with Demo("streaming-files") as d:
        core.ensure_schema(d, "stream")
        folder, checkpoint, table = LANDING / "clicks", CHECKPOINTS / "clicks_bronze", f"{SCHEMA}.clicks_bronze"
        for p in (folder, checkpoint):
            core.remove_dir(p)
        core.drop_table(d, table)
        source = s.readStream.schema("event_id LONG, user STRING, page STRING").json(str(folder)).withColumn("ingested_at", F.current_timestamp())
        d.command(f"source = spark.readStream.schema('event_id LONG, user STRING, page STRING').json('{folder}')")
        runs, written = [], []
        plan = [("first run", n), ("run again, nothing new", 0), ("2 new files arrive", 2)]
        index = 0
        for label, new_files in plan:
            names = []
            for _ in range(new_files):
                names.append(write_batch(folder, index))
                index += 1
            written += names
            if names:
                d.command(f"landing/clicks/ <- {', '.join(names)}")
            result = run_available_now(d, source, table, checkpoint, "source")
            total = core.scalar(d, f"SELECT count(*) FROM {table}")
            runs.append({"label": label, "newFiles": names, **result, "totalRows": total})
        d.data.update(runs=runs, files=written, checkpoint=checkpoint_listing(checkpoint))
        d.check(f"Run 1 ingested all {n} files", runs[0]["inputRows"] == n * 5, f"{runs[0]['inputRows']} rows")
        d.check("Run 2 found nothing new: the checkpoint remembers what was read", runs[1]["inputRows"] == 0, "0 rows")
        d.check("Run 3 read only the 2 new files", runs[2]["inputRows"] == 10, f"{runs[2]['inputRows']} rows")
        d.check("Exactly once: every file landed in the table once", runs[-1]["totalRows"] == len(written) * 5, f"{runs[-1]['totalRows']} rows")
        if mistake:
            _, no_schema = d.attempt(f"spark.readStream.json('{folder}').schema  # no schema given (Connect analyzes lazily)",
                                     lambda: s.readStream.json(str(folder)).schema)
            _, no_checkpoint = d.attempt("source.writeStream.trigger(availableNow=True).toTable(...)  # no checkpointLocation",
                                         lambda: source.writeStream.trigger(availableNow=True).toTable(f"{SCHEMA}.clicks_nocheckpoint"))
            d.check("File streams need an explicit schema", no_schema and "Schema must be specified" in no_schema, no_schema)
            d.check("A streaming write without a checkpoint is refused", no_checkpoint and "checkpointLocation must be specified" in no_checkpoint, no_checkpoint)
            d.data["mistake"] = {"schema": no_schema, "checkpoint": no_checkpoint}
    return d.result()


@router.post("/delta-source")
def delta_as_source(mistake: bool = False):
    s = core.spark()
    with Demo("delta-stream-source") as d:
        core.ensure_schema(d, "stream")
        bronze, silver, checkpoint = f"{SCHEMA}.orders_bronze", f"{SCHEMA}.orders_silver", CHECKPOINTS / "orders_silver"
        core.remove_dir(checkpoint)
        for t in (bronze, silver):
            core.drop_table(d, t)
        core.sql(d, f"CREATE TABLE {bronze} (order_id INT, amount DOUBLE) USING DELTA")
        stream = s.readStream.table(bronze).where("amount > 0").withColumn("amount_with_tax", F.round(F.col("amount") * 1.23, 2))
        d.command(f"stream = spark.readStream.table('{bronze}').where('amount > 0').withColumn('amount_with_tax', ...)")
        runs = []
        for label, values in (("5 orders appended", "(1, 10.0), (2, 25.5), (3, -4.0), (4, 8.0), (5, 99.0)"), ("3 more orders", "(6, 12.0), (7, 3.5), (8, 40.0)")):
            core.sql(d, f"INSERT INTO {bronze} VALUES {values}")
            result = run_available_now(d, stream, silver, checkpoint, "stream")
            runs.append({"label": label, **result, "silverRows": core.scalar(d, f"SELECT count(*) FROM {silver}")})
        d.data.update(runs=runs, silver=core.sql(d, f"SELECT * FROM {silver} ORDER BY order_id"))
        d.check("The first run picked up the 5 appended rows (and filtered the negative one)", runs[0]["inputRows"] == 5 and runs[0]["silverRows"] == 4, runs[0])
        d.check("The second run read only the 3 new rows", runs[1]["inputRows"] == 3 and runs[1]["silverRows"] == 7, runs[1])
        if mistake:
            core.sql(d, f"UPDATE {bronze} SET amount = 11.0 WHERE order_id = 1")  # rewrites a row in the streaming source
            _, error = d.attempt("stream (again) .toTable(silver)  # the source now contains a non-append commit",
                                 lambda: run_available_now(d, stream, silver, checkpoint, "stream"))
            d.check("A streaming read fails when the source table had an update or delete", error and "DELTA_SOURCE_TABLE_IGNORE_CHANGES" in error, error)
            fixed_stream = s.readStream.option("skipChangeCommits", "true").table(bronze).where("amount > 0").withColumn("amount_with_tax", F.round(F.col("amount") * 1.23, 2))
            result = run_available_now(d, fixed_stream, silver, checkpoint, "spark.readStream.option('skipChangeCommits', 'true').table(bronze)")
            d.check("skipChangeCommits skips rewritten files and the stream continues", result["inputRows"] == 0, result)
            d.data["mistake"] = {"error": error, "afterFix": result}
    return d.result()


RAW_EVENTS = [
    '{"event_id": 1, "store": "north", "amount": 12.5}',
    '{"event_id": 2, "store": "south", "amount": 30.0}',
    '{"event_id": 2, "store": "south", "amount": 30.0}',
    '{"event_id": 3, "store": "north", "amount": -1}',
    '{"event_id": 4, "store": "east", "amount": 8.25}',
    'this is not json',
    '{"event_id": 5, "store": "north", "amount": 19.75}',
    '{"event_id": 6, "store": "east"}',
    '{"event_id": 7, "store": "south", "amount": 44.0}',
    '{"event_id": 1, "store": "north", "amount": 12.5}',
]


@router.post("/medallion")
def medallion():
    s = core.spark()
    with Demo("medallion") as d:
        core.ensure_schema(d, "stream")
        folder, checkpoint = LANDING / "sales", CHECKPOINTS / "sales_bronze"
        bronze, silver, quarantine, gold = (f"{SCHEMA}.sales_bronze", f"{SCHEMA}.sales_silver", f"{SCHEMA}.sales_quarantine", f"{SCHEMA}.sales_gold")
        for p in (folder, checkpoint):
            core.remove_dir(p)
        for t in (bronze, silver, quarantine, gold):
            core.drop_table(d, t)
        folder.mkdir(parents=True)
        (folder / "sales_001.jsonl").write_text("\n".join(RAW_EVENTS[:5]) + "\n")
        (folder / "sales_002.jsonl").write_text("\n".join(RAW_EVENTS[5:]) + "\n")
        d.command("landing/sales/ <- sales_001.jsonl, sales_002.jsonl (10 raw lines: duplicates, bad JSON, bad values)")
        raw = s.readStream.format("text").load(str(folder)).select(F.col("value").alias("raw"), F.col("_metadata.file_name").alias("source_file"),
                                                                    F.current_timestamp().alias("ingested_at"))
        bronze_run = run_available_now(d, raw, bronze, checkpoint, "spark.readStream.format('text')  # bronze keeps every raw line")
        schema = "event_id INT, store STRING, amount DOUBLE"
        core.sql(d, f"""CREATE TABLE {silver} USING DELTA AS
            SELECT DISTINCT p.event_id, p.store, p.amount FROM (SELECT from_json(raw, '{schema}') AS p FROM {bronze})
            WHERE p.event_id IS NOT NULL AND p.amount > 0""")
        core.sql(d, f"""CREATE TABLE {quarantine} USING DELTA AS
            SELECT raw, source_file, CASE WHEN from_json(raw, '{schema}').event_id IS NULL THEN 'malformed JSON'
                                          WHEN from_json(raw, '{schema}').amount IS NULL THEN 'missing amount'
                                          ELSE 'amount <= 0' END AS reason
            FROM {bronze} WHERE from_json(raw, '{schema}').event_id IS NULL OR coalesce(from_json(raw, '{schema}').amount, 0) <= 0""")
        core.sql(d, f"CREATE TABLE {gold} USING DELTA AS SELECT store, count(*) AS sales, round(sum(amount), 2) AS revenue FROM {silver} GROUP BY store")
        counts = {name: core.scalar(d, f"SELECT count(*) FROM {t}") for name, t in (("bronze", bronze), ("silver", silver), ("quarantine", quarantine), ("gold", gold))}
        d.data.update(raw=RAW_EVENTS, counts=counts, bronzeRun=bronze_run,
                      silver=core.sql(d, f"SELECT * FROM {silver} ORDER BY event_id"),
                      quarantine=core.sql(d, f"SELECT raw, reason FROM {quarantine}"),
                      gold=core.sql(d, f"SELECT * FROM {gold} ORDER BY revenue DESC"))
        silver_total = round(sum(r["amount"] for r in d.data["silver"]), 2)
        gold_total = round(sum(r["revenue"] for r in d.data["gold"]), 2)
        d.check("Bronze kept every raw line, even the broken ones", counts["bronze"] == len(RAW_EVENTS), counts["bronze"])
        d.check("Silver holds each valid event once (2 duplicates dropped)", counts["silver"] == 5, [r["event_id"] for r in d.data["silver"]])
        d.check("Bad records went to quarantine with a reason, not into the bin", counts["quarantine"] == 3, [r["reason"] for r in d.data["quarantine"]])
        d.check("Gold totals reconcile with silver", silver_total == gold_total, f"{gold_total}")
    return d.result()
