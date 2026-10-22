"""Delta Lake: the transaction log, time travel, MERGE, schema enforcement and evolution,
OPTIMIZE / clustering with data skipping, VACUUM, Change Data Feed and constraints."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from pyspark.sql import functions as F

from .. import core
from ..lab import Demo, clamp

router = APIRouter(prefix="/api/demos/delta")
SCHEMA = "main.delta_lab"


def fresh_table(d: Demo, name: str, columns: str, extra: str = "", props: str = "") -> tuple[str, Path]:
    """Drop and recreate an external Delta table so every run starts from version 0."""
    core.ensure_schema(d, "delta_lab")
    full, location = f"{SCHEMA}.{name}", core.ext_path(f"delta_lab_{name}")
    core.drop_table(d, full, str(location))
    tblprops = f" TBLPROPERTIES ({props})" if props else ""
    core.sql(d, f"CREATE TABLE {full} ({columns}) USING DELTA {extra} LOCATION '{location}'{tblprops}")
    return full, location


def live_files(commits: list[dict]) -> set[str]:
    files: set[str] = set()
    for c in commits:
        files -= {r["path"] for r in c["remove"]}
        files |= {a["path"] for a in c["add"]}
    return files


@router.post("/log")
def transaction_log(deletion_vectors: bool = False, mistake: bool = False):
    s = core.spark()
    with Demo("delta-log") as d:
        props = "'delta.enableDeletionVectors' = 'true'" if deletion_vectors else "'delta.enableDeletionVectors' = 'false'"
        full, loc = fresh_table(d, "accounts", "id INT, owner STRING, balance DOUBLE", props=props)
        steps = [
            ("INSERT 3 rows", f"INSERT INTO {full} SELECT /*+ COALESCE(1) */ * FROM VALUES (1,'ana',100.0),(2,'ben',250.0),(3,'cy',75.0)"),
            ("INSERT 2 rows", f"INSERT INTO {full} SELECT /*+ COALESCE(1) */ * FROM VALUES (4,'dee',500.0),(5,'eli',20.0)"),
            ("UPDATE 1 row", f"UPDATE {full} SET balance = balance + 50 WHERE id = 2"),
            ("DELETE 1 row", f"DELETE FROM {full} WHERE id = 5"),
        ]
        timeline = []
        commits = core.delta_log(loc)
        timeline.append({"label": "CREATE TABLE", "version": 0, "rows": 0, "liveFiles": 0, "diskFiles": len(core.data_files(loc))})
        for label, statement in steps:
            core.sql(d, statement)
            commits = core.delta_log(loc)
            d.command(f"ls {loc.name}/_delta_log  -> {', '.join(c['file'] for c in commits)}")
            rows = core.scalar(d, f"SELECT count(*) FROM {full}")
            timeline.append({"label": label, "version": commits[-1]["version"], "rows": rows,
                             "liveFiles": len(live_files(commits)), "diskFiles": len(core.data_files(loc))})
        history = core.sql(d, f"DESCRIBE HISTORY {full}")
        d.data.update(commits=commits, timeline=timeline, deletionVectors=deletion_vectors,
                      history=[{"version": h["version"], "operation": h["operation"]} for h in sorted(history, key=lambda h: h["version"])])
        versions = [c["version"] for c in commits]
        update = next(c for c in commits if c["operation"] == "UPDATE")
        delete = next(c for c in commits if c["operation"] == "DELETE")
        d.check("Every write added exactly one JSON commit (versions 0..4)", versions == list(range(5)), versions)
        if deletion_vectors:
            dv_adds = [a for c in (update, delete) for a in c["add"] if a["dv"]]
            d.check("With deletion vectors, UPDATE/DELETE mark rows deleted instead of rewriting whole files",
                    len(dv_adds) >= 1, f"{len(dv_adds)} file(s) re-added with a deletion vector")
        else:
            d.check("UPDATE is copy-on-write: it removed the old file and added a rewritten one",
                    len(update["remove"]) >= 1 and len(update["add"]) >= 1, f"remove {len(update['remove'])}, add {len(update['add'])}")
        d.check("Old files stay on disk for time travel (disk ≥ live files)", timeline[-1]["diskFiles"] >= timeline[-1]["liveFiles"],
                f"{timeline[-1]['diskFiles']} on disk, {timeline[-1]['liveFiles']} live")
        d.check("DESCRIBE HISTORY reads the same log", [h["operation"] for h in d.data["history"]][-2:] == ["UPDATE", "DELETE"],
                [h["operation"] for h in d.data["history"]])
        if mistake:
            victim = sorted(live_files(commits))[0]
            (loc / victim).unlink()
            d.command(f"rm {loc.name}/{victim}  # deleting a data file by hand, behind Delta's back")
            _, error = d.attempt(f"SELECT * FROM {full}", lambda: s.sql(f"SELECT * FROM {full}").collect())
            d.check("Reading the table now fails: the log still points at the missing file", error and "FILE_NOT_EXIST" in error, error)
            s.conf.set("spark.sql.files.ignoreMissingFiles", "true")
            d.command('spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")  # salvage what is left')
            core.sql(d, f"INSERT OVERWRITE {full} SELECT * FROM {full}")
            s.conf.unset("spark.sql.files.ignoreMissingFiles")
            d.command('spark.conf.unset("spark.sql.files.ignoreMissingFiles")')
            survivors = core.scalar(d, f"SELECT count(*) FROM {full}")
            d.check("After rewriting the surviving rows the table reads again (the lost rows are gone for good)",
                    survivors is not None and survivors < timeline[-1]["rows"], f"{survivors} of {timeline[-1]['rows']} rows left")
            d.data["mistake"] = {"deleted": victim, "error": error, "survivors": survivors}
    return d.result()


@router.post("/time-travel")
def time_travel(restore_to: int = 1, mistake: bool = False):
    target = clamp(restore_to, 0, 4)
    s = core.spark()
    with Demo("time-travel") as d:
        full, _ = fresh_table(d, "prices", "product STRING, price DOUBLE")
        for statement in (f"INSERT INTO {full} VALUES ('apple', 1.0), ('bread', 2.5), ('milk', 1.2)",
                          f"UPDATE {full} SET price = round(price * 1.1, 2)",
                          f"DELETE FROM {full} WHERE product = 'bread'",
                          f"INSERT INTO {full} VALUES ('coffee', 7.9)"):
            core.sql(d, statement)
        history = sorted(core.sql(d, f"DESCRIBE HISTORY {full}"), key=lambda h: h["version"])
        snapshots = []
        for h in history:
            rows = core.sql(d, f"SELECT product, price FROM {full} VERSION AS OF {h['version']} ORDER BY product")
            snapshots.append({"version": h["version"], "operation": h["operation"], "timestamp": str(h["timestamp"]), "rows": rows})
        ts = history[2]["timestamp"]
        by_time = core.sql(d, f"SELECT product, price FROM {full} TIMESTAMP AS OF '{ts}' ORDER BY product")
        restored = core.sql(d, f"RESTORE TABLE {full} TO VERSION AS OF {target}")
        after = core.sql(d, f"SELECT product, price FROM {full} ORDER BY product")
        latest = max(h["version"] for h in core.sql(d, f"DESCRIBE HISTORY {full}"))
        d.data.update(snapshots=snapshots, restoredTo=target, afterRestore=after, latestVersion=latest,
                      restoreMetrics=restored[0] if restored else {}, byTimestamp={"timestamp": str(ts), "rows": by_time})
        d.check("Each version returns its own snapshot", len({str(sn["rows"]) for sn in snapshots[1:]}) == len(snapshots) - 1,
                f"{len(snapshots)} versions")
        d.check("TIMESTAMP AS OF finds the same snapshot as VERSION AS OF", by_time == snapshots[2]["rows"], str(ts))
        d.check(f"RESTORE brought back version {target}'s data", after == snapshots[target]["rows"], after)
        d.check("RESTORE is itself a new commit: history is never rewritten", latest == len(snapshots), f"now at version {latest}")
        if mistake:
            _, error = d.attempt(f"SELECT * FROM {full} VERSION AS OF 99", lambda: s.sql(f"SELECT * FROM {full} VERSION AS OF 99").collect())
            d.check("Asking for a version that does not exist fails with the available range", error and "DELTA_VERSION_NOT_FOUND" in error, error)
            d.data["mistake"] = error
    return d.result()


@router.post("/merge")
def merge_upsert(mistake: bool = False):
    s = core.spark()
    with Demo("merge") as d:
        full, _ = fresh_table(d, "customers", "id INT, name STRING, tier STRING, updated INT")
        core.sql(d, f"INSERT INTO {full} VALUES (1,'Ana','gold',1),(2,'Ben','silver',1),(3,'Cy','bronze',1),(4,'Dee','silver',1),(5,'Eli','bronze',1)")
        before = core.sql(d, f"SELECT * FROM {full} ORDER BY id")
        changes = [(2, "Ben", "gold", 2, "UPSERT"), (4, "Dee", "gold", 2, "UPSERT"), (5, None, None, 2, "DELETE"),
                   (6, "Fay", "bronze", 2, "UPSERT"), (7, "Gus", "silver", 2, "UPSERT")]
        source = s.createDataFrame(changes, "id INT, name STRING, tier STRING, updated INT, op STRING")
        source.createOrReplaceTempView("changes")
        d.command("spark.createDataFrame(changes).createOrReplaceTempView('changes')  # 5 change events")
        merge_sql = f"""MERGE INTO {full} AS t USING changes AS s ON t.id = s.id
            WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
            WHEN MATCHED THEN UPDATE SET t.name = s.name, t.tier = s.tier, t.updated = s.updated
            WHEN NOT MATCHED AND s.op != 'DELETE' THEN INSERT (id, name, tier, updated) VALUES (s.id, s.name, s.tier, s.updated)"""
        metrics = core.sql(d, merge_sql)[0]
        after = core.sql(d, f"SELECT * FROM {full} ORDER BY id")
        d.data.update(before=before, changes=[dict(zip(["id", "name", "tier", "updated", "op"], c)) for c in changes],
                      after=after, metrics=metrics)
        d.check("MERGE updated the 2 matched rows", metrics["num_updated_rows"] == 2, metrics)
        d.check("MERGE deleted the row flagged DELETE", metrics["num_deleted_rows"] == 1 and all(r["id"] != 5 for r in after), "id 5 gone")
        d.check("MERGE inserted the 2 new keys", metrics["num_inserted_rows"] == 2 and {6, 7} <= {r["id"] for r in after}, "ids 6, 7 added")
        d.check("It all landed as one atomic commit", core.sql(d, f"DESCRIBE HISTORY {full} LIMIT 1")[0]["operation"] == "MERGE", "1 MERGE commit")
        if mistake:
            dupes = s.createDataFrame([(1, "Ana", "silver", 3, "UPSERT"), (1, "Ana", "platinum", 4, "UPSERT")],
                                      "id INT, name STRING, tier STRING, updated INT, op STRING")
            dupes.createOrReplaceTempView("dupes")
            d.command("dupes: two change events for id 1 in the same batch")
            _, error = d.attempt("MERGE INTO customers USING dupes ON t.id = s.id WHEN MATCHED THEN UPDATE ...",
                                 lambda: s.sql(f"MERGE INTO {full} AS t USING dupes AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.tier = s.tier").collect())
            d.check("Two source rows for one target row are rejected", error and "MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW" in error.upper().replace(" ", "_"), error)
            fixed = core.sql(d, f"""MERGE INTO {full} AS t USING (
                    SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY updated DESC) AS rn FROM dupes) WHERE rn = 1
                ) AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.tier = s.tier, t.updated = s.updated""",
                show="MERGE INTO customers USING (latest row per id via row_number()) ... WHEN MATCHED THEN UPDATE")
            tier = core.scalar(d, f"SELECT tier FROM {full} WHERE id = 1")
            d.check("Deduplicating the source (latest event wins) makes the MERGE succeed", fixed[0]["num_updated_rows"] == 1 and tier == "platinum", tier)
            d.data["mistake"] = {"error": error, "tierAfterFix": tier}
    return d.result()


@router.post("/schema")
def schema_evolution(mistake: bool = False):
    s = core.spark()
    with Demo("schema-evolution") as d:
        full, _ = fresh_table(d, "signups", "id INT, email STRING")
        s.createDataFrame([(1, "a@x.io"), (2, "b@x.io")], "id INT, email STRING").write.mode("append").saveAsTable(full)
        d.command(f"df.write.mode('append').saveAsTable('{full}')  # 2 rows, same schema")
        schemas = [{"step": "created", "columns": [(f.name, f.dataType.simpleString()) for f in s.read.table(full).schema]}]
        new_cols = s.createDataFrame([(3, "c@x.io", "PT")], "id INT, email STRING, country STRING")
        _, enforced = d.attempt("df_with_country.write.mode('append').saveAsTable(...)  # a new column",
                                lambda: new_cols.write.mode("append").saveAsTable(full))
        d.check("Schema enforcement rejects an unexpected column", enforced and ("METADATA_MISMATCH" in enforced or "schema mismatch" in enforced.lower()), enforced)
        new_cols.write.mode("append").option("mergeSchema", "true").saveAsTable(full)
        d.command("df_with_country.write.mode('append').option('mergeSchema', 'true').saveAsTable(...)")
        schemas.append({"step": "mergeSchema", "columns": [(f.name, f.dataType.simpleString()) for f in s.read.table(full).schema]})
        core.sql(d, f"ALTER TABLE {full} ADD COLUMNS (plan STRING COMMENT 'pricing plan')")
        schemas.append({"step": "ALTER TABLE ADD COLUMNS", "columns": [(f.name, f.dataType.simpleString()) for f in s.read.table(full).schema]})
        rows = core.sql(d, f"SELECT * FROM {full} ORDER BY id")
        d.data.update(schemas=schemas, rows=rows, enforcedError=enforced)
        d.check("mergeSchema added the column; older rows read it as NULL",
                [c[0] for c in schemas[1]["columns"]] == ["id", "email", "country"] and rows[0]["country"] is None, schemas[1]["columns"])
        d.check("ALTER TABLE ADD COLUMNS evolved the schema without rewriting data", schemas[2]["columns"][-1][0] == "plan", schemas[2]["columns"])
        if mistake:
            bad_type = s.createDataFrame([("4", "d@x.io"), ("four", "e@x.io")], "id STRING, email STRING")
            _, error = d.attempt("df_where_id_is_text.write.mode('append').saveAsTable(...)  # id = '4' and id = 'four'",
                                 lambda: bad_type.write.mode("append").saveAsTable(full))
            d.check("A STRING id is cast to the INT column at write time, and 'four' fails the cast", error is not None and "CAST" in error, error)
            d.check("The failed write left the table untouched (atomic)", core.scalar(d, f"SELECT count(*) FROM {full} WHERE id = 4") == 0, "no partial rows")
            clean = bad_type.withColumn("id", F.expr("try_cast(id AS INT)"))
            clean.where("id IS NOT NULL").write.mode("append").saveAsTable(full)
            d.command("df.withColumn('id', expr('try_cast(id AS INT)')).where('id IS NOT NULL').write.mode('append').saveAsTable(...)")
            rejected = clean.where("id IS NULL").count()
            d.command(f"rejected rows (send to a quarantine table): {rejected}")
            d.check("Validating with try_cast keeps good rows and quarantines the bad one",
                    core.scalar(d, f"SELECT count(*) FROM {full} WHERE id = 4") == 1 and rejected == 1, "1 written, 1 quarantined")
            d.data["mistake"] = error
    return d.result()


def file_ranges(loc: Path) -> list[dict]:
    commits = core.delta_log(loc)
    live = live_files(commits)
    ranges = {}
    for c in commits:
        for a in c["add"]:
            if a["path"] in live and a.get("minValues"):
                ranges[a["path"]] = {"file": a["path"][:14], "min": a["minValues"].get("key"), "max": a["maxValues"].get("key"),
                                     "rows": a["numRecords"], "size": a["size"]}
    return sorted(ranges.values(), key=lambda r: (r["min"] or 0))


@router.post("/optimize")
def optimize(files: int = 24, strategy: str = "zorder"):
    n = clamp(files, 6, 60)
    liquid = strategy == "liquid"
    s = core.spark()
    with Demo("optimize") as d:
        full, loc = fresh_table(d, "clicks", "key INT, page STRING", extra="CLUSTER BY (key)" if liquid else "")
        for i in range(n):  # many small appends with keys scattered over the whole range: the "small files" shape
            s.range(200).select(((F.col("id") * 7919 + i * 104729) % 10_000).cast("int").alias("key"),
                                F.concat(F.lit("/page/"), (F.col("id") % 25).cast("string")).alias("page")).write.mode("append").saveAsTable(full)
        d.command(f"for i in range({n}): df_200_rows.write.mode('append').saveAsTable('{full}')  # {n} small files")
        probe = f"SELECT count(*) AS n, count(DISTINCT _metadata.file_path) AS files_read FROM {full} WHERE key BETWEEN 4200 AND 4300"
        before_files = file_ranges(loc)
        before = core.sql(d, probe)[0]
        s.conf.set("spark.databricks.delta.optimize.maxFileSize", str(48 * 1024))
        d.command('spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "48KB")  # small target so the lab shows several output files')
        try:
            metrics = core.sql(d, f"OPTIMIZE {full}" if liquid else f"OPTIMIZE {full} ZORDER BY (key)")[0]["metrics"]
        finally:
            s.conf.unset("spark.databricks.delta.optimize.maxFileSize")
        after_files = file_ranges(loc)
        after = core.sql(d, probe)[0]
        d.data.update(strategy="liquid clustering" if liquid else "Z-order", before=before_files, after=after_files,
                      probe={"before": before, "after": after, "range": [4200, 4300]},
                      metrics={k: metrics.get(k) for k in ("numFilesAdded", "numFilesRemoved")})
        d.check("OPTIMIZE compacted many small files into a few larger ones", len(after_files) < len(before_files),
                f"{len(before_files)} -> {len(after_files)} files")
        d.check("Same rows before and after (OPTIMIZE never changes data)",
                sum(f["rows"] for f in before_files) == sum(f["rows"] for f in after_files) == n * 200, f"{n * 200} rows")
        d.check("Data skipping: the range query reads fewer files after clustering by key",
                after["files_read"] < before["files_read"] and after["n"] == before["n"], f"{before['files_read']} -> {after['files_read']} files read")
    return d.result()


@router.post("/vacuum")
def vacuum():
    s = core.spark()
    with Demo("vacuum") as d:
        full, loc = fresh_table(d, "orders", "id INT, status STRING")
        core.sql(d, f"INSERT INTO {full} SELECT id, 'new' FROM range(100) AS r(id)")
        core.sql(d, f"UPDATE {full} SET status = 'paid' WHERE id % 2 = 0")
        core.sql(d, f"UPDATE {full} SET status = 'shipped' WHERE id % 4 = 0")
        commits = core.delta_log(loc)
        before = {"disk": len(core.data_files(loc)), "live": len(live_files(commits))}
        dry = core.sql(d, f"VACUUM {full} DRY RUN")
        _, too_short = d.attempt(f"VACUUM {full} RETAIN 0 HOURS", lambda: s.sql(f"VACUUM {full} RETAIN 0 HOURS").collect())
        v1_before = core.scalar(d, f"SELECT count(*) FROM {full} VERSION AS OF 1 WHERE status = 'new'")
        s.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        d.command('spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")  # lab only!')
        try:
            core.sql(d, f"VACUUM {full} RETAIN 0 HOURS")
        finally:
            s.conf.unset("spark.databricks.delta.retentionDurationCheck.enabled")
        after = {"disk": len(core.data_files(loc)), "live": len(live_files(core.delta_log(loc)))}
        # count(*) alone can be answered from file statistics; read real column values instead
        _, travel = d.attempt(f"SELECT status, count(*) FROM {full} VERSION AS OF 1 GROUP BY status",
                              lambda: s.sql(f"SELECT status, count(*) FROM {full} VERSION AS OF 1 GROUP BY status").collect())
        current = core.scalar(d, f"SELECT count(*) FROM {full}")
        d.data.update(before=before, after=after, dryRun=len(dry), retentionError=too_short, timeTravelError=travel,
                      versions=len(commits), v1Rows=v1_before)
        d.check("DRY RUN with the default 7-day retention deletes nothing (files are too recent)", len(dry) == 0, f"{len(dry)} files listed")
        d.check("A retention shorter than 7 days is refused by default", too_short and "RETENTION_PERIOD_TOO_SHORT" in too_short, too_short)
        d.check("VACUUM RETAIN 0 HOURS deleted every unreferenced file", after["disk"] == after["live"] < before["disk"],
                f"{before['disk']} -> {after['disk']} files on disk")
        d.check("Time travel to an old version now fails: its files are gone", travel is not None, travel)
        d.check("The current version still reads fine", current == 100, f"{current} rows")
    return d.result()


@router.post("/cdf")
def change_data_feed(mistake: bool = False):
    s = core.spark()
    with Demo("change-data-feed") as d:
        full, _ = fresh_table(d, "inventory", "sku STRING, qty INT", props="'delta.enableChangeDataFeed' = 'true'")
        for statement in (f"INSERT INTO {full} VALUES ('A', 10), ('B', 5), ('C', 0)",
                          f"UPDATE {full} SET qty = qty - 2 WHERE sku = 'A'",
                          f"DELETE FROM {full} WHERE qty = 0",
                          f"MERGE INTO {full} t USING (SELECT 'B' sku, 9 qty UNION ALL SELECT 'D', 4) s ON t.sku = s.sku "
                          "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"):
            core.sql(d, statement)
        changes = core.sql(d, f"SELECT sku, qty, _change_type, _commit_version FROM table_changes('{full}', 1) ORDER BY _commit_version, sku, _change_type")
        df_changes = s.read.option("readChangeFeed", "true").option("startingVersion", 2).table(full).count()
        d.command(f"spark.read.option('readChangeFeed', 'true').option('startingVersion', 2).table('{full}').count() -> {df_changes}")
        d.data.update(changes=changes)
        types = [c["_change_type"] for c in changes]
        d.check("Inserts are recorded", types.count("insert") == 4, f"{types.count('insert')} inserts")
        d.check("Updates carry both the pre- and post-image", types.count("update_preimage") == types.count("update_postimage") == 2, "2 pairs")
        d.check("Deletes are recorded with the deleted row", any(c["_change_type"] == "delete" and c["sku"] == "C" for c in changes), "C deleted")
        d.check("The DataFrame reader sees the same feed from version 2 on", df_changes == len([c for c in changes if c["_commit_version"] >= 2]), df_changes)
        if mistake:
            plain, _ = fresh_table(d, "inventory_plain", "sku STRING, qty INT")
            core.sql(d, f"INSERT INTO {plain} VALUES ('A', 1)")
            _, error = d.attempt(f"SELECT * FROM table_changes('{plain}', 0)", lambda: s.sql(f"SELECT * FROM table_changes('{plain}', 0)").collect())
            d.check("A table without delta.enableChangeDataFeed has no change data to read", error and "MISSING_CHANGE_DATA" in error, error)
            d.data["mistake"] = error
    return d.result()


@router.post("/constraints")
def constraints(mistake: bool = False):
    s = core.spark()
    with Demo("constraints") as d:
        core.ensure_schema(d, "delta_lab")
        full = f"{SCHEMA}.payments"
        core.sql(d, f"DROP TABLE IF EXISTS {full}")
        core.sql(d, f"""CREATE TABLE {full} (
                id BIGINT GENERATED ALWAYS AS IDENTITY,
                account STRING NOT NULL,
                amount DOUBLE,
                paid_at TIMESTAMP,
                paid_date DATE GENERATED ALWAYS AS (CAST(paid_at AS DATE))
            ) USING DELTA""")
        core.sql(d, f"ALTER TABLE {full} ADD CONSTRAINT positive_amount CHECK (amount > 0)")
        core.sql(d, f"INSERT INTO {full} (account, amount, paid_at) VALUES ('acc-1', 25.0, TIMESTAMP '2026-10-01 09:30:00'), ('acc-2', 99.9, TIMESTAMP '2026-10-02 18:05:00')")
        _, check_error = d.attempt("INSERT ... VALUES ('acc-3', 10.0, ...), ('acc-4', -5.0, ...)  # one bad row in the batch",
                                   lambda: s.sql(f"INSERT INTO {full} (account, amount, paid_at) VALUES ('acc-3', 10.0, current_timestamp()), ('acc-4', -5.0, current_timestamp())").collect())
        _, null_error = d.attempt("INSERT ... VALUES (NULL, 5.0, ...)  # account is NOT NULL",
                                  lambda: s.sql(f"INSERT INTO {full} (account, amount, paid_at) VALUES (NULL, 5.0, current_timestamp())").collect())
        rows = core.sql(d, f"SELECT * FROM {full} ORDER BY id")
        props = {r["key"]: r["value"] for r in core.sql(d, f"SHOW TBLPROPERTIES {full}") if r["key"].startswith("delta.constraints")}
        d.data.update(rows=rows, constraints=props, errors={"check": check_error, "notNull": null_error})
        d.check("A CHECK violation rejects the whole batch", check_error and "VIOLATE_CONSTRAINT" in check_error and len(rows) == 2, check_error)
        d.check("NOT NULL is enforced", null_error and "NOT_NULL" in null_error.upper().replace(" ", "_"), null_error)
        d.check("IDENTITY assigned ids and the generated column was computed", [r["id"] for r in rows] == [1, 2] and str(rows[0]["paid_date"]) == "2026-10-01",
                [(r["id"], str(r["paid_date"])) for r in rows])
        d.check("The constraint is stored as a table property", "delta.constraints.positive_amount" in props, props)
        if mistake:
            _, inline = d.attempt("CREATE TABLE ... (amount DOUBLE CONSTRAINT positive CHECK (amount > 0))  # inline, in a UC catalog",
                                  lambda: s.sql(f"CREATE TABLE {SCHEMA}.payments_inline (amount DOUBLE CONSTRAINT positive CHECK (amount > 0)) USING DELTA").collect())
            d.check("Inline CHECK constraints at CREATE TABLE are not supported through the UC catalog", inline and "UNSUPPORTED_FEATURE" in inline, inline)
            d.data["mistake"] = inline
    return d.result()
