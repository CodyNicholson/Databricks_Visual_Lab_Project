# Databricks Visual Lab

An interactive, animated tour of Databricks in which every button runs real code. Databricks is a
paid cloud platform and cannot run in a container, so the lab runs the open-source engines
Databricks is built on (and still maintains): **Apache Spark 4.2** over Spark Connect (the protocol
Databricks Connect uses), **Delta Lake 4.4**, **Unity Catalog OSS 0.7**, **MLflow 3.17**, **Spark
Declarative Pipelines** (the open-source core of Lakeflow Declarative Pipelines), and the official
**Databricks SDK for Python**. 28 demos in 8 categories each explain a feature in plain language,
animate the request flow, run it, draw the result, show the code, and teach the mistakes people make
with it, reproducing most of them for real.

## What each page shows

- **Overview & getting started**: the mental model, versions, the real Databricks setup (CLI,
  Asset Bundles, Databricks Connect) typed out, pros / cons / when to use, how *not* to use
  Databricks, alternatives, and a map of every demo.
- **One page per feature**: explanation, key APIs, an animated request-flow diagram (browser →
  your code → Spark cluster → Unity Catalog / MLflow → storage), the buttons, a feature-specific
  animated visual of the real result, the code, common mistakes (exact error text, cause, fix,
  how to avoid it, Don't/Do code, and a "Reproduce it for real" button), and the commands and
  self-checks from the last run.
- **⚠ Mistakes, errors & fixes**: every anti-pattern, troubleshooting entry and per-feature pitfall
  on one searchable page. Paste an error message to find it.
- **▶ Run all demos**: runs every demo's self-test and marks each one ✓/✗ in the sidebar.

## Quick start

```bash
docker network create tunnel-net        # once; docker-compose.yml expects it
docker compose up -d --build            # first build downloads ~4 GB of images and jars
open http://localhost:3000              # the backend seeds samples.nyctaxi.trips on startup (~1 min)
```

| Service | Container | Port | What it is |
|---|---|---|---|
| `frontend` | `rixus-frontend` | 3000 → 80 | React build served by nginx; proxies `/api/` to the backend |
| `backend` | `databricks-lab-backend` | 8000 | FastAPI + PySpark Connect client, MLflow client, Databricks SDK |
| `spark` | `databricks-lab-spark` | (15002, 4040 internal) | Spark 4.2 Connect server, `local[4]`, Delta 4.4.1 + UC connector |
| `unitycatalog` | `databricks-lab-unitycatalog` | (8080 internal) | Unity Catalog OSS 0.7.0 REST server (H2 metadata in volume `uc-db`) |
| `mlflow` | `databricks-lab-mlflow` | (5000 internal) | MLflow 3.17.2 tracking server (SQLite + artifacts in volume `mlflow-data`) |

All services share the `lakehouse` volume, the lab's stand-in for cloud object storage.
Memory caps: Spark 3 GB, backend 2 GB, UC 1 GB, MLflow 1 GB.

## Feature coverage

| Area | Demo | Features / APIs |
|---|---|---|
| Spark on Databricks | Spark Connect & sessions | `SparkSession.builder.remote`, plan vs data size, jobs from the Spark UI API; RDD / `sparkContext` / static config errors |
| | Lazy evaluation & the optimizer | transformations vs actions, `explain('formatted')`, column pruning, predicate pushdown, `cache()` |
| | SQL and DataFrames | `spark.sql` with safe values, DataFrame joins, temp views, SQL injection, `:marker` incompatibility |
| | Shuffles, AQE & join strategies | `shuffle.partitions`, AQE partition coalescing, broadcast vs sort-merge join |
| | Python UDFs vs built-ins | `when()`, `pandas_udf`, `udf(useArrow=True)`, pickled UDFs |
| Delta Lake | The transaction log | `_delta_log` commits (add/remove), copy-on-write vs deletion vectors, `DESCRIBE HISTORY`; hand-deleted file |
| | Time travel & RESTORE | `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE`; missing version |
| | MERGE (upserts) | update / delete / insert in one commit, metrics; duplicate source keys |
| | Schema enforcement & evolution | enforcement, `mergeSchema`, `ALTER TABLE ADD COLUMNS`, `try_cast` quarantine |
| | OPTIMIZE, Z-order & liquid clustering | small-file compaction, per-file min/max stats, data skipping (`_metadata.file_path`), `CLUSTER BY` |
| | VACUUM & retention | `DRY RUN`, retention check, lost time travel |
| | Change Data Feed | `table_changes`, `readChangeFeed`, pre/post images |
| | Constraints & generated columns | `NOT NULL`, `CHECK`, `IDENTITY`, generated columns |
| Unity Catalog | Three-level namespace | catalogs/schemas/tables over the UC REST API, `USE`, three-part names |
| | Managed vs external tables | storage locations, catalog-managed commits, `DROP` semantics, re-registering |
| | Volumes | managed volume via REST, files → Delta table, `/Volumes` paths |
| | Functions | UC function registration, SQL masking function, temporary function scope |
| Ingestion & streaming | Incremental file ingestion | `readStream` + checkpoints + `trigger(availableNow=True)` (the engine under Auto Loader) |
| | Delta tables as streams | `readStream.table`, incremental bronze → silver, `skipChangeCommits` |
| | Medallion architecture | bronze / silver / quarantine / gold with reconciliation |
| Lakeflow pipelines | Declarative pipelines | `spark-pipelines run` / `dry-run`, Python + SQL datasets, streaming tables, materialized views, event log |
| | Change data capture (SCD1) | sequenced `MERGE`, late events, deletes; `create_auto_cdc_flow` limitation |
| Machine learning | MLflow experiment tracking | experiments, runs, params, metrics, `search_runs` |
| | Models in Unity Catalog | `registry_uri = uc:`, versions, signatures, `models:/` loading, batch scoring to Delta |
| Jobs & platform | Jobs with the Databricks SDK | `jobs.JobSettings` / `Task` / `depends_on` / `run_if`, Jobs API JSON, Asset Bundle YAML, unified auth |
| Common mistakes | Data skew in joins | per-task shuffle records, AQE skew join |
| | Row-by-row writes | commits / files / time per row vs one batch, OPTIMIZE afterwards |
| | Pulling data to the driver | `toPandas()` everything vs aggregate first |

## Architecture

```
 Browser ──HTTP──▶ rixus-frontend (nginx :80) ──/api/──▶ backend (FastAPI :8000)
                                                          │  PySpark Connect client, MLflow client,
                                                          │  Databricks SDK, spark-pipelines CLI
                     ┌────────────────gRPC :15002─────────┤
                     ▼                                    ├──REST /api/2.1/unity-catalog──▶ unitycatalog (:8080)
              spark (Connect server, local[4])            └──REST──▶ mlflow (:5000)
              Delta Lake 4.4.1 + UC connector ──REST──▶ unitycatalog
                     │
                     └──────────── lakehouse volume (/lakehouse: Delta tables, volumes, models, checkpoints)
```

Demos run one at a time (a lock in the backend): they rewrite shared tables on a single-node
"cluster", so visitors queue for up to 75 s.

## How not to use it, and common errors

Anti-patterns (details on the `#pitfalls` page): Delta tables as an OLTP app database; `collect()` /
`toPandas()` on big data; scheduled jobs on idle all-purpose clusters; UI-clicked notebooks as
production; partitioning by high-cardinality columns; new work in `hive_metastore`, DBFS root or
mounts; tokens in code; Python UDFs for built-in logic.

The most common errors, with the exact text the lab's stack prints. Most are reproduced by a "Do it
wrong" button.

| Error | Cause | Fix | Avoid |
|---|---|---|---|
| `[JVM_ATTRIBUTE_NOT_SUPPORTED] Attribute sparkContext is not supported in Spark Connect` | Classic JVM APIs over Connect / serverless | DataFrame API instead of RDDs | Test with `builder.remote()` |
| `[FAILED_READ_FILE.FILE_NOT_EXIST] … File does not exist` | Data files deleted behind Delta's back, or VACUUM before time travel | Restore files; salvage with `ignoreMissingFiles` + rewrite | Only change tables through Delta; keep retention ≥ 7 days |
| `[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE]` | Two source rows for one key in MERGE | Deduplicate with `row_number()` | Dedupe every CDC batch |
| `[DELTA_METADATA_MISMATCH] … schema mismatch` | Appending new columns | `mergeSchema` / `ALTER TABLE` | Deliberate schema evolution |
| `[DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT]` | VACUUM under 168 hours | Keep ≥ 168 h | Don't disable the check |
| `[DELTA_SOURCE_TABLE_IGNORE_CHANGES] Detected a data update …` | UPDATE/DELETE in a streaming source | `skipChangeCommits` or CDF | Append-only bronze |
| `[TABLE_OR_VIEW_NOT_FOUND] … Search path: [… main.default]` | One-part names, wrong current schema | Three-part names | Qualify names in production |
| `Unity Catalog does not support non-Delta managed table.` | OSS Spark defaults to Parquet | `USING DELTA` / `spark.sql.sources.default=delta` | Set the default in cluster config |
| `[UNBOUND_SQL_PARAMETER] Found the unbound parameter: b.` | Delta 4.4.1 extension + Spark 4.2.0 parameter markers | `spark.sql("… {b}", b=v)` | Test on the versions you deploy |
| `[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.10 than that in driver: 3.12` | Client and cluster Python differ (UDFs) | Same minor version as the runtime | Pin Python per runtime |
| `MlflowException: The saved sklearn model references untrusted types …` | MLflow 3 skops serialization | `skops_trusted_types=[…]` | Test log → load in CI |
| `default auth: cannot configure default credentials …` | SDK/CLI with no credentials | `databricks auth login` / env vars | OAuth service principals in CI |

Sources: the error text above was captured from this lab's own runs (Spark 4.2.0, Delta 4.4.1,
UC 0.7.0, MLflow 3.17.2, databricks-sdk 0.155.0) and is recorded in the demos' results. Background:
[Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html),
[Delta Lake docs](https://docs.delta.io/latest/), [Unity Catalog OSS](https://github.com/unitycatalog/unitycatalog),
[MLflow docs](https://mlflow.org/docs/latest/), [Databricks unified authentication](https://docs.databricks.com/en/dev-tools/auth.html),
[Delta releases](https://github.com/delta-io/delta/releases), [Spark 4.2.0 release notes](https://spark.apache.org/releases/spark-release-4-2-0.html).

## Configuration (`.env`)

| Variable | Default | Purpose |
|---|---|---|
| `TUNNEL_TOKEN` | – | Cloudflare tunnel token; used by `pipeline-run.sh` only if not set globally |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | empty | Optional real workspace: the SDK demo then also makes read-only calls (current user, list jobs). Offline when empty. |
| `LIVE_RUNS_PER_MINUTE`, `LIVE_RUNS_PER_HOUR` | 5, 60 | Rate limits for those live calls |

`.env` is git-ignored; `.env.example` lists every variable. Secrets reach only the backend
container through `environment:` entries.

## Tests

| What | Command | Notes |
|---|---|---|
| Backend offline tests | `docker build --target test backend` | routing, Delta log reader, job DAG, error formatting, health |
| Backend live tests | `docker compose exec backend python -m pytest -q -m live` | every demo (28) must pass its own checks; every "Do it wrong" mode (20) must verify extra checks |
| Frontend | `docker build --target test frontend` | Vitest: catalog completeness (pitfalls on every demo), every page and visual renders |
| In the UI | `#suite` | "Run all demos" calls every demo's `test` request |
| Isolated stack | `bash ~/.claude/skills/visual-lab/scripts/test-isolated.sh .` then `… . down` | builds a renamed copy on :3900 with no tunnel network and smoke-tests every demo through nginx |

## Local development

```bash
docker compose up -d --build unitycatalog spark mlflow backend
cd frontend && npm install && npm run dev        # Vite on :3000, proxies /api to localhost:8000
```

Backend code lives in `backend/app` (`demos/<area>.py`, one endpoint per feature returning the
DemoResult contract: `commands`, `data`, `checks`, `elapsedMs`). Pipeline projects for the
`spark-pipelines` CLI are in `backend/app/pipelines/`. The Spark server image is `spark/`
(`spark-defaults.conf` holds the Delta / UC / single-node settings); UC's config is
`unitycatalog/server.properties`.

## Deployment

`./pipeline-run.sh` (git-ignored) pulls the project, **removes all containers on the host**, starts
the `cloudflared-tunnel` container on `tunnel-net`, and runs `docker compose up -d --build`. The
tunnel routes to `http://rixus-frontend:80`, so the frontend container name and the `tunnel-net`
network must stay as they are. Only one app is live in the tunnel at a time.

## Notes and limits

- **Not Databricks itself.** Workspace UI, notebooks, Photon, serverless compute, SQL warehouses,
  Auto Loader (`cloudFiles`), Lakeflow pipeline expectations and UI, model serving, Delta Sharing,
  Lakebase, Genie/AI functions and UC permissions/ABAC/lineage are Databricks services and are
  explained but not run. The engines underneath are run for real.
- **Differences found in the OSS stack, shown as such in the demos:** UC OSS 0.7 blocks OPTIMIZE on
  catalog-managed tables; inline CHECK constraints at CREATE TABLE fail through the UC connector;
  OSS Spark cannot execute UC functions; MLflow's UC OSS store has no aliases; `create_auto_cdc_flow`
  fails on Delta targets with Spark 4.2 + Delta 4.4; Delta 4.4.1's extension breaks SQL parameter
  markers on Spark 4.2.0. Delta maintenance demos therefore use external tables.
- **Single node.** Spark runs `local[4]`; AQE skew thresholds are lowered in the skew demo so the
  effect shows on lab-sized data. Timings are illustrative, not benchmarks.
- **Public-site safety.** Inputs are clamped, no visitor code is executed, demos run one at a time,
  and live workspace calls are off by default, read-only and rate-limited.
