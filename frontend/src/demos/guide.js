// The overview page: what Databricks is, how to start a project with it (typed out in an animated
// terminal), and an honest pros / cons / when-to-use comparison. antiPatterns and troubleshooting
// also feed the #pitfalls page, next to every demo's own pitfalls.

export const OVERVIEW = {
  title: 'Databricks in one page',
  intro:
    'Databricks is a managed "lakehouse" platform: your data sits as open files (Delta Lake tables) in cloud object storage you own, ' +
    'Apache Spark clusters (or serverless compute) process it, Unity Catalog governs who can see what, and MLflow, Lakeflow Jobs and ' +
    'Lakeflow Declarative Pipelines run the ML and the production schedules. Databricks itself is a paid cloud service, so this lab runs ' +
    'the open-source engines Databricks built and still maintains, for real: Spark 4.2 over Spark Connect (the protocol Databricks Connect uses), ' +
    'Delta Lake 4.4, Unity Catalog OSS 0.7, MLflow 3.17 and Spark Declarative Pipelines, plus the official Databricks SDK. Every button runs real code on them.',
  facts: [
    ['Spark (Connect server)', '4.2.0'],
    ['Delta Lake', '4.4.1'],
    ['Unity Catalog OSS', '0.7.0'],
    ['MLflow', '3.17.2'],
    ['databricks-sdk (Python)', '0.155.0'],
    ['Licenses', 'Apache 2.0 (all engines)'],
  ],
  mentalModel: [
    ['Storage', 'Delta tables: Parquet files plus a _delta_log of JSON commits, in your cloud bucket (here: the /lakehouse volume).'],
    ['Compute', 'Spark clusters, SQL warehouses or serverless. Your code is a thin client that sends plans (Spark Connect).'],
    ['Governance', 'Unity Catalog: catalog.schema.object names, permissions, lineage, volumes, functions and models.'],
    ['Workloads', 'Notebooks, Jobs (DAGs of tasks), Declarative Pipelines, MLflow experiments and model serving.'],
  ],
  steps: [
    {
      title: 'Get a workspace and the CLI',
      text: 'Sign up for a Databricks workspace (the Free Edition works for learning), install the Databricks CLI and log in with OAuth. The CLI writes a profile to ~/.databrickscfg that the SDK, Databricks Connect and bundles all reuse.',
      commands: [
        'curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh',
        'databricks auth login --host https://<your-workspace>.cloud.databricks.com',
        'databricks current-user me',
      ],
    },
    {
      title: 'Start a project as an Asset Bundle',
      text: 'Bundles keep jobs, pipelines and code in git as databricks.yml, deployed per environment (dev, prod) instead of clicked together in the UI.',
      commands: [
        'databricks bundle init default-python',
        'cd my_project && databricks bundle validate',
        'databricks bundle deploy -t dev',
        'databricks bundle run -t dev my_project_job',
      ],
    },
    {
      title: 'Run Spark from your laptop with Databricks Connect',
      text: 'Databricks Connect is a Spark Connect client: your IDE builds the DataFrame plan and the workspace runs it. Pin the version to your cluster or serverless runtime.',
      commands: [
        'python -m venv .venv && . .venv/bin/activate',
        'pip install "databricks-connect==17.3.*"   # match your runtime: 17.3.* for Databricks Runtime 17.3 LTS',
      ],
      file: {
        name: 'main.py',
        lang: 'python',
        code: `from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().getOrCreate()   # or .clusterId("...")

trips = spark.read.table("samples.nyctaxi.trips")
(trips.groupBy("pickup_zip").count()
      .orderBy("count", ascending=False)
      .show(5))`,
      },
    },
    {
      title: 'Or run this lab locally (no account needed)',
      text: 'The same open-source engines on one machine: Spark Connect, Delta Lake, Unity Catalog and MLflow in Docker.',
      commands: [
        'docker network create tunnel-net   # once',
        'docker compose up -d --build',
        'open http://localhost:3000',
      ],
    },
  ],
  pros: [
    'Open formats: Delta tables are Parquet + JSON in your own bucket, readable by other engines (Delta Kernel, DuckDB, Trino, UniForm to Iceberg).',
    'One platform for ETL, SQL analytics, streaming and ML, with one governance layer (Unity Catalog) over all of it.',
    'Spark scales from a laptop sample to petabytes with the same DataFrame code; Photon and serverless make SQL fast without tuning.',
    'ACID transactions, time travel, MERGE and schema enforcement on cheap object storage.',
    'MLflow tracking and the model registry are built in, with models governed like tables.',
    'Strong production tooling: Jobs with retries and DAGs, Declarative Pipelines, Asset Bundles, a complete REST API and SDKs.',
  ],
  cons: [
    'Cost: you pay Databricks DBUs on top of cloud VMs; idle all-purpose clusters and oversized jobs burn money quietly.',
    'Spark has real overhead: seconds of scheduling for queries a single-node database answers in milliseconds.',
    'Many conveniences are Databricks-only (Photon, Auto Loader, serverless, UC ABAC, aliases in the OSS MLflow store), so local OSS testing is not 1:1, as this lab shows.',
    'Distributed-systems pitfalls (skew, shuffles, small files, driver memory) still need understanding.',
    'Not a transactional database: no row-level OLTP latency, no high-rate single-row updates.',
  ],
  whenToUse: [
    'Many teams share terabytes of data for ETL, BI and ML, and need one governed copy.',
    'Batch plus streaming pipelines with exactly-once guarantees into tables.',
    'Training and tracking ML models on large data, then batch scoring or serving them.',
    'Replacing a costly warehouse plus a separate data lake with one open-format lakehouse.',
  ],
  whenNotToUse: [
    'The data fits in one machine: DuckDB, Polars or Postgres will be faster and cheaper.',
    'An application backend with single-row reads and writes at low latency (use Postgres or another OLTP database).',
    'Sub-second event processing with per-event state (look at Flink or Kafka Streams).',
    'Very small teams with tiny budgets and simple reporting needs.',
  ],
  alternatives: [
    ['Snowflake', 'SQL-first managed warehouse with proprietary storage (Iceberg tables now too); less Spark/ML-native, simpler to operate.'],
    ['Google BigQuery', 'Serverless SQL warehouse, pay per query; great for SQL analytics, less for custom Spark and ML workloads.'],
    ['Microsoft Fabric / Synapse', 'Azure-integrated analytics with OneLake (Delta under the hood); tighter Power BI integration.'],
    ['AWS EMR / Glue + Iceberg', 'Run open-source Spark yourself on AWS: cheaper per hour, much more to build and operate.'],
    ['DuckDB / Polars', 'Single-node engines: far faster for data that fits on one machine, no cluster at all.'],
    ['Apache Flink', 'True event-at-a-time stream processing with large state; Spark streams in micro-batches.'],
  ],
  // How NOT to use Databricks: misuses of the tool as a whole (wrong job, wrong architecture).
  antiPatterns: [
    {
      dont: 'Using Delta tables as the database behind an app (one-row inserts and updates per request)',
      why: 'Every write is a commit with new files and a JSON log entry: hundreds of milliseconds each, small files pile up, and concurrent writers conflict. The "Row-by-row writes" demo shows it.',
      instead: 'Keep OLTP in Postgres (Databricks also offers a managed Postgres, Lakebase) and land changes in batches or via CDC.',
    },
    {
      dont: 'Pulling big tables to the driver with collect() or toPandas() and processing them in Python',
      why: 'The driver is one machine: it runs out of memory or hits spark.driver.maxResultSize, and the cluster sits idle.',
      instead: 'Filter and aggregate with DataFrame or SQL operations first; use pandas UDFs or pandas-on-Spark for per-group Python.',
    },
    {
      dont: 'Running scheduled jobs on long-lived all-purpose clusters, or leaving clusters up "just in case"',
      why: 'All-purpose compute costs more per DBU, and idle clusters bill by the minute.',
      instead: 'Use job clusters or serverless jobs, set auto-termination, and size clusters to the workload.',
    },
    {
      dont: 'Treating notebooks clicked together in the UI as the production system',
      why: 'No code review, no tests, no reproducible deployments; someone edits the notebook and production changes.',
      instead: 'Keep code in git and deploy jobs and pipelines with Asset Bundles through CI (see the "Jobs with the Databricks SDK" demo).',
    },
    {
      dont: 'Partitioning tables by high-cardinality columns (user_id, timestamp) "for speed"',
      why: 'Thousands of tiny partitions and files make every query slower and OPTIMIZE expensive.',
      instead: 'Leave small tables unpartitioned; use liquid clustering (CLUSTER BY) or Z-order for data skipping.',
    },
    {
      dont: 'Starting new work in hive_metastore, DBFS root or cloud mounts',
      why: 'These bypass Unity Catalog: no fine-grained permissions, lineage or auditing, and they are legacy features.',
      instead: 'Create catalogs and schemas in Unity Catalog and use volumes for files.',
    },
    {
      dont: 'Hard-coding personal access tokens or cloud keys in notebooks and repos',
      why: 'Tokens leak through git history and notebook exports and act with your full permissions.',
      instead: 'Use OAuth (databricks auth login), service principals for automation, and secret scopes (dbutils.secrets.get).',
    },
    {
      dont: 'Writing Python UDFs for things built-in functions already do',
      why: 'Row-at-a-time Python UDFs serialize every row out of the JVM and back; Photon cannot accelerate them.',
      instead: 'Use pyspark.sql.functions first, then pandas UDFs (vectorized, Arrow) when Python is really needed.',
    },
  ],
  // Errors people hit outside any single feature: install, connection, auth, config, version upgrades.
  troubleshooting: [
    {
      mistake: 'Pointing a Spark Connect / Databricks Connect client at the wrong host or port',
      symptom: 'No error at all: getOrCreate() and the first action retry silently and hang (over 10 minutes in this lab before giving up)',
      why: 'The gRPC client retries UNAVAILABLE errors with backoff, so a typo looks like a slow query.',
      fix: 'Check the URL (sc://host:15002 for OSS; workspace host + cluster id or serverless for Databricks Connect) and test the port first, e.g. `nc -vz host 15002`.',
      avoid: 'Fail fast in scripts: test with spark.range(1).collect() under a timeout right after creating the session.',
    },
    {
      mistake: 'Running Python UDFs when the client and the cluster use different Python versions',
      symptom: '[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.10 than that in driver: 3.12, PySpark cannot run with different minor versions.',
      why: 'UDFs are pickled on the client and run in Python workers on the cluster; pickles are not portable across minor versions.',
      fix: 'Use the client Python minor version the cluster runtime ships (this lab: 3.10, matching the apache/spark 4.2.0 image).',
      avoid: 'Check the runtime\'s Python version in the release notes before creating the virtualenv.',
    },
    {
      mistake: 'Creating a managed table without USING DELTA on open-source Spark with Unity Catalog',
      symptom: '(io.unitycatalog.client.ApiException) Unity Catalog does not support non-Delta managed table.',
      why: 'Open-source Spark defaults to Parquet; Databricks defaults to Delta. Managed tables in UC must be Delta.',
      fix: 'Add USING DELTA, or set spark.sql.sources.default=delta on the server (as this lab does).',
      avoid: 'Set the default format in cluster config so local and Databricks behave the same.',
    },
    {
      mistake: 'Calling the SDK, CLI or Connect without credentials',
      symptom: 'default auth: cannot configure default credentials, please check https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication to configure credentials for your preferred authentication method.',
      why: 'Unified auth found no token, profile, OAuth cache or cloud identity. Reproduced in the "Jobs with the Databricks SDK" demo.',
      fix: 'Run databricks auth login --host ..., or set DATABRICKS_HOST and DATABRICKS_TOKEN (or a client id/secret for a service principal).',
      avoid: 'Use named profiles (DATABRICKS_CONFIG_PROFILE) per workspace, and OAuth M2M for CI instead of personal tokens.',
    },
    {
      mistake: 'Constructing WorkspaceClient() for an unreachable or mistyped host',
      symptom: 'The constructor blocks for about 5 minutes, then logs "Failed to automatically resolve config from host metadata: ... Timed out"',
      why: 'databricks-sdk 0.155 fetches /.well-known/databricks-config while building the config, retrying for retry_timeout_seconds (default 300).',
      fix: 'Fix the host, or pass Config(host=..., retry_timeout_seconds=10) so a bad host fails fast.',
      avoid: 'Validate DATABRICKS_HOST at startup in services and keep retry budgets short in health checks.',
    },
    {
      mistake: 'Running the MLflow tracking server behind a service name or proxy without allowing that host',
      symptom: 'Invalid Host header - possible DNS rebinding attack detected',
      why: 'MLflow 3 servers only accept Host headers they were told about.',
      fix: 'Start it with --allowed-hosts mlflow,mlflow:5000,localhost (or your domain).',
      avoid: 'Put the allowed hosts in the server command in compose or your deployment manifest.',
    },
    {
      mistake: 'Giving the MLflow 3 tracking server a small memory limit',
      symptom: 'The container exits with code 137 (OOMKilled) right after start; clients then retry for minutes',
      why: 'MLflow 3 starts several background job workers (huey consumers, about 200 MB each) next to the web server. Seen in this lab at 1 GB.',
      fix: 'Set MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false if you do not use server-side jobs, and --workers 1 for small setups (about 350 MB).',
      avoid: 'Give it headroom, and set MLFLOW_HTTP_REQUEST_MAX_RETRIES / MLFLOW_HTTP_REQUEST_TIMEOUT on clients so outages surface quickly.',
    },
    {
      mistake: 'Sharing one storage volume between services that run as different users',
      symptom: 'Failed to create directory (in the Unity Catalog server log, when registering a model or volume under folders Spark created)',
      why: 'Spark (uid 185) created folders with mode 755; the UC server (uid 100) could not write inside them.',
      fix: 'Create files group/world-writable (umask 000, spark.hadoop.fs.permissions.umask-mode=000) or run the services with one uid.',
      avoid: 'In the cloud this is IAM: give the UC storage credential write access to the managed storage location.',
    },
    {
      mistake: 'Using SQL parameter markers with the Delta extension on Spark 4.2.0 + Delta 4.4.1',
      symptom: '[UNBOUND_SQL_PARAMETER] Found the unbound parameter: b.',
      why: 'A known incompatibility: DeltaSparkSessionExtension\'s parser drops the bound parameters (verified: works with the extension off).',
      fix: 'Use spark.sql("... {b}", b=value), which renders a safe literal, or DataFrame filters (see the SQL demo).',
      avoid: 'Test parameterized queries on the exact Spark + Delta versions you deploy; Databricks Runtime does not have this issue.',
    },
  ],
};
