// Every lab demo in one place: what it teaches, how its request flows through the stack
// (drives the animated diagram), which buttons it has, and which call the "Run all" suite uses.
// Flow steps are [from, to, label]; from === to means work done inside that node.

/** The participants a flow can show. Colors are CSS variables defined in App.css. */
export const NODE_META = {
  ui: { label: 'Browser', sub: 'React', color: 'var(--ui)' },
  api: { label: 'Your code', sub: 'PySpark client', color: 'var(--api)' },
  spark: { label: 'Spark cluster', sub: 'Connect server', color: 'var(--spark)' },
  uc: { label: 'Unity Catalog', sub: 'metastore', color: 'var(--uc)' },
  mlflow: { label: 'MLflow', sub: 'tracking server', color: 'var(--mlflow)' },
  store: { label: 'Storage', sub: 'Delta files', color: 'var(--store)' },
};

/** Left-to-right order of the participants in every diagram. */
export const NODE_ORDER = ['ui', 'api', 'spark', 'uc', 'mlflow', 'store'];

export function nodesFor(flow) {
  const used = new Set(flow.flatMap(([from, to]) => [from, to]));
  return NODE_ORDER.filter((id) => used.has(id));
}

export const CATEGORIES = [
  'Spark on Databricks',
  'Delta Lake',
  'Unity Catalog',
  'Ingestion & streaming',
  'Lakeflow pipelines',
  'Machine learning',
  'Jobs & platform',
  'Common mistakes',
];

const run = (path, extra = {}) => ({ id: 'run', label: 'Run it', method: 'POST', path, tone: 'accent', ...extra });
const wrong = (path, extra = {}) => ({
  id: 'mistake', label: 'Do it wrong', method: 'POST', path, tone: 'ghost', ...extra,
  params: (i) => ({ ...(extra.params ? extra.params(i) : {}), mistake: true }),
});

export const DEMOS = [
  // ---- Spark on Databricks ------------------------------------------------------------------
  {
    id: 'spark-connect',
    category: 'Spark on Databricks',
    title: 'Spark Connect & sessions',
    tagline: 'A thin client sends query plans; the cluster does the work',
    description: [
      'On Databricks your notebook or IDE never touches the data directly. Code builds a DataFrame, which is only a description of a query (a plan). Since Spark 3.4 that plan can be sent over gRPC to a remote cluster: Spark Connect. Databricks Connect, serverless notebooks and shared clusters all work this way.',
      'Press Run: the backend builds spark.range(n).sum(), which is a few hundred bytes of plan. The cluster generates and sums n numbers (n × 8 bytes) and sends back one row. Watch the bars: the plan and the answer are tiny compared to the work done.',
      'Because the client has no JVM, the old SparkContext and RDD APIs do not exist over Connect, and static configs cannot change once the cluster runs. "Do it wrong" calls them for real.',
    ],
    keyApis: ['SparkSession.builder.remote()', 'DatabricksSession.builder', 'spark.version', 'df.collect()'],
    useCases: ['IDE development against a remote cluster', 'Apps that query the lakehouse', 'Serverless notebooks'],
    snippet: {
      lang: 'python', name: 'basics.py',
      code: `spark = SparkSession.builder.remote("sc://spark:15002").getOrCreate()
# On Databricks: DatabricksSession.builder.serverless().getOrCreate()

df = spark.range(10_000_000).selectExpr("sum(id) AS total", "count(*) AS row_count")
row = df.collect()[0]          # only now does the plan travel to the cluster`,
    },
    inputs: [{ name: 'rows', label: 'Rows to generate (1k–50M)', type: 'number', default: 10000000, min: 1000, max: 50000000 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/basics/connect'],
      ['api', 'api', 'build the plan (no data)'],
      ['api', 'spark', 'gRPC ExecutePlan (bytes)'],
      ['spark', 'spark', 'job: generate + sum n rows'],
      ['spark', 'api', 'Arrow batch: 1 row'],
      ['api', 'ui', 'DemoResult + checks'],
    ],
    actions: [
      run('/api/demos/basics/connect', { label: 'Run on the cluster', params: (i) => ({ rows: i.rows }) }),
      wrong('/api/demos/basics/connect', { label: 'Use classic APIs (fails)', params: () => ({ rows: 1000 }) }),
    ],
    pitfalls: [
      {
        mistake: 'Porting old code that uses spark.sparkContext or RDDs to Spark Connect / serverless',
        symptom: '[JVM_ATTRIBUTE_NOT_SUPPORTED] Attribute `sparkContext` is not supported in Spark Connect as it depends on the JVM.',
        why: 'The Connect client is a thin Python library with no local JVM; SparkContext, RDDs and sc.broadcast live only inside the driver.',
        fix: 'Rewrite RDD code with DataFrame operations (map → select/withColumn, reduceByKey → groupBy().agg()), or run it on a dedicated (single-user) classic cluster.',
        avoid: 'Write new code against the DataFrame and SQL APIs only; run your tests with SparkSession.builder.remote() so Connect gaps show up early.',
        lang: 'python',
        wrongCode: 'rdd = spark.sparkContext.parallelize(range(5))\ntotal = rdd.map(lambda x: x * 2).sum()',
        rightCode: 'from pyspark.sql import functions as F\ntotal = spark.range(5).select((F.col("id") * 2).alias("x")).agg(F.sum("x")).first()[0]',
        reproduce: 'mistake',
      },
      {
        mistake: 'Changing a static cluster config from the session',
        symptom: '[CANNOT_MODIFY_STATIC_CONFIG] Cannot modify the value of the static Spark config: "spark.sql.warehouse.dir". SQLSTATE: 46110',
        why: 'Some settings are read once when the cluster starts (warehouse dir, extensions, catalog plugins, driver memory). spark.conf.set only changes runtime SQL settings.',
        fix: 'Set it in the cluster configuration (Spark config box, cluster policy, or spark-defaults.conf) and restart the cluster.',
        avoid: 'Check spark.conf.isModifiable("key") before setting configs in code.',
        reproduce: 'mistake',
      },
    ],
    visual: 'connect',
    test: { method: 'POST', path: '/api/demos/basics/connect?rows=1000000' },
  },
  {
    id: 'lazy-evaluation',
    category: 'Spark on Databricks',
    title: 'Lazy evaluation & the optimizer',
    tagline: 'Transformations build a plan; actions run it',
    description: [
      'Spark does nothing when you call where(), select() or groupBy(). Each call returns a new DataFrame describing more work. Only an action (collect, count, write, show) sends the whole plan to the Catalyst optimizer, which rewrites it and runs it as jobs.',
      'Press Run and watch the steps appear: five transformations, each taking microseconds and running zero Spark jobs, then one action that does all the work. Below, the optimized plan shows two rewrites you did not ask for: only the columns you use are read (column pruning), and the filter is pushed into the Delta scan so whole files can be skipped (predicate pushdown).',
      'Laziness has a cost when you reuse a DataFrame: every action recomputes it from the source. "Do it wrong" runs two actions on the same expensive DataFrame with and without cache() and counts the rows read each time.',
    ],
    keyApis: ['df.explain(mode="formatted")', 'df.cache()', 'df.unpersist()', 'PushedFilters', 'ReadSchema'],
    useCases: ['Reading query plans', 'Spotting full scans', 'Deciding when to cache'],
    snippet: {
      lang: 'python', name: 'basics.py',
      code: `trips = spark.read.table("samples.nyctaxi.trips")           # no job
long = trips.where(col("trip_distance") > 5)                # no job
agg = (long.select("pickup_zip", "fare_amount")
           .groupBy("pickup_zip").agg(avg("fare_amount"), count("*")))
top = agg.orderBy(desc("count(1)")).limit(5)                 # still no job
top.explain(mode="formatted")   # see pruning + pushdown
top.collect()                   # the action: one optimized job`,
    },
    inputs: [{ name: 'min_distance', label: 'Minimum trip distance (0–15 mi)', type: 'number', default: 5, min: 0, max: 15, step: 0.5 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/basics/lazy'],
      ['api', 'api', '5 transformations: plan only'],
      ['api', 'spark', 'collect(): send the plan'],
      ['spark', 'spark', 'Catalyst: prune + push down'],
      ['spark', 'store', 'scan 3 columns, skip files'],
      ['spark', 'api', 'top 5 rows'],
      ['api', 'ui', 'steps, plan, checks'],
    ],
    actions: [
      run('/api/demos/basics/lazy', { label: 'Build & run the query', params: (i) => ({ min_distance: i.min_distance }) }),
      wrong('/api/demos/basics/lazy', { label: 'Reuse it without cache', params: (i) => ({ min_distance: i.min_distance }) }),
    ],
    pitfalls: [
      {
        mistake: 'Calling several actions on the same expensive DataFrame without caching',
        symptom: 'Each count(), show() or write() re-reads and re-computes the whole lineage; jobs take N times longer than expected',
        why: 'DataFrames are recipes, not results. Nothing is kept between actions unless you ask for it.',
        fix: 'df.cache() (or persist()) before the first action, and unpersist() when done; or write the intermediate result to a Delta table.',
        avoid: 'Cache only what is reused and fits in memory. On serverless and shared clusters, prefer writing intermediate tables, since cache() may be unavailable or disk-backed.',
        lang: 'python',
        wrongCode: 'expensive = trips.where(...).withColumn(...)\nexpensive.count()\nexpensive.agg(avg("fare_per_mile")).collect()   # scans again',
        rightCode: 'expensive = trips.where(...).withColumn(...).cache()\nexpensive.count()                               # fills the cache\nexpensive.agg(avg("fare_per_mile")).collect()   # memory\nexpensive.unpersist()',
        reproduce: 'mistake',
      },
      {
        mistake: 'Timing transformations and concluding the code is fast',
        symptom: 'df = big.join(other).groupBy(...) "takes 2 ms", then the job fails or runs for an hour at the first write()',
        why: 'Transformations are only recorded; errors in data and the real cost surface at the action.',
        fix: 'Measure actions (or use the Spark UI / query profile) and read df.explain() for full scans and shuffles.',
        avoid: 'Trigger a cheap action (limit(10).collect()) while developing to validate logic early.',
      },
    ],
    visual: 'lazy',
    test: { method: 'POST', path: '/api/demos/basics/lazy?min_distance=5' },
  },
  {
    id: 'sql-dataframes',
    category: 'Spark on Databricks',
    title: 'SQL and DataFrames',
    tagline: 'Two front-ends, one engine, one plan',
    description: [
      'Databricks lets each person pick a language: analysts write SQL, engineers write PySpark, and both run on the same engine. spark.sql() and the DataFrame API compile to the same logical plan, so neither is faster. Temp views connect them: register a DataFrame and query it from SQL.',
      'Press Run: the same question (trips and average fare in one borough) is asked both ways. The answers match and both plans pick the same broadcast join.',
      'Mixing user input into SQL text is where it goes wrong. "Do it wrong" builds the query with an f-string and a classic injection payload, then passes the same input as a value. It also shows a real incompatibility in this exact stack: named :markers fail with the Delta 4.4.1 extension on Spark 4.2.0.',
    ],
    keyApis: ['spark.sql(query, **values)', 'createOrReplaceTempView()', 'spark.read.table()', 'df.join()'],
    useCases: ['Mixing SQL and Python in one pipeline', 'Parameterized queries', 'Reusable views'],
    snippet: {
      lang: 'python', name: 'basics.py',
      code: `sql_rows = spark.sql("""
    SELECT z.borough, count(*) AS trips, round(avg(t.fare_amount), 2) AS avg_fare
    FROM samples.nyctaxi.trips t JOIN main.basics.zones z ON t.pickup_zip = z.zip
    WHERE z.borough = {borough} GROUP BY z.borough""", borough=borough).collect()

df_rows = (trips.join(zones, trips.pickup_zip == zones.zip)
                .where(zones.borough == borough)
                .groupBy("borough").agg(count("*").alias("trips"),
                                        round(avg("fare_amount"), 2).alias("avg_fare"))).collect()`,
    },
    inputs: [{ name: 'borough', label: 'Borough', type: 'select', default: 'Manhattan', options: ['Manhattan', 'Brooklyn', 'Queens', 'Bronx'] }],
    flow: [
      ['ui', 'api', 'POST /api/demos/basics/sql'],
      ['api', 'spark', 'spark.sql(...) plan'],
      ['api', 'spark', 'DataFrame plan'],
      ['spark', 'uc', 'resolve table names'],
      ['spark', 'store', 'scan + broadcast join'],
      ['spark', 'api', 'two identical answers'],
      ['api', 'ui', 'DemoResult + checks'],
    ],
    actions: [
      run('/api/demos/basics/sql', { label: 'Ask both ways', params: (i) => ({ borough: i.borough }) }),
      wrong('/api/demos/basics/sql', { label: 'Inject into the SQL text', params: (i) => ({ borough: i.borough }) }),
    ],
    pitfalls: [
      {
        mistake: 'Building SQL with f-strings or string concatenation from user input',
        symptom: "Input Queens' OR '1'='1 turns WHERE borough = '...' into a filter that matches every row (all 40 zones in the demo)",
        why: 'The input becomes part of the SQL text, so quotes in it change the query itself.',
        fix: 'Pass values separately: spark.sql("... WHERE borough = {b}", b=value) renders a quoted literal, or use args={"b": value} with :b markers (where supported), or a DataFrame filter.',
        avoid: 'Never format identifiers or values from users into SQL; whitelist identifiers (table and column names) explicitly.',
        lang: 'python',
        wrongCode: 'spark.sql(f"SELECT * FROM zones WHERE borough = \'{user_input}\'")',
        rightCode: 'spark.sql("SELECT * FROM zones WHERE borough = {b}", b=user_input)\n# or: zones.where(col("borough") == user_input)',
        reproduce: 'mistake',
      },
      {
        mistake: 'Using named parameter markers on open-source Spark 4.2.0 with Delta 4.4.1',
        symptom: '[UNBOUND_SQL_PARAMETER] Found the unbound parameter: b. Please, fix `args` and provide a mapping of the parameter to either a SQL literal or collection constructor functions such as `map()`, `array()`, `struct()`. SQLSTATE: 42P02',
        why: 'A version-specific incompatibility: with io.delta.sql.DeltaSparkSessionExtension enabled the bound parameters are dropped (the same query works with the extension off). Databricks Runtime is not affected.',
        fix: 'Use the {name} formatter form (values are rendered as safe literals) or DataFrame filters on this stack.',
        avoid: 'Test parameterized SQL on the exact Spark and Delta versions you deploy.',
        reproduce: 'mistake',
      },
    ],
    visual: 'sql',
    test: { method: 'POST', path: '/api/demos/basics/sql?borough=Queens' },
  },
  {
    id: 'shuffle-aqe',
    category: 'Spark on Databricks',
    title: 'Shuffles, AQE & join strategies',
    tagline: 'Why groupBy and join move data, and how Spark adapts',
    description: [
      'groupBy and join need all rows with the same key on the same machine, so Spark writes every row to a shuffle and reads it back partitioned by key. A shuffle is often the most expensive part of a job. spark.sql.shuffle.partitions sets how many reduce tasks it uses (default 200).',
      'Press Run: the same aggregation runs with Adaptive Query Execution off and on. Off, it launches exactly the configured number of reduce tasks, mostly empty for 40 groups. On, AQE measures the real shuffle size at runtime and coalesces them into a few tasks. Then a join of the big trips table with a 40-row zones table: Spark broadcasts the small side to every task (no shuffle of the big side) unless broadcasting is disabled, in which case it falls back to a sort-merge join.',
      'Databricks enables AQE by default and also offers auto-optimized shuffle; you rarely need to hand-tune partitions, but you must recognize shuffles in plans and the Spark UI.',
    ],
    keyApis: ['spark.sql.adaptive.enabled', 'spark.sql.shuffle.partitions', 'spark.sql.autoBroadcastJoinThreshold', 'broadcast()'],
    useCases: ['Tuning slow aggregations', 'Joining facts with small dimensions', 'Reading the Spark UI'],
    snippet: {
      lang: 'python', name: 'basics.py',
      code: `spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "200")
trips.groupBy("pickup_zip").agg(sum("fare_amount")).collect()   # 200 reduce tasks

spark.conf.set("spark.sql.adaptive.enabled", "true")             # coalesces to a few

trips.join(zones, col("pickup_zip") == col("zip"))               # BroadcastHashJoin
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")     # -> SortMergeJoin`,
    },
    inputs: [{ name: 'partitions', label: 'shuffle.partitions (8–400)', type: 'number', default: 200, min: 8, max: 400 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/basics/shuffle'],
      ['api', 'spark', 'groupBy, AQE off'],
      ['spark', 'spark', 'map → shuffle → N reduce tasks'],
      ['api', 'spark', 'groupBy, AQE on'],
      ['spark', 'spark', 'coalesce partitions at runtime'],
      ['api', 'spark', 'join: broadcast vs sort-merge'],
      ['spark', 'api', 'task counts from the Spark UI'],
      ['api', 'ui', 'DemoResult + checks'],
    ],
    actions: [run('/api/demos/basics/shuffle', { label: 'Compare AQE off / on', params: (i) => ({ partitions: i.partitions }) })],
    pitfalls: [
      {
        mistake: 'Joining a big table with a small one and disabling or exceeding the broadcast threshold',
        symptom: 'The plan shows SortMergeJoin with Exchange on both sides; the whole fact table is shuffled for a 40-row lookup',
        why: 'Without a broadcast, Spark must co-partition both sides by the join key.',
        fix: 'Use broadcast(small_df) as a hint, or raise spark.sql.autoBroadcastJoinThreshold so the small side qualifies (keep it well under executor memory).',
        avoid: 'Check df.explain() for BroadcastHashJoin on star-schema joins; keep dimension tables small and filtered.',
        lang: 'python',
        wrongCode: 'spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")\ntrips.join(zones, "zip")',
        rightCode: 'from pyspark.sql.functions import broadcast\ntrips.join(broadcast(zones), "zip")',
      },
      {
        mistake: 'Hard-coding spark.sql.shuffle.partitions to a big number (or leaving 200 for tiny data) with AQE off',
        symptom: 'Hundreds of tasks that each process a few KB; scheduling overhead dominates and output has hundreds of tiny files',
        why: 'Without AQE the number of reduce tasks is fixed, whatever the data size.',
        fix: 'Turn AQE on (default on Databricks) or set shuffle.partitions to roughly total shuffle size / 128 MB.',
        avoid: 'Leave AQE and auto-optimized shuffle on; tune only after looking at the Spark UI.',
      },
    ],
    visual: 'shuffle',
    test: { method: 'POST', path: '/api/demos/basics/shuffle?partitions=200' },
  },
  {
    id: 'udfs',
    category: 'Spark on Databricks',
    title: 'Python UDFs vs built-ins',
    tagline: 'Same logic, very different speed',
    description: [
      'A Python user-defined function (UDF) lets you run any Python on each row, but the rows live in the JVM: Spark must serialize each batch to a Python worker process and read the results back. Built-in functions run inside the JVM (and on Databricks inside the Photon engine).',
      'Press Run: four implementations put fares into "low / mid / high" buckets over up to 2 million rows. All four agree on the counts; the bars show the time. The pandas UDF and the Arrow-optimized UDF ship columnar batches via Apache Arrow; the classic UDF pickles row by row.',
      'Use built-in functions (when, regexp_extract, from_json, …) first, pandas UDFs when you really need Python libraries, and avoid row-at-a-time pickled UDFs in hot paths.',
    ],
    keyApis: ['pyspark.sql.functions.when()', '@pandas_udf', '@udf(useArrow=True)', 'spark.sql.execution.pythonUDF.arrow.enabled'],
    useCases: ['Custom business logic', 'Calling Python libraries per row', 'Migrating pandas code'],
    snippet: {
      lang: 'python', name: 'basics.py',
      code: `builtin = when(col("fare") >= 30, "high").when(col("fare") >= 15, "mid").otherwise("low")

@pandas_udf(StringType())
def bucket_pandas(fare: pd.Series) -> pd.Series:      # one Arrow batch at a time
    return pd.Series(np.where(fare >= 30, "high", np.where(fare >= 15, "mid", "low")))

bucket_rows = udf(bucket, StringType(), useArrow=False)   # pickled, row by row`,
    },
    inputs: [{ name: 'rows', label: 'Rows (10k–2M)', type: 'number', default: 1000000, min: 10000, max: 2000000 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/basics/udfs'],
      ['api', 'spark', '4 plans, same result'],
      ['spark', 'spark', 'JVM built-in when()'],
      ['spark', 'spark', 'JVM ⇄ Python workers (Arrow / pickle)'],
      ['spark', 'api', 'counts + timings'],
      ['api', 'ui', 'race results'],
    ],
    actions: [run('/api/demos/basics/udfs', { label: 'Race the four', params: (i) => ({ rows: i.rows }) })],
    pitfalls: [
      {
        mistake: 'Writing a row-at-a-time Python UDF for logic a built-in function already covers',
        symptom: 'The job is several times slower than the SQL equivalent; the Spark UI shows a BatchEvalPython / ArrowEvalPython stage and busy Python workers',
        why: 'Every row is serialized from the JVM to Python and back; Photon and the optimizer cannot see inside a UDF.',
        fix: 'Replace it with pyspark.sql.functions (when, coalesce, regexp_*, date_*, from_json), or a SQL expression.',
        avoid: 'Reach for pandas_udf or useArrow=True only when Python is really needed.',
        lang: 'python',
        wrongCode: '@udf("string")\ndef bucket(fare):\n    return "high" if fare >= 30 else "mid" if fare >= 15 else "low"',
        rightCode: 'bucket = when(col("fare") >= 30, "high").when(col("fare") >= 15, "mid").otherwise("low")',
      },
      {
        mistake: 'Running UDFs with a client Python version different from the cluster',
        symptom: '[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.10 than that in driver: 3.12, PySpark cannot run with different minor versions.',
        why: 'UDFs are pickled on the client and unpickled by Python workers on the cluster.',
        fix: 'Create the client environment with the cluster runtime\'s Python minor version (this lab: 3.10).',
        avoid: 'Pin the Python version in your project (e.g. .python-version / requires-python) to the runtime you deploy on.',
      },
    ],
    visual: 'udfs',
    test: { method: 'POST', path: '/api/demos/basics/udfs?rows=200000' },
  },

  // ---- Delta Lake --------------------------------------------------------------------------
  {
    id: 'delta-log',
    category: 'Delta Lake',
    title: 'The transaction log',
    tagline: 'Parquet files + a JSON commit log = ACID tables',
    description: [
      'A Delta table is a folder of ordinary Parquet files plus a _delta_log folder. Every write adds one numbered JSON file (a commit) listing which data files it added and removed. Readers replay the log to know exactly which files make up the current version, so a reader never sees a half-finished write.',
      'Press Run: the table is created and then gets two inserts, one update and one delete. Each one appears as a commit with its add and remove actions; the counters show rows, files the log references (live) and files physically on disk. Tick "deletion vectors" to see UPDATE and DELETE mark rows as deleted instead of rewriting whole files.',
      '"Do it wrong" deletes a data file by hand, behind Delta\'s back, which is a common way people "clean up" storage. The log still points at the file, so the table stops reading until the surviving rows are rewritten.',
    ],
    keyApis: ['_delta_log/*.json', 'DESCRIBE HISTORY', 'DESCRIBE DETAIL', 'delta.enableDeletionVectors'],
    useCases: ['Understanding what a write costs', 'Auditing changes', 'Debugging "file not found" errors'],
    snippet: {
      lang: 'sql', name: 'delta.sql',
      code: `CREATE TABLE main.delta_lab.accounts (id INT, owner STRING, balance DOUBLE) USING DELTA;
INSERT INTO main.delta_lab.accounts VALUES (1,'ana',100.0),(2,'ben',250.0),(3,'cy',75.0);
INSERT INTO main.delta_lab.accounts VALUES (4,'dee',500.0),(5,'eli',20.0);
UPDATE main.delta_lab.accounts SET balance = balance + 50 WHERE id = 2;
DELETE FROM main.delta_lab.accounts WHERE id = 5;
DESCRIBE HISTORY main.delta_lab.accounts;   -- versions 0..4, read from _delta_log`,
    },
    inputs: [{ name: 'deletion_vectors', label: 'Enable deletion vectors', type: 'checkbox', default: false }],
    flow: [
      ['ui', 'api', 'POST /api/demos/delta/log'],
      ['api', 'spark', 'CREATE, INSERT ×2, UPDATE, DELETE'],
      ['spark', 'store', 'write Parquet files'],
      ['spark', 'store', 'atomically add 0000N.json'],
      ['api', 'store', 'read _delta_log'],
      ['api', 'ui', 'commits, files, checks'],
    ],
    actions: [
      run('/api/demos/delta/log', { label: 'Write and inspect the log', params: (i) => ({ deletion_vectors: i.deletion_vectors }) }),
      wrong('/api/demos/delta/log', { label: 'Delete a data file by hand', params: (i) => ({ deletion_vectors: i.deletion_vectors }) }),
    ],
    pitfalls: [
      {
        mistake: 'Deleting, moving or overwriting files inside a Delta table folder with cloud tools or rm',
        symptom: '[FAILED_READ_FILE.FILE_NOT_EXIST] Encountered error while reading file file:/lakehouse/ext/delta_lab_accounts/part-00000-….snappy.parquet. File does not exist. It is possible the underlying files have been updated.',
        why: 'The transaction log still references the file; Delta trusts the log, not a directory listing.',
        fix: 'Restore the file from backup or bucket versioning if you can. Otherwise salvage: read with spark.sql.files.ignoreMissingFiles=true and INSERT OVERWRITE the table (the lost rows are gone). Databricks also offers FSCK REPAIR TABLE (not in OSS Delta).',
        avoid: 'Only change Delta tables through Delta (DELETE, VACUUM, DROP). Lock down write access to table locations; with Unity Catalog managed tables, users never see the paths.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Treating a Delta folder as plain Parquet (spark.read.parquet on the path, or another engine without a Delta reader)',
        symptom: 'Deleted and updated rows come back, and row counts are too high: old file versions are read alongside current ones',
        why: 'Files removed in the log stay on disk until VACUUM; only the log says which ones are live.',
        fix: 'Read with format("delta") / spark.read.table, or a Delta-aware engine (Delta Kernel, DuckDB delta extension, UniForm for Iceberg readers).',
        avoid: 'Share tables through Unity Catalog or Delta Sharing, never by bucket path.',
      },
    ],
    visual: 'deltaLog',
    test: { method: 'POST', path: '/api/demos/delta/log' },
  },
  {
    id: 'time-travel',
    category: 'Delta Lake',
    title: 'Time travel & RESTORE',
    tagline: 'Query any earlier version, roll back with one command',
    description: [
      'Because old data files stay until VACUUM removes them, every version in the log can still be read: SELECT … VERSION AS OF 3 or TIMESTAMP AS OF \'…\'. This is how you audit "what did the table look like yesterday", reproduce an ML training set, or undo a bad write.',
      'Press Run: a small price list is inserted, updated (+10 %), loses a row and gains one. Click any version dot to see that snapshot; the right side shows the table after RESTORE to the version you picked in the input. RESTORE does not rewrite history: it adds a new commit whose content equals the old version.',
      'Time travel only reaches back as far as the files and log entries are kept: delta.logRetentionDuration (30 days) and delta.deletedFileRetentionDuration (7 days, which VACUUM honours).',
    ],
    keyApis: ['VERSION AS OF', 'TIMESTAMP AS OF', 'RESTORE TABLE … TO VERSION AS OF', 'DESCRIBE HISTORY'],
    useCases: ['Undoing a bad write', 'Reproducible ML training sets', 'Audits and debugging'],
    snippet: {
      lang: 'sql', name: 'delta.sql',
      code: `SELECT * FROM main.delta_lab.prices VERSION AS OF 1;
SELECT * FROM main.delta_lab.prices TIMESTAMP AS OF '2026-10-21 18:02:11';
RESTORE TABLE main.delta_lab.prices TO VERSION AS OF 1;   -- a new commit
DESCRIBE HISTORY main.delta_lab.prices;`,
    },
    inputs: [{ name: 'restore_to', label: 'RESTORE to version (0–4)', type: 'number', default: 1, min: 0, max: 4 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/delta/time-travel'],
      ['api', 'spark', '4 writes, then VERSION AS OF n'],
      ['spark', 'store', 'replay log up to version n'],
      ['api', 'spark', 'RESTORE TO VERSION AS OF'],
      ['spark', 'store', 'new commit = old snapshot'],
      ['api', 'ui', 'snapshots + checks'],
    ],
    actions: [
      run('/api/demos/delta/time-travel', { label: 'Travel and restore', params: (i) => ({ restore_to: i.restore_to }) }),
      wrong('/api/demos/delta/time-travel', { label: 'Ask for version 99', params: (i) => ({ restore_to: i.restore_to }) }),
    ],
    pitfalls: [
      {
        mistake: 'Querying a version that does not exist (or was cleaned up)',
        symptom: '[DELTA_VERSION_NOT_FOUND] Cannot time travel Delta table to version 99. Available versions: [0, 5].',
        why: 'Versions start at 0 and end at the latest commit; log cleanup removes old ones after delta.logRetentionDuration.',
        fix: 'Run DESCRIBE HISTORY to find valid versions or timestamps.',
        avoid: 'Pin versions you depend on (e.g. record the training version in MLflow) and raise retention for tables that need longer audit windows.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Relying on time travel as a backup after running VACUUM with a short retention',
        symptom: '[FAILED_READ_FILE.FILE_NOT_EXIST] when reading VERSION AS OF an older version (see the VACUUM demo)',
        why: 'VACUUM deletes the files old versions need; the log entry survives, the data does not.',
        fix: 'Restore from a real backup (DEEP CLONE, bucket versioning).',
        avoid: 'Keep the default 7-day retention or longer; use DEEP CLONE for backups.',
      },
    ],
    visual: 'timeTravel',
    test: { method: 'POST', path: '/api/demos/delta/time-travel?restore_to=1' },
  },
  {
    id: 'merge',
    category: 'Delta Lake',
    title: 'MERGE (upserts)',
    tagline: 'Update, insert and delete from a batch of changes in one commit',
    description: [
      'MERGE INTO matches a source of changes against a target table by key and decides per row: update matched rows, delete them, or insert new keys. It is the workhorse of incremental pipelines: applying CDC feeds, deduplicating late data, maintaining dimensions.',
      'Press Run: five change events are applied to a five-row customer table. Orange rows are updated, red ones deleted, green ones inserted, all in one atomic commit, with the metrics MERGE returns.',
      'MERGE needs each target row to match at most one source row. If a batch carries two events for the same key, Delta refuses rather than guessing. "Do it wrong" sends such a batch, then fixes it by keeping only the latest event per key.',
    ],
    keyApis: ['MERGE INTO … USING … ON', 'WHEN MATCHED THEN UPDATE / DELETE', 'WHEN NOT MATCHED THEN INSERT', 'WHEN NOT MATCHED BY SOURCE'],
    useCases: ['Applying CDC feeds', 'Slowly changing dimensions', 'Idempotent upserts'],
    snippet: {
      lang: 'sql', name: 'delta.sql',
      code: `MERGE INTO main.delta_lab.customers AS t
USING changes AS s ON t.id = s.id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.tier = s.tier, t.updated = s.updated
WHEN NOT MATCHED AND s.op != 'DELETE' THEN
  INSERT (id, name, tier, updated) VALUES (s.id, s.name, s.tier, s.updated);`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/delta/merge'],
      ['api', 'spark', 'MERGE INTO customers USING changes'],
      ['spark', 'spark', 'join source ⋈ target on id'],
      ['spark', 'store', 'rewrite touched files, 1 commit'],
      ['spark', 'api', 'updated / deleted / inserted'],
      ['api', 'ui', 'before → after'],
    ],
    actions: [run('/api/demos/delta/merge', { label: 'Apply 5 changes' }), wrong('/api/demos/delta/merge', { label: 'Merge duplicate keys' })],
    pitfalls: [
      {
        mistake: 'Merging a source that has several rows for the same key',
        symptom: '[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same target row in the Delta table in possibly conflicting ways.',
        why: 'SQL MERGE semantics are ambiguous when two source rows update one target row, so Delta stops instead of picking one.',
        fix: 'Deduplicate the source first: keep the latest event per key with row_number() OVER (PARTITION BY key ORDER BY seq DESC) = 1.',
        avoid: 'Make deduplication part of every CDC MERGE; in Lakeflow pipelines use AUTO CDC with SEQUENCE BY, which does this for you.',
        lang: 'sql',
        wrongCode: 'MERGE INTO customers t USING dupes s ON t.id = s.id\nWHEN MATCHED THEN UPDATE SET t.tier = s.tier',
        rightCode: 'MERGE INTO customers t USING (\n  SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY updated DESC) rn FROM dupes)\n  WHERE rn = 1) s ON t.id = s.id\nWHEN MATCHED THEN UPDATE SET t.tier = s.tier',
        reproduce: 'mistake',
      },
      {
        mistake: 'MERGE on a huge table without a selective condition',
        symptom: 'A small batch of changes takes as long as rewriting the table; the Spark UI shows a full scan of the target',
        why: 'Without a filter on partition / clustering columns, every target file must be checked for matches.',
        fix: 'Add predicates to the ON clause (e.g. t.date >= current_date() - 7) and cluster the target on the merge key.',
        avoid: 'Use liquid clustering on the key and enable deletion vectors so MERGE rewrites less.',
      },
    ],
    visual: 'merge',
    test: { method: 'POST', path: '/api/demos/delta/merge' },
  },
  {
    id: 'schema-evolution',
    category: 'Delta Lake',
    title: 'Schema enforcement & evolution',
    tagline: 'Bad writes are rejected; intended changes are explicit',
    description: [
      'A Delta table has a schema stored in its log, and every write is checked against it. An extra column, a missing type cast or a renamed field fails the write instead of silently corrupting the table. This is schema enforcement, and it is why Delta tables stay trustworthy while plain Parquet folders drift.',
      'Press Run: a DataFrame with a new country column is rejected, then accepted with mergeSchema=true; finally ALTER TABLE adds a column without rewriting any data. Old rows read new columns as NULL.',
      '"Do it wrong" appends ids as text, including "four". Spark casts the column to INT at write time and the bad value fails the whole write, so nothing partial lands. The fix validates with try_cast and quarantines rows that fail.',
    ],
    keyApis: ['.option("mergeSchema", "true")', 'ALTER TABLE … ADD COLUMNS', 'spark.databricks.delta.schema.autoMerge.enabled', 'try_cast()'],
    useCases: ['Adding fields to ingested data', 'Protecting tables from bad upstream data', 'Data contracts'],
    snippet: {
      lang: 'python', name: 'delta.py',
      code: `df_with_country.write.mode("append").saveAsTable(t)                  # rejected
df_with_country.write.mode("append").option("mergeSchema", "true").saveAsTable(t)
spark.sql(f"ALTER TABLE {t} ADD COLUMNS (plan STRING COMMENT 'pricing plan')")

clean = raw.withColumn("id", expr("try_cast(id AS INT)"))
clean.where("id IS NOT NULL").write.mode("append").saveAsTable(t)
clean.where("id IS NULL").write.mode("append").saveAsTable(t + "_quarantine")`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/delta/schema'],
      ['api', 'spark', 'append with a new column'],
      ['spark', 'store', 'schema check: rejected'],
      ['api', 'spark', 'mergeSchema / ALTER TABLE'],
      ['spark', 'store', 'new schema in metaData'],
      ['api', 'ui', 'schemas + rows'],
    ],
    actions: [run('/api/demos/delta/schema', { label: 'Evolve the schema' }), wrong('/api/demos/delta/schema', { label: 'Append text ids' })],
    pitfalls: [
      {
        mistake: 'Appending data whose columns differ from the table',
        symptom: '[DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta table. - A schema mismatch detected when writing to the Delta table (Table ID: …). To enable schema migration using DataFrameWriter or DataStreamWriter, please set: \'.option("mergeSchema", "true")\'.',
        why: 'Schema enforcement: the table\'s schema is the contract.',
        fix: 'If the new column is intended, add .option("mergeSchema", "true") or ALTER TABLE ADD COLUMNS; if not, select the expected columns before writing.',
        avoid: 'Evolve schemas deliberately; don\'t enable autoMerge globally, or a typo in upstream data becomes a new column.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Writing values that do not fit the column type',
        symptom: "[CAST_INVALID_INPUT] The value 'four' of the type \"STRING\" cannot be cast to \"INT\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead. SQLSTATE: 22018",
        why: 'With ANSI mode (default in Spark 4 and Databricks) invalid casts fail instead of becoming NULL.',
        fix: 'Validate with try_cast, write good rows, and route the NULLs to a quarantine table with the reason.',
        avoid: 'Add expectations / CHECK constraints at ingestion (bronze → silver) so bad records are caught with context.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Using overwrite with overwriteSchema=true to "fix" a schema error in production',
        symptom: 'The write succeeds, and downstream jobs then fail or silently get NULLs because columns were dropped or changed type',
        why: 'overwriteSchema replaces the table\'s schema and data; consumers are not warned.',
        fix: 'Restore the previous version (RESTORE TABLE … TO VERSION AS OF n) and evolve the schema additively.',
        avoid: 'Prefer additive changes; for renames and drops enable column mapping (delta.columnMapping.mode = name) and coordinate with consumers.',
      },
    ],
    visual: 'schema',
    test: { method: 'POST', path: '/api/demos/delta/schema' },
  },
  {
    id: 'optimize',
    category: 'Delta Lake',
    title: 'OPTIMIZE, Z-order & liquid clustering',
    tagline: 'Fewer, better-organized files let queries skip data',
    description: [
      'Every write adds files. Streaming jobs and frequent small appends leave thousands of tiny files, and each file costs a request and metadata overhead. OPTIMIZE compacts them. Delta also stores min/max statistics per file, so a query with WHERE key BETWEEN … can skip files whose range cannot match. That only helps if similar keys sit in the same files.',
      'Press Run: many small appends with keys scattered over 0–10 000 create files that each span almost the whole range (every bar is a file). After OPTIMIZE with Z-order or liquid clustering on key, each file covers a narrow slice, and the same range query opens far fewer files.',
      'Liquid clustering (CLUSTER BY) is the current recommendation on Databricks: it replaces partitioning and Z-order, can change keys without rewriting, and clusters incrementally. Predictive optimization runs OPTIMIZE for you on Unity Catalog managed tables.',
    ],
    keyApis: ['OPTIMIZE t ZORDER BY (col)', 'CREATE TABLE … CLUSTER BY (col)', 'delta.dataSkippingNumIndexedCols', '_metadata.file_path'],
    useCases: ['Compacting streaming output', 'Speeding up selective queries', 'Replacing partitioning'],
    snippet: {
      lang: 'sql', name: 'delta.sql',
      code: `-- many small appends -> many small files spanning the whole key range
OPTIMIZE main.delta_lab.clicks ZORDER BY (key);
-- or, recommended: cluster at table level
CREATE TABLE main.delta_lab.clicks (key INT, page STRING) CLUSTER BY (key);
OPTIMIZE main.delta_lab.clicks;

SELECT count(*), count(DISTINCT _metadata.file_path) AS files_read
FROM main.delta_lab.clicks WHERE key BETWEEN 4200 AND 4300;`,
    },
    inputs: [
      { name: 'files', label: 'Small appends (6–60)', type: 'number', default: 24, min: 6, max: 60 },
      { name: 'strategy', label: 'Strategy', type: 'select', default: 'zorder', options: ['zorder', 'liquid'] },
    ],
    flow: [
      ['ui', 'api', 'POST /api/demos/delta/optimize'],
      ['api', 'spark', 'N small appends'],
      ['spark', 'store', 'N files, wide min/max'],
      ['api', 'spark', 'OPTIMIZE (Z-order / liquid)'],
      ['spark', 'store', 'rewrite into sorted files'],
      ['api', 'spark', 'range query: files read'],
      ['api', 'ui', 'file ranges before/after'],
    ],
    actions: [run('/api/demos/delta/optimize', { label: 'Write, then OPTIMIZE', params: (i) => ({ files: i.files, strategy: i.strategy }) })],
    pitfalls: [
      {
        mistake: 'Partitioning by a high-cardinality column (user_id, event_time) to "speed up" queries',
        symptom: 'Thousands of folders with tiny files; listing and planning take longer than the query, and OPTIMIZE takes hours',
        why: 'Each partition value gets its own files; small partitions defeat compaction and data skipping.',
        fix: 'Recreate the table with CLUSTER BY on those columns (CREATE TABLE … CLUSTER BY … AS SELECT …).',
        avoid: 'Only partition very large tables by low-cardinality columns (e.g. date with ≥1 GB per day); otherwise use liquid clustering.',
      },
      {
        mistake: 'Z-ordering or clustering on a column nobody filters on (or on too many columns)',
        symptom: 'OPTIMIZE runs for a long time and query times do not change; files_read stays the same',
        why: 'Data skipping only works for columns in WHERE clauses that have statistics (first 32 columns by default); each extra Z-order column weakens the others.',
        fix: 'Pick 1–4 columns that queries actually filter on (check query history); ALTER TABLE … CLUSTER BY changes keys for liquid tables.',
        avoid: 'Let Databricks choose with CLUSTER BY AUTO on managed tables, or base keys on real query patterns.',
      },
    ],
    visual: 'optimize',
    test: { method: 'POST', path: '/api/demos/delta/optimize?files=12&strategy=zorder' },
  },
  {
    id: 'vacuum',
    category: 'Delta Lake',
    title: 'VACUUM & retention',
    tagline: 'Delete files no version needs, and lose time travel with them',
    description: [
      'Updates and deletes leave the old files on disk so older versions stay readable. VACUUM deletes files that are no longer referenced by any version within the retention period (7 days by default), which is how storage stops growing forever.',
      'Press Run: 100 rows are inserted and updated twice, which leaves stale files. DRY RUN with the default retention lists nothing (the files are minutes old). A retention below 7 days is refused, so the lab turns the safety check off (never do this in production) and vacuums with 0 hours: the grid shrinks to the live files, and time travel to version 1 now fails.',
      'On Databricks, predictive optimization runs VACUUM automatically for Unity Catalog managed tables.',
    ],
    keyApis: ['VACUUM t [RETAIN n HOURS] [DRY RUN]', 'delta.deletedFileRetentionDuration', 'spark.databricks.delta.retentionDurationCheck.enabled'],
    useCases: ['Controlling storage cost', 'Meeting deletion requirements (GDPR)', 'Cleaning up after big rewrites'],
    snippet: {
      lang: 'sql', name: 'delta.sql',
      code: `VACUUM main.delta_lab.orders DRY RUN;          -- what would be deleted (7-day default)
VACUUM main.delta_lab.orders RETAIN 0 HOURS;    -- refused unless the check is disabled
-- lab only: SET spark.databricks.delta.retentionDurationCheck.enabled = false
SELECT * FROM main.delta_lab.orders VERSION AS OF 1;  -- now fails: files are gone`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/delta/vacuum'],
      ['api', 'spark', 'INSERT, UPDATE ×2'],
      ['api', 'spark', 'VACUUM DRY RUN / RETAIN 0'],
      ['spark', 'store', 'delete unreferenced files'],
      ['api', 'spark', 'time travel to v1 (fails)'],
      ['api', 'ui', 'files before/after'],
    ],
    actions: [run('/api/demos/delta/vacuum', { label: 'Vacuum the table' })],
    pitfalls: [
      {
        mistake: 'Vacuuming with a retention shorter than your longest-running job or stream',
        symptom: '[DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT] The specified VACUUM retention period is too low and may corrupt this Delta table if any writes are in progress. … use a retention period of at least 168 hours.',
        why: 'A reader or writer that started before VACUUM can still need the files it deletes; streams resuming from an old checkpoint too.',
        fix: 'Use RETAIN 168 HOURS or more. If files were already removed, restore from backup; readers of old versions fail with FILE_NOT_EXIST.',
        avoid: 'Never disable retentionDurationCheck outside tests; let predictive optimization schedule VACUUM.',
      },
      {
        mistake: 'Expecting DELETE alone to remove personal data for compliance',
        symptom: 'After DELETE FROM users WHERE id = 42, the row is still readable with VERSION AS OF and still in the old Parquet files',
        why: 'DELETE writes a new version; the old files stay until VACUUM, and deletion vectors only mark rows.',
        fix: 'DELETE, then REORG TABLE … APPLY (PURGE) when deletion vectors are on, then VACUUM after the retention period.',
        avoid: 'Document the deletion window (retention + VACUUM schedule) in your GDPR process.',
      },
    ],
    visual: 'vacuum',
    test: { method: 'POST', path: '/api/demos/delta/vacuum' },
  },
  {
    id: 'change-data-feed',
    category: 'Delta Lake',
    title: 'Change Data Feed',
    tagline: 'Read row-level changes instead of whole tables',
    description: [
      'With delta.enableChangeDataFeed = true, Delta also records what changed in each commit: inserted rows, deleted rows, and both the old and new image of updated rows. Downstream jobs read only those changes (table_changes or readChangeFeed) instead of diffing whole tables.',
      'Press Run: a stock table gets an insert, an update, a delete and a MERGE. The feed comes back as a tape of events with _change_type and _commit_version. Updates appear twice, as a pre-image and a post-image.',
      'The feed only exists from the version where it was turned on. "Do it wrong" asks a plain table for its changes.',
    ],
    keyApis: ["table_changes('t', start)", 'readChangeFeed', 'delta.enableChangeDataFeed', '_change_type'],
    useCases: ['Incremental ETL to silver/gold', 'Syncing to search indexes or caches', 'Audit trails'],
    snippet: {
      lang: 'sql', name: 'delta.sql',
      code: `CREATE TABLE main.delta_lab.inventory (sku STRING, qty INT)
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
-- … INSERT, UPDATE, DELETE, MERGE …
SELECT sku, qty, _change_type, _commit_version
FROM table_changes('main.delta_lab.inventory', 1);
-- Python: spark.read.option("readChangeFeed", "true").option("startingVersion", 2).table(t)`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/delta/cdf'],
      ['api', 'spark', 'INSERT, UPDATE, DELETE, MERGE'],
      ['spark', 'store', 'data files + _change_data files'],
      ['api', 'spark', 'table_changes(t, 1)'],
      ['spark', 'api', 'change rows'],
      ['api', 'ui', 'change tape'],
    ],
    actions: [run('/api/demos/delta/cdf', { label: 'Change and read the feed' }), wrong('/api/demos/delta/cdf', { label: 'Read changes of a plain table' })],
    pitfalls: [
      {
        mistake: 'Reading changes from a table (or version range) without Change Data Feed enabled',
        symptom: '[DELTA_MISSING_CHANGE_DATA] Error getting change data for range [0 , 1] as change data was not recorded for version [0].',
        why: 'Change data is written only by commits made after delta.enableChangeDataFeed was set.',
        fix: 'ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true), then start reading from a version after that (see DESCRIBE HISTORY).',
        avoid: 'Enable CDF when creating tables that feed other systems.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Treating update_preimage rows as new data downstream',
        symptom: 'Every updated row is counted twice in aggregates built from the feed',
        why: 'Each update yields an update_preimage (old values) and an update_postimage (new values).',
        fix: "Filter _change_type IN ('insert', 'update_postimage', 'delete') and keep the latest per key by _commit_version.",
        avoid: 'Use MERGE or AUTO CDC with sequencing when applying feeds.',
      },
    ],
    visual: 'cdf',
    test: { method: 'POST', path: '/api/demos/delta/cdf' },
  },
  {
    id: 'constraints',
    category: 'Delta Lake',
    title: 'Constraints & generated columns',
    tagline: 'NOT NULL, CHECK, IDENTITY and computed columns enforced at write time',
    description: [
      'Delta tables can enforce data quality themselves: NOT NULL columns, CHECK constraints (any boolean expression), identity columns that hand out unique ids, and generated columns computed from others. Violations fail the whole transaction, so no partial batch is ever written.',
      'Press Run: two good payments commit; a batch with one negative amount is rejected as a whole; a NULL account is rejected. The table shows ids assigned by IDENTITY and paid_date computed from paid_at.',
      'In this open-source stack CHECK constraints must be added with ALTER TABLE: declaring them inline in CREATE TABLE through the Unity Catalog connector is not supported. "Do it wrong" shows the real error.',
    ],
    keyApis: ['ALTER TABLE … ADD CONSTRAINT … CHECK', 'NOT NULL', 'GENERATED ALWAYS AS IDENTITY', 'GENERATED ALWAYS AS (expr)'],
    useCases: ['Guarding gold tables', 'Surrogate keys', 'Derived partition/date columns'],
    snippet: {
      lang: 'sql', name: 'delta.sql',
      code: `CREATE TABLE main.delta_lab.payments (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  account STRING NOT NULL,
  amount DOUBLE,
  paid_at TIMESTAMP,
  paid_date DATE GENERATED ALWAYS AS (CAST(paid_at AS DATE))
) USING DELTA;
ALTER TABLE main.delta_lab.payments ADD CONSTRAINT positive_amount CHECK (amount > 0);`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/delta/constraints'],
      ['api', 'spark', 'CREATE + ADD CONSTRAINT'],
      ['api', 'spark', 'INSERT good / bad batches'],
      ['spark', 'spark', 'check every row before commit'],
      ['spark', 'store', 'commit only valid batches'],
      ['api', 'ui', 'accepted / rejected'],
    ],
    actions: [run('/api/demos/delta/constraints', { label: 'Insert good and bad rows' }), wrong('/api/demos/delta/constraints', { label: 'Inline CHECK in CREATE TABLE' })],
    pitfalls: [
      {
        mistake: 'Expecting a CHECK constraint to drop only the bad rows',
        symptom: '[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint positive_amount (amount > 0) violated by row with values: - amount : -5.0',
        why: 'Constraints are transactional: one violating row fails the whole write, good rows included.',
        fix: 'Split the batch first (good rows to the table, bad rows to a quarantine table), or use pipeline expectations with ON VIOLATION DROP ROW.',
        avoid: 'Use constraints as a last line of defence on curated tables; validate and quarantine earlier in the pipeline.',
        reproduce: 'run',
      },
      {
        mistake: 'Declaring CHECK constraints inline in CREATE TABLE on open-source Spark with Unity Catalog',
        symptom: '[UNSUPPORTED_FEATURE.TABLE_OPERATION] The feature is not supported: Table `main`.`delta_lab`.`payments_inline` does not support table constraint.',
        why: 'The OSS Unity Catalog connector (0.7) does not accept table constraints at create time; Databricks does.',
        fix: 'Create the table, then ALTER TABLE … ADD CONSTRAINT name CHECK (…).',
        avoid: 'Keep DDL portable: add constraints with ALTER TABLE in migrations.',
        reproduce: 'mistake',
      },
    ],
    visual: 'constraints',
    test: { method: 'POST', path: '/api/demos/delta/constraints' },
  },

  // ---- Unity Catalog -----------------------------------------------------------------------
  {
    id: 'uc-namespace',
    category: 'Unity Catalog',
    title: 'The three-level namespace',
    tagline: 'catalog.schema.table, everywhere',
    description: [
      'Unity Catalog is the governance layer of Databricks: one metastore per region holds catalogs, each catalog holds schemas, and schemas hold tables, views, volumes, functions and models. Everything has a three-part name and permissions are granted on any level. The same REST API (/api/2.1/unity-catalog) is served by Databricks and by the open-source server in this lab.',
      'Press Run: Spark creates a table, and the backend walks the metastore through the REST API: catalogs → schemas → tables. The highlighted table was created by Spark SQL and is visible to any engine that speaks UC. The session starts in main.default, like a Databricks workspace.',
      '"Do it wrong" uses a one-part table name from the wrong schema, and the Databricks-only USE CATALOG syntax, which open-source Spark spells differently.',
    ],
    keyApis: ['catalog.schema.table', 'USE catalog.schema', 'GET /api/2.1/unity-catalog/tables/{full_name}', 'current_catalog()'],
    useCases: ['Organizing data by domain or environment', 'Access control', 'Discovery and lineage'],
    snippet: {
      lang: 'python', name: 'governance.py',
      code: `spark.sql("CREATE TABLE IF NOT EXISTS main.gov.customers (id INT, email STRING, country STRING)")

UC = "http://unitycatalog:8080/api/2.1/unity-catalog"
requests.get(f"{UC}/catalogs").json()
requests.get(f"{UC}/schemas", params={"catalog_name": "main"}).json()
requests.get(f"{UC}/tables/main.gov.customers").json()   # columns, type, location`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/uc/namespace'],
      ['api', 'spark', 'CREATE TABLE main.gov.customers'],
      ['spark', 'uc', 'register table + schema'],
      ['api', 'uc', 'REST: list catalogs/schemas/tables'],
      ['uc', 'api', 'metadata'],
      ['api', 'ui', 'tree + checks'],
    ],
    actions: [run('/api/demos/uc/namespace', { label: 'Walk the metastore' }), wrong('/api/demos/uc/namespace', { label: 'Use one-part names' })],
    pitfalls: [
      {
        mistake: 'Using unqualified table names in jobs that run with a different current schema',
        symptom: '[TABLE_OR_VIEW_NOT_FOUND] The table or view `trips` cannot be found. Verify the spelling and correctness of the schema and catalog. Search path: [`system`.`builtin`, `system`.`session`, `main`.`default`].',
        why: 'One-part names resolve against current_catalog() and current_schema(), which differ between notebooks, jobs and users.',
        fix: 'Use three-part names (samples.nyctaxi.trips), or set USE catalog.schema at the start of the job.',
        avoid: 'Parameterize the catalog per environment (dev/prod) and always qualify names in production code.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Running Databricks SQL syntax on open-source Spark',
        symptom: "[PARSE_SYNTAX_ERROR] Syntax error at or near 'samples': extra input 'samples'. SQLSTATE: 42601 (line 1, pos 12)",
        why: 'USE CATALOG x is Databricks SQL; open-source Spark 4.2 accepts USE x or SET CATALOG x.',
        fix: 'Use USE catalog.schema, which works on both.',
        avoid: 'Test portable SQL on OSS Spark if you run it outside Databricks.',
        reproduce: 'mistake',
      },
    ],
    visual: 'namespace',
    test: { method: 'POST', path: '/api/demos/uc/namespace' },
  },
  {
    id: 'uc-managed-external',
    category: 'Unity Catalog',
    title: 'Managed vs external tables',
    tagline: 'Who owns the files, and what DROP does',
    description: [
      'A managed table lives in storage Unity Catalog chooses (under the catalog\'s or schema\'s managed location) and UC manages its lifecycle; it is the default and recommended kind. An external table points at a path you chose; UC governs access, but the files are yours.',
      'Press Run: the same three rows go into one table of each kind. The cards show their locations and table features. Then both are dropped: both disappear from the catalog, but the external table\'s files stay, and registering the path again brings the data back.',
      'In UC OSS 0.7 managed tables are catalog-managed: the catalog coordinates every commit. That blocks some maintenance commands in this version. "Do it wrong" shows two real errors.',
    ],
    keyApis: ['CREATE TABLE … (managed)', 'CREATE TABLE … LOCATION', 'DESCRIBE DETAIL', 'UNDROP TABLE (Databricks)'],
    useCases: ['Choosing table types', 'Sharing files with other tools', 'Recovering dropped tables'],
    snippet: {
      lang: 'sql', name: 'governance.sql',
      code: `CREATE TABLE main.gov.orders_managed (id INT, total DOUBLE);                 -- UC picks the path
CREATE TABLE main.gov.orders_external (id INT, total DOUBLE)
  LOCATION '/lakehouse/ext/gov_orders_external';                                -- you pick it
DROP TABLE main.gov.orders_managed;     -- data is UC's to clean up
DROP TABLE main.gov.orders_external;    -- only the registration goes
CREATE TABLE main.gov.orders_external LOCATION '/lakehouse/ext/gov_orders_external';  -- back`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/uc/managed-external'],
      ['api', 'spark', 'CREATE managed + external'],
      ['spark', 'uc', 'register, get locations'],
      ['spark', 'store', 'write files'],
      ['api', 'spark', 'DROP both, re-register external'],
      ['api', 'ui', 'locations + survivors'],
    ],
    actions: [run('/api/demos/uc/managed-external', { label: 'Create, drop, re-attach' }), wrong('/api/demos/uc/managed-external', { label: 'OPTIMIZE a managed table' })],
    pitfalls: [
      {
        mistake: 'Running OPTIMIZE on a catalog-managed table with open-source Unity Catalog 0.7',
        symptom: '[DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION] DATA_REORGANIZATION is blocked by the catalog for Catalog-Managed tables.',
        why: 'With catalog-managed commits UC coordinates writes, and Delta 4.4 + UC OSS 0.7 do not allow data reorganization yet. Databricks runs it (and predictive optimization does it for you).',
        fix: 'Use external tables for maintenance-heavy workloads on this stack, or run maintenance on Databricks.',
        avoid: 'Check which operations your UC/Delta versions support for managed tables before migrating.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Creating a managed table in a non-Delta format',
        symptom: '(io.unitycatalog.client.ApiException) Unity Catalog does not support non-Delta managed table.',
        why: 'UC managed tables must be Delta (or Iceberg on Databricks); open-source Spark defaults to Parquet.',
        fix: 'USING DELTA, or set spark.sql.sources.default=delta; keep other formats as external tables.',
        avoid: 'Set the default format in the cluster config.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Dropping a managed table assuming the data can be recovered from the bucket later',
        symptom: 'The table is gone from the catalog and its files are not reachable by name; on Databricks they are purged after the retention window',
        why: 'Managed means UC owns the lifecycle of the files.',
        fix: 'On Databricks run UNDROP TABLE within 7 days; otherwise restore from a DEEP CLONE backup.',
        avoid: 'Grant DROP/MODIFY narrowly and back up critical tables with DEEP CLONE.',
      },
    ],
    visual: 'managedExternal',
    test: { method: 'POST', path: '/api/demos/uc/managed-external' },
  },
  {
    id: 'uc-volumes',
    category: 'Unity Catalog',
    title: 'Volumes (governed files)',
    tagline: 'A catalog object for raw files: CSVs, images, models, wheels',
    description: [
      'Not all data is tabular. A volume is a Unity Catalog object that represents a folder of files, with the same permissions model as tables (READ VOLUME, WRITE VOLUME). On Databricks it is mounted at /Volumes/catalog/schema/volume/… for Spark, Python, the CLI and shell commands. Volumes replace DBFS mounts.',
      'Press Run: a managed volume main.gov.landing is created through the UC REST API, three partner CSV files are dropped into it, Spark reads the whole folder and saves it as a governed Delta table.',
      'The /Volumes path only exists where Databricks mounts it. Outside Databricks you address the volume by its storage location, as this lab does. "Do it wrong" uses the Databricks path here.',
    ],
    keyApis: ['CREATE VOLUME', '/Volumes/<catalog>/<schema>/<volume>/', 'POST /api/2.1/unity-catalog/volumes', 'READ VOLUME / WRITE VOLUME'],
    useCases: ['Landing raw partner files', 'Storing ML artifacts and images', 'Libraries and init scripts'],
    snippet: {
      lang: 'python', name: 'governance.py',
      code: `requests.post(f"{UC}/volumes", json={"catalog_name": "main", "schema_name": "gov",
                                     "name": "landing", "volume_type": "MANAGED"})
# On Databricks: spark.read.csv("/Volumes/main/gov/landing/exports")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(storage_location + "/exports")
df.write.mode("overwrite").saveAsTable("main.gov.signups")`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/uc/volumes'],
      ['api', 'uc', 'create / get volume'],
      ['api', 'store', 'write 3 CSV files'],
      ['api', 'spark', 'read folder, saveAsTable'],
      ['spark', 'store', 'Delta table'],
      ['api', 'ui', 'files → rows'],
    ],
    actions: [run('/api/demos/uc/volumes', { label: 'Land files, load table' }), wrong('/api/demos/uc/volumes', { label: 'Read /Volumes/... here' })],
    pitfalls: [
      {
        mistake: 'Using /Volumes/… or dbfs:/ paths in code that also runs outside Databricks (tests, local Spark, other engines)',
        symptom: '[PATH_NOT_FOUND] Path does not exist: file:/Volumes/main/gov/landing/exports. SQLSTATE: 42K03',
        why: '/Volumes is a FUSE mount that Databricks provides on its compute; elsewhere it is just a missing local folder.',
        fix: 'Resolve the volume\'s storage_location through the UC API (or config) when running outside Databricks.',
        avoid: 'Keep paths in configuration per environment, not hard-coded.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Using volumes as a database: many small files written and read one by one',
        symptom: 'Listing and reading thousands of tiny JSON files takes minutes; queries over the folder are slow',
        why: 'Object storage has per-file latency; Spark plans one task split per small file.',
        fix: 'Ingest files incrementally into a Delta table (Auto Loader / streaming) and query the table.',
        avoid: 'Use volumes for landing and unstructured files; keep queryable data in tables.',
      },
    ],
    visual: 'volumes',
    test: { method: 'POST', path: '/api/demos/uc/volumes' },
  },
  {
    id: 'uc-functions',
    category: 'Unity Catalog',
    title: 'Functions in Unity Catalog',
    tagline: 'Governed, reusable logic: masking, business rules, AI tools',
    description: [
      'Unity Catalog can store functions as catalog objects (catalog.schema.function) with permissions, comments and lineage. On Databricks they are used for column masks and row filters, shared business logic in SQL, and as tools for AI agents.',
      'Press Run: a Python masking function main.gov.mask_email is registered through the UC REST API and listed back. Calling it from open-source Spark fails: the OSS Spark connector cannot execute UC functions (Databricks can). The demo then applies the same masking with a SQL function inside Spark, so the result is visible.',
      'Temporary functions live only in the session that created them. "Do it wrong" calls one from a second session.',
    ],
    keyApis: ['CREATE FUNCTION catalog.schema.fn', 'POST /api/2.1/unity-catalog/functions', 'ALTER TABLE … SET MASK fn', 'CREATE TEMPORARY FUNCTION'],
    useCases: ['Column masking of PII', 'Shared business rules', 'Agent tools'],
    snippet: {
      lang: 'sql', name: 'governance.sql',
      code: `-- On Databricks (UC executes it, and it can be a column mask):
CREATE FUNCTION main.gov.mask_email(email STRING) RETURNS STRING
  LANGUAGE PYTHON AS $$ return email[0] + '***@' + email.split('@')[1] $$;
ALTER TABLE main.gov.contacts ALTER COLUMN email SET MASK main.gov.mask_email;

-- Portable SQL function for OSS Spark:
CREATE OR REPLACE TEMPORARY FUNCTION mask_email(email STRING) RETURNS STRING
  RETURN concat(left(email, 1), '***@', split_part(email, '@', 2));`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/uc/functions'],
      ['api', 'uc', 'register mask_email'],
      ['api', 'spark', 'SELECT main.gov.mask_email(...)'],
      ['spark', 'uc', 'resolve function (unsupported)'],
      ['api', 'spark', 'SQL function instead'],
      ['api', 'ui', 'masked emails'],
    ],
    actions: [run('/api/demos/uc/functions', { label: 'Register and apply' }), wrong('/api/demos/uc/functions', { label: 'Temp function, other session' })],
    pitfalls: [
      {
        mistake: 'Expecting open-source Spark to execute functions registered in Unity Catalog',
        symptom: '[MISSING_CATALOG_ABILITY.FUNCTIONS] Catalog main does not support functions. SQLSTATE: 0A000',
        why: 'The UC OSS Spark connector (0.7) implements tables only; executing UC functions, masks and row filters is a Databricks feature.',
        fix: 'Run on Databricks, or define the logic as a SQL/temporary function (or a view) in OSS Spark.',
        avoid: 'Know which UC features your engine supports before relying on them for security.',
        reproduce: 'run',
      },
      {
        mistake: 'Creating TEMPORARY functions in one notebook or job and calling them from another',
        symptom: 'The other session cannot resolve the name. Here: [MISSING_CATALOG_ABILITY.FUNCTIONS] Catalog main does not support functions. (On Databricks: [UNRESOLVED_ROUTINE] Cannot resolve routine `mask_email`.)',
        why: 'Temporary functions and views belong to the SparkSession that created them.',
        fix: 'Create a permanent function in a schema (CREATE FUNCTION catalog.schema.fn) or re-create it in each session.',
        avoid: 'Put shared logic in UC functions or a Python package, not in session state.',
        reproduce: 'mistake',
      },
    ],
    visual: 'functions',
    test: { method: 'POST', path: '/api/demos/uc/functions' },
  },

  // ---- Ingestion & streaming ---------------------------------------------------------------
  {
    id: 'streaming-files',
    category: 'Ingestion & streaming',
    title: 'Incremental file ingestion',
    tagline: 'Read only new files, exactly once, thanks to the checkpoint',
    description: [
      'Most lakehouse data arrives as files in a landing folder. Structured Streaming reads a folder as a stream: each run processes only the files it has not seen and records its progress in a checkpoint folder (offsets, commits, source log). Databricks Auto Loader (format("cloudFiles")) is this same engine plus file notification and schema inference.',
      'Press Run: the first run ingests the files that are already there. The second run finds nothing new and reads 0 rows. Then two files arrive and the third run reads only those. trigger(availableNow=True) processes everything available and stops, so a scheduled job gets streaming\'s exactly-once bookkeeping without a cluster running all day.',
      '"Do it wrong" leaves out the two things a file stream needs: an explicit schema and a checkpointLocation.',
    ],
    keyApis: ['spark.readStream.schema(...).json(path)', 'trigger(availableNow=True)', 'checkpointLocation', 'cloudFiles (Auto Loader)'],
    useCases: ['Landing-zone ingestion', 'Scheduled incremental loads', 'Bronze tables'],
    snippet: {
      lang: 'python', name: 'streaming.py',
      code: `source = (spark.readStream.schema("event_id LONG, user STRING, page STRING")
               .json("/lakehouse/landing/clicks")
               .withColumn("ingested_at", current_timestamp()))
# On Databricks: spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")...

(source.writeStream
       .option("checkpointLocation", "/lakehouse/checkpoints/clicks_bronze")
       .trigger(availableNow=True)
       .toTable("main.stream.clicks_bronze")
       .awaitTermination())`,
    },
    inputs: [{ name: 'batches', label: 'Files in the first run (1–8)', type: 'number', default: 3, min: 1, max: 8 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/stream/files'],
      ['api', 'store', 'land JSON files'],
      ['api', 'spark', 'readStream … availableNow'],
      ['spark', 'store', 'list files, skip seen ones'],
      ['spark', 'store', 'append + write checkpoint'],
      ['api', 'ui', 'runs + checkpoint'],
    ],
    actions: [
      run('/api/demos/stream/files', { label: 'Ingest in three runs', params: (i) => ({ batches: i.batches }) }),
      wrong('/api/demos/stream/files', { label: 'No schema, no checkpoint', params: (i) => ({ batches: i.batches }) }),
    ],
    pitfalls: [
      {
        mistake: 'Starting a file stream without a schema',
        symptom: "Schema must be specified when creating a streaming source DataFrame. If some files already exist in the directory, then depending on the file format you may be able to create a static DataFrame on that directory with 'spark.read.load(directory)' and infer schema from it.",
        why: 'A stream cannot sample files that do not exist yet, so plain Structured Streaming requires the schema up front.',
        fix: 'Pass .schema(...), or on Databricks use Auto Loader with cloudFiles.schemaLocation, which infers and evolves the schema.',
        avoid: 'Keep the bronze schema explicit (or Auto Loader with schema hints) and capture unknown fields in _rescued_data.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Writing a stream without a checkpoint location (or sharing / deleting one)',
        symptom: 'checkpointLocation must be specified either through option("checkpointLocation", ...) or SparkSession.conf.set("spark.sql.streaming.checkpointLocation", ...).',
        why: 'The checkpoint is the stream\'s memory of what it processed; without it there is no exactly-once.',
        fix: 'Give each stream its own checkpoint folder. If you deleted it, the next run reprocesses everything: clean the target first or deduplicate.',
        avoid: 'Never share a checkpoint between two queries, and treat checkpoints like data (back them up, don\'t "clean" them).',
        reproduce: 'mistake',
      },
    ],
    visual: 'fileStream',
    test: { method: 'POST', path: '/api/demos/stream/files?batches=3' },
  },
  {
    id: 'delta-stream-source',
    category: 'Ingestion & streaming',
    title: 'Delta tables as streams',
    tagline: 'Chain tables incrementally: bronze → silver',
    description: [
      'A Delta table is also a streaming source: readStream.table() reads each new commit as it appears. Chaining tables this way (bronze → silver → gold) means every layer only processes what is new, with checkpoints for exactly-once.',
      'Press Run: five orders are appended to bronze and streamed into silver (negative amounts filtered, tax added). Three more are appended and the second run reads only those three.',
      'A streaming read assumes the source only grows. An UPDATE or DELETE in the source rewrites files, so the stream stops. "Do it wrong" updates a bronze row and shows the error and the skipChangeCommits workaround.',
    ],
    keyApis: ['spark.readStream.table()', 'skipChangeCommits', 'readChangeFeed', 'trigger(availableNow=True)'],
    useCases: ['Medallion pipelines', 'Incremental aggregates', 'Fan-out from one bronze table'],
    snippet: {
      lang: 'python', name: 'streaming.py',
      code: `stream = (spark.readStream.table("main.stream.orders_bronze")
               .where("amount > 0")
               .withColumn("amount_with_tax", round(col("amount") * 1.23, 2)))
(stream.writeStream.option("checkpointLocation", ckpt)
       .trigger(availableNow=True).toTable("main.stream.orders_silver").awaitTermination())`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/stream/delta-source'],
      ['api', 'spark', 'INSERT into bronze'],
      ['api', 'spark', 'readStream.table(bronze)'],
      ['spark', 'store', 'read new commits only'],
      ['spark', 'store', 'append to silver'],
      ['api', 'ui', 'runs + silver rows'],
    ],
    actions: [run('/api/demos/stream/delta-source', { label: 'Stream bronze → silver' }), wrong('/api/demos/stream/delta-source', { label: 'UPDATE the source' })],
    pitfalls: [
      {
        mistake: 'Updating or deleting rows in a table that a stream reads from',
        symptom: '[DELTA_SOURCE_TABLE_IGNORE_CHANGES] Detected a data update (for example UPDATE …) in the source table at version 3. This is currently not supported. If this is going to happen regularly and you are okay to skip changes, set the option \'skipChangeCommits\' to \'true\'.',
        why: 'An update rewrites files that contain rows the stream already emitted; a plain append-only reader cannot express that change.',
        fix: "Restart with .option('skipChangeCommits', 'true') to ignore such commits (the changes do not propagate), or read the Change Data Feed (readChangeFeed) and MERGE downstream.",
        avoid: 'Keep streaming sources append-only (bronze), and do corrections downstream or via CDF.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Leaving a stream with trigger(processingTime) running 24/7 for data that arrives hourly',
        symptom: 'A cluster billed around the clock processing empty micro-batches',
        why: 'Continuous triggers keep compute up even when there is nothing to do.',
        fix: 'Use trigger(availableNow=True) in a scheduled job, or a file-arrival / table-update trigger in Lakeflow Jobs.',
        avoid: 'Match the trigger to the latency you really need.',
      },
    ],
    visual: 'deltaSource',
    test: { method: 'POST', path: '/api/demos/stream/delta-source' },
  },
  {
    id: 'medallion',
    category: 'Ingestion & streaming',
    title: 'Medallion architecture',
    tagline: 'Bronze keeps everything, silver is clean, gold answers questions',
    description: [
      'The medallion pattern layers tables by quality. Bronze stores raw input exactly as received (even broken lines), so you can always reprocess. Silver parses, validates and de-duplicates. Gold holds business-level aggregates for dashboards and ML. Bad records go to a quarantine table with a reason instead of disappearing.',
      'Press Run: ten raw lines arrive with two duplicates, a non-JSON line, a negative amount and a missing amount. Bronze keeps all ten, silver keeps the five valid unique events, quarantine explains the other three, and gold sums revenue per store. A check confirms gold reconciles with silver.',
      'Declarative Pipelines (next category) express the same layers as table definitions with expectations; this demo builds them by hand so each step is visible.',
    ],
    keyApis: ["readStream.format('text')", 'from_json()', '_metadata.file_name', 'SELECT DISTINCT'],
    useCases: ['Any ingestion pipeline', 'Reprocessing after bug fixes', 'Data quality tracking'],
    snippet: {
      lang: 'sql', name: 'medallion.sql',
      code: `-- bronze: every raw line + source file (streamed in)
CREATE TABLE main.stream.sales_silver AS
SELECT DISTINCT p.event_id, p.store, p.amount
FROM (SELECT from_json(raw, 'event_id INT, store STRING, amount DOUBLE') AS p FROM main.stream.sales_bronze)
WHERE p.event_id IS NOT NULL AND p.amount > 0;

CREATE TABLE main.stream.sales_gold AS
SELECT store, count(*) AS sales, round(sum(amount), 2) AS revenue FROM main.stream.sales_silver GROUP BY store;`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/stream/medallion'],
      ['api', 'store', 'land 2 raw files'],
      ['api', 'spark', 'bronze: stream raw text'],
      ['spark', 'spark', 'silver: parse, validate, dedupe'],
      ['spark', 'store', 'quarantine + gold tables'],
      ['api', 'ui', 'layer counts'],
    ],
    actions: [run('/api/demos/stream/medallion', { label: 'Run the layers' })],
    pitfalls: [
      {
        mistake: 'Parsing and filtering in the very first table, dropping what does not parse',
        symptom: 'After a schema change upstream, rows are silently missing and cannot be recovered: the raw input is gone',
        why: 'Without a raw bronze copy there is nothing to reprocess once the parser is fixed.',
        fix: 'Re-ingest from the source if it still exists; then add a raw bronze layer.',
        avoid: 'Bronze = raw + metadata (file name, ingest time); validate in silver and quarantine failures with a reason.',
      },
      {
        mistake: 'Building gold tables straight from bronze',
        symptom: 'Dashboards double-count retries and duplicates; numbers do not reconcile between reports',
        why: 'Raw data contains duplicates, late and malformed events.',
        fix: 'Rebuild gold from a de-duplicated silver table and add reconciliation checks (gold totals = silver totals).',
        avoid: 'Make silver the only source for gold, and test the reconciliation in CI or pipeline expectations.',
      },
    ],
    visual: 'medallion',
    test: { method: 'POST', path: '/api/demos/stream/medallion' },
  },

  // ---- Lakeflow pipelines ------------------------------------------------------------------
  {
    id: 'declarative-pipeline',
    category: 'Lakeflow pipelines',
    title: 'Declarative pipelines',
    tagline: 'Declare tables; the engine works out order, increments and retries',
    description: [
      'Lakeflow Declarative Pipelines (formerly Delta Live Tables) let you declare the tables you want, as SQL or Python functions, instead of writing the jobs that build them. The engine reads the definitions, builds a dataflow graph, runs flows in dependency order, keeps streaming tables incremental and recomputes materialized views. Databricks donated the core to Apache Spark as Spark Declarative Pipelines (Spark 4.1+); this lab runs that open-source core with the real spark-pipelines CLI.',
      'Press Run: a file of new orders lands, and the pipeline runs. orders_raw (Python, streaming table) ingests only the new file, orders_clean (SQL, streaming table) drops invalid amounts, and two materialized views aggregate. The graph lights up as the event log reports each flow. Run again to add more orders; "Reset" starts over.',
      'Expectations, Auto Loader, serverless and the pipeline UI are Databricks additions on top. "Do it wrong" runs dry-runs that catch two classic mistakes before any data moves.',
    ],
    keyApis: ['@dp.table / @dp.materialized_view', 'CREATE STREAMING TABLE … AS SELECT FROM STREAM(…)', 'spark-pipelines run / dry-run', 'spark-pipeline.yml'],
    useCases: ['Production ETL without hand-written orchestration', 'Incremental medallion pipelines', 'Data quality with expectations'],
    snippet: {
      lang: 'python', name: 'pipelines/orders/transformations/ingest.py',
      code: `from pyspark import pipelines as dp

@dp.table(name="orders_raw", comment="Raw orders, ingested incrementally")
def orders_raw():
    return (spark.readStream.schema("order_id INT, customer STRING, store STRING, amount DOUBLE")
                 .json("/lakehouse/landing/sdp_orders"))

@dp.materialized_view(name="revenue_by_store")
def revenue_by_store():
    return spark.read.table("orders_clean").groupBy("store").agg(count("*").alias("orders"), sum("amount").alias("revenue"))

# clean.sql:  CREATE STREAMING TABLE orders_clean AS SELECT * FROM STREAM(orders_raw) WHERE amount > 0;
# run:        spark-pipelines run --spec orders/spark-pipeline.yml`,
    },
    inputs: [{ name: 'new_orders', label: 'New orders in this file (1–30)', type: 'number', default: 6, min: 1, max: 30 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/pipelines/run'],
      ['api', 'store', 'land orders_N.json'],
      ['api', 'spark', 'spark-pipelines run (graph)'],
      ['spark', 'spark', 'flows in dependency order'],
      ['spark', 'uc', 'register tables in main.sdp'],
      ['spark', 'store', 'write tables + checkpoints'],
      ['api', 'ui', 'event log + counts'],
    ],
    actions: [
      run('/api/demos/pipelines/run', { label: 'Land a file & run', params: (i) => ({ new_orders: i.new_orders }) }),
      { id: 'reset', label: 'Reset and run', method: 'POST', path: '/api/demos/pipelines/run', tone: 'store', params: (i) => ({ new_orders: i.new_orders, reset: true }) },
      wrong('/api/demos/pipelines/run', { label: 'Dry-run broken pipelines', params: (i) => ({ new_orders: i.new_orders }) }),
    ],
    pitfalls: [
      {
        mistake: 'Reading a dataset that no definition in the pipeline creates (a typo, or a table from another pipeline without its full name)',
        symptom: '[TABLE_OR_VIEW_NOT_FOUND] The table or view `main`.`sdp_broken`.`orders_cleaned` cannot be found. Verify the spelling and correctness of the schema and catalog.',
        why: 'Unqualified names resolve to datasets in the pipeline\'s own catalog and schema; the graph is validated before running.',
        fix: 'Fix the name, or use the full catalog.schema.table name for external inputs.',
        avoid: 'Run spark-pipelines dry-run (or "Validate" in the Databricks UI) in CI before deploying.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Running a pipeline whose target schema does not exist',
        symptom: '[SCHEMA_NOT_FOUND] The schema `main`.`sdp_broken` cannot be found. Verify the spelling and correctness of the schema and catalog.',
        why: 'Open-source SDP does not create the target schema for you.',
        fix: 'CREATE SCHEMA IF NOT EXISTS catalog.schema before the run (or in your bundle / deployment).',
        avoid: 'Create catalogs and schemas as part of infrastructure deployment.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Running actions (collect, count, write) or side effects inside dataset functions',
        symptom: 'The pipeline fails during graph analysis, or the code runs several times with surprising results',
        why: 'Dataset functions are evaluated to build the graph; they must only return a DataFrame.',
        fix: 'Return a DataFrame and let the engine run it; move side effects to a separate job task.',
        avoid: 'Keep dataset functions pure.',
      },
    ],
    visual: 'pipeline',
    test: { method: 'POST', path: '/api/demos/pipelines/run?new_orders=6' },
  },
  {
    id: 'cdc-scd1',
    category: 'Lakeflow pipelines',
    title: 'Change data capture (SCD type 1)',
    tagline: 'Apply out-of-order change events and keep only the latest state',
    description: [
      'A CDC feed from an operational database is a stream of upserts and deletes with a sequence number. Applying it correctly means: take the latest event per key within a batch, ignore events older than what the table already has, and turn deletes into deletes. This is SCD type 1 (keep the current value only).',
      'Press Run: two batches arrive. Batch 1 contains two events for Ana, and the newest (Braga) wins. Batch 2 deletes Ben, inserts Dee, and carries a late event for Ana with seq 2, which is ignored because the stored row has seq 4. It is all done with a MERGE that has sequence guards.',
      'On Databricks this is one declaration: AUTO CDC INTO … SEQUENCE BY seq (create_auto_cdc_flow in Python). Spark 4.2 ships the API, but "Do it wrong" shows that it cannot run against Delta targets with Delta 4.4 yet.',
    ],
    keyApis: ['MERGE … WHEN MATCHED AND s.seq > t.seq', 'row_number() OVER (PARTITION BY key ORDER BY seq DESC)', 'dp.create_auto_cdc_flow()', 'AUTO CDC INTO … SEQUENCE BY'],
    useCases: ['Replicating OLTP tables', 'Customer and product dimensions', 'Debezium / DMS feeds'],
    snippet: {
      lang: 'sql', name: 'cdc.sql',
      code: `MERGE INTO main.sdp_cdc.customers_scd1 AS t
USING (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY seq DESC) AS rn FROM batch)
       WHERE rn = 1) AS s
ON t.id = s.id
WHEN MATCHED AND s.seq > t.seq AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.seq > t.seq THEN UPDATE SET t.name = s.name, t.city = s.city, t.seq = s.seq
WHEN NOT MATCHED AND s.op != 'DELETE' THEN INSERT (id, name, city, seq) VALUES (s.id, s.name, s.city, s.seq);

-- Databricks: CREATE FLOW f AS AUTO CDC INTO customers FROM STREAM(cdc_feed)
--             KEYS (id) APPLY AS DELETE WHEN op = 'DELETE' SEQUENCE BY seq;`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/pipelines/cdc'],
      ['api', 'spark', 'append batch to cdc_feed'],
      ['spark', 'spark', 'latest event per key'],
      ['spark', 'store', 'MERGE with seq guards'],
      ['api', 'ui', 'table after each batch'],
    ],
    actions: [run('/api/demos/pipelines/cdc', { label: 'Apply two batches' }), wrong('/api/demos/pipelines/cdc', { label: 'Try create_auto_cdc_flow' })],
    pitfalls: [
      {
        mistake: 'Applying CDC events without a sequence guard',
        symptom: 'A late or replayed event overwrites newer data (Ana moves back to "Lisbon (stale)"), and replays after failures corrupt the table',
        why: 'Event arrival order is not change order: retries, parallel extract and network delays reorder events.',
        fix: 'Compare a monotonically increasing sequence (LSN, commit timestamp) and update only when s.seq > t.seq; deduplicate per key first.',
        avoid: 'Always carry a sequence column in CDC feeds; on Databricks use AUTO CDC … SEQUENCE BY.',
      },
      {
        mistake: 'Using create_auto_cdc_flow on open-source Spark 4.2 with Delta 4.4 targets',
        symptom: '[AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE] Cannot start AutoCDC flow: the target table `main`.`sdp_cdc`.`customers` (format: delta) does not support row-level operations',
        why: 'The open-source Auto CDC implementation needs DSv2 row-level operations that Delta 4.4 tables do not expose yet. On Databricks it works.',
        fix: 'Use a MERGE-based flow (this demo) or run the pipeline on Databricks.',
        avoid: 'Check the SDP feature matrix for your Spark + Delta versions before relying on new pipeline APIs.',
        reproduce: 'mistake',
      },
    ],
    visual: 'cdc',
    test: { method: 'POST', path: '/api/demos/pipelines/cdc' },
  },

  // ---- Machine learning --------------------------------------------------------------------
  {
    id: 'mlflow-tracking',
    category: 'Machine learning',
    title: 'MLflow experiment tracking',
    tagline: 'Every training run recorded: params, metrics, code, models',
    description: [
      'Training a model is trial and error. MLflow, built into every Databricks workspace, records each attempt as a run in an experiment: the parameters you chose, the metrics you got, tags and artifacts. You compare runs instead of trusting a spreadsheet or memory.',
      'Press Run: a sample of taxi trips is pulled from Delta into pandas, and four regressors (linear, and random forests of three depths) are trained to predict the fare. Each run logs its params and MAE / r² to the MLflow server. Then mlflow.search_runs ranks them by MAE. Here the linear model wins, because fares in the sample data are linear in distance: the comparison tells you, not intuition.',
      'Runs are global state in the MLflow client. "Do it wrong" forgets to end a run and changes a param inside a run, both real errors.',
    ],
    keyApis: ['mlflow.set_experiment()', 'mlflow.start_run()', 'mlflow.log_params() / log_metrics()', 'mlflow.search_runs()'],
    useCases: ['Hyperparameter tuning', 'Reproducing a past model', 'Team-wide model comparison'],
    snippet: {
      lang: 'python', name: 'ml.py',
      code: `mlflow.set_experiment("/Shared/lab/taxi-fares")
for kind, params in grid:
    with mlflow.start_run(run_name=f"{kind}-{params.get('max_depth', 'ols')}"):
        model = make_model(kind, params).fit(X_train, y_train)
        mlflow.log_params({"model": kind, **params})
        mlflow.log_metrics({"mae": mean_absolute_error(y_test, model.predict(X_test))})

best = mlflow.search_runs(order_by=["metrics.mae ASC"]).iloc[0]`,
    },
    inputs: [{ name: 'rows', label: 'Training rows (500–20,000)', type: 'number', default: 4000, min: 500, max: 20000 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/ml/tracking'],
      ['api', 'spark', 'sample trips → toPandas()'],
      ['api', 'api', 'fit 4 models (scikit-learn)'],
      ['api', 'mlflow', 'log params + metrics per run'],
      ['api', 'mlflow', 'search_runs(order_by mae)'],
      ['api', 'ui', 'leaderboard'],
    ],
    actions: [
      run('/api/demos/ml/tracking', { label: 'Train & track 4 models', params: (i) => ({ rows: i.rows }) }),
      wrong('/api/demos/ml/tracking', { label: 'Leak a run', params: (i) => ({ rows: i.rows }) }),
    ],
    pitfalls: [
      {
        mistake: 'Calling mlflow.start_run() without a with-block and never ending it',
        symptom: 'Exception: Run with UUID 6e34d44b… is already active. To start a new run, first end the current run with mlflow.end_run(). To start a nested run, call start_run with nested=True',
        why: 'The fluent API keeps one active run per thread; an exception or early return leaves it open.',
        fix: 'Call mlflow.end_run(), then use with mlflow.start_run(): so runs always close (and are marked FAILED on exceptions).',
        avoid: 'Always use the context manager; use nested=True for child runs in tuning loops.',
        lang: 'python',
        wrongCode: 'mlflow.start_run()\nmodel.fit(X, y)\nmlflow.log_metric("mae", mae)   # an exception here leaves the run open',
        rightCode: 'with mlflow.start_run():\n    model.fit(X, y)\n    mlflow.log_metric("mae", mae)',
        reproduce: 'mistake',
      },
      {
        mistake: 'Logging the same param twice with different values in one run',
        symptom: "MlflowException: INVALID_PARAMETER_VALUE: Changing param values is not allowed. Param with key='max_depth' was already logged with value='3' for run ID='6e34d44b…'. Attempted logging new value '6'.",
        why: 'Params describe one run\'s configuration and are immutable; values that change over time are metrics.',
        fix: 'Start a new run per configuration, or log the changing value as a metric with a step.',
        avoid: 'One configuration = one run; loop over configurations outside the run.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Calling toPandas() on the full table to train on the driver',
        symptom: 'Training "works" on samples, then the driver runs out of memory at production size',
        why: 'scikit-learn trains on one machine; toPandas() moves every row there.',
        fix: 'Sample or aggregate in Spark first, or use distributed training (Spark ML, TorchDistributor, Ray on Databricks).',
        avoid: 'Log the training data version (Delta version) with the run so samples are reproducible.',
      },
    ],
    visual: 'tracking',
    test: { method: 'POST', path: '/api/demos/ml/tracking?rows=2000' },
  },
  {
    id: 'model-registry',
    category: 'Machine learning',
    title: 'Models in Unity Catalog',
    tagline: 'Register versions, enforce signatures, score in batch',
    description: [
      'A trained model becomes a governed asset when you register it: on Databricks the MLflow model registry lives in Unity Catalog, so models have three-part names (catalog.schema.model), permissions and lineage like tables. Each registration creates a numbered version; the signature records the input and output schema the model expects.',
      'Press Run: two forests are logged with signatures and registered as versions 1 and 2 of main.ml.fare_model in the open-source Unity Catalog server. UC reports both READY, with files in governed storage. The better version is loaded by its models:/ URI, scores five trips, and the predictions are written to a Delta table.',
      'On Databricks you then point an alias (@champion) at the version to serve. "Do it wrong" shows what differs here: aliases are not implemented by the OSS UC store, two-level names are rejected, and MLflow 3 refuses to serialize types it has not been told to trust.',
    ],
    keyApis: ['mlflow.set_registry_uri("databricks-uc")', 'log_model(..., registered_model_name="c.s.m")', 'infer_signature()', 'models:/c.s.m/<version>'],
    useCases: ['Promoting models to production', 'Batch scoring jobs', 'Model governance and lineage'],
    snippet: {
      lang: 'python', name: 'ml.py',
      code: `mlflow.set_registry_uri("uc:http://unitycatalog:8080")     # on Databricks: "databricks-uc" (default)
with mlflow.start_run():
    model = RandomForestRegressor(max_depth=8).fit(X_train, y_train)
    mlflow.sklearn.log_model(model, name="model",
                             signature=infer_signature(X_train, model.predict(X_train)),
                             registered_model_name="main.ml.fare_model",
                             skops_trusted_types=["sklearn.tree._tree.Tree"])

model = mlflow.pyfunc.load_model("models:/main.ml.fare_model/2")   # Databricks: ".../fare_model@champion"
predictions = model.predict(X_new)`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/ml/registry'],
      ['api', 'mlflow', 'log 2 models (runs)'],
      ['api', 'uc', 'register versions 1, 2'],
      ['api', 'store', 'model files → UC storage'],
      ['api', 'uc', 'load models:/…/2'],
      ['api', 'spark', 'write predictions table'],
      ['api', 'ui', 'versions + predictions'],
    ],
    actions: [run('/api/demos/ml/registry', { label: 'Register & score' }), wrong('/api/demos/ml/registry', { label: 'Aliases, 2-level names, untrusted types' })],
    pitfalls: [
      {
        mistake: 'Logging a tree-based scikit-learn model with MLflow 3 without trusting its types',
        symptom: "MlflowException: The saved sklearn model references untrusted types. If you are sure loading these types is safe, set the 'skops_trusted_types' parameter when calling 'log_model' or 'save_model' to the list of trusted types. Root error: Untrusted types found in the file: ['sklearn.tree._tree.Tree'].",
        why: 'MLflow 3.17 saves scikit-learn models with skops instead of pickle and only loads types on an allow-list, because tree internals can be abused by malicious files.',
        fix: 'Pass skops_trusted_types=["sklearn.tree._tree.Tree"] (only for models you produced), or choose serialization_format explicitly.',
        avoid: 'Pin the MLflow version in training and serving environments and test log_model → load_model in CI.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Registering a model with a two-level name (or none) when the registry is Unity Catalog',
        symptom: 'ValueError: not enough values to unpack (expected 3, got 1)  (OSS store). Databricks reports that the name must be of the form catalog.schema.model.',
        why: 'UC registries need catalog.schema.model; the old workspace registry used flat names.',
        fix: 'Use registered_model_name="main.ml.fare_model" and make sure you can CREATE MODEL in that schema.',
        avoid: 'Keep the catalog per environment in config and build the full name from it.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Using aliases (@champion) or stages with the open-source Unity Catalog registry',
        symptom: 'NotImplementedError: Method not implemented',
        why: 'MLflow\'s UC OSS store (UC 0.7) does not implement aliases or tags; Databricks UC does. Stages ("Production") are deprecated with UC registries everywhere.',
        fix: 'Load by explicit version (models:/name/2) on OSS UC; use aliases on Databricks.',
        avoid: 'Write deployment code against aliases on Databricks, and keep a version-pinned fallback for local tests.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Logging models without a signature',
        symptom: 'Databricks UC rejects the registration because the model has no signature (the OSS server in this lab accepts it); serving later accepts any input shape',
        why: 'Unity Catalog requires signatures so inputs can be validated.',
        fix: 'Pass signature=infer_signature(X, model.predict(X)) or input_example=X[:5].',
        avoid: 'Make signatures part of your training template.',
      },
    ],
    visual: 'registry',
    test: { method: 'POST', path: '/api/demos/ml/registry' },
  },

  // ---- Jobs & platform ---------------------------------------------------------------------
  {
    id: 'sdk-jobs',
    category: 'Jobs & platform',
    title: 'Jobs with the Databricks SDK',
    tagline: 'A task DAG as code: SDK classes, Jobs API JSON and an Asset Bundle',
    description: [
      'Lakeflow Jobs run production work: a job is a DAG of tasks (notebooks, pipelines, Python files, SQL, dbt…) with dependencies, retries, schedules, parameters and notifications, on job clusters or serverless. You define them in code with the Databricks SDK, the REST API, Terraform, or Asset Bundles (databricks.yml).',
      'Press Run: the backend builds a four-task job with the official databricks-sdk classes: ingest (notebook, 2 retries) → refresh_pipeline → train_model → notify_failure (runs only if something failed). It shows the exact JSON body the SDK would POST to /api/2.2/jobs/create, the same job as a bundle file, and the run order computed from depends_on. Nothing is sent unless a workspace token is configured; then it also makes read-only calls (current user, list jobs), rate-limited.',
      '"Do it wrong" reproduces the two most common SDK problems: no credentials, and the constructor blocking on an unreachable host.',
    ],
    keyApis: ['WorkspaceClient()', 'jobs.JobSettings / jobs.Task', 'depends_on + run_if', 'databricks bundle deploy'],
    useCases: ['Scheduling pipelines and training', 'CI/CD of jobs', 'Automating workspaces'],
    snippet: {
      lang: 'python', name: 'platform.py',
      code: `from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

settings = jobs.JobSettings(
    name="taxi-daily",
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="UTC"),
    tasks=[
        jobs.Task(task_key="ingest", notebook_task=jobs.NotebookTask(notebook_path="/Workspace/lab/ingest_trips"), max_retries=2),
        jobs.Task(task_key="refresh_pipeline", depends_on=[jobs.TaskDependency(task_key="ingest")],
                  pipeline_task=jobs.PipelineTask(pipeline_id="...")),
        jobs.Task(task_key="notify_failure", depends_on=[jobs.TaskDependency(task_key="train_model")],
                  run_if=jobs.RunIf.AT_LEAST_ONE_FAILED, notebook_task=jobs.NotebookTask(notebook_path="/Workspace/lab/alert")),
    ])
w = WorkspaceClient()                                   # host + auth from env / ~/.databrickscfg
job = w.jobs.create(**settings.as_shallow_dict())`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/platform/jobs'],
      ['api', 'api', 'build JobSettings (SDK)'],
      ['api', 'api', 'as_dict() → Jobs API JSON'],
      ['api', 'api', 'DAG order from depends_on'],
      ['api', 'ui', 'DAG, JSON, bundle'],
    ],
    actions: [run('/api/demos/platform/jobs', { label: 'Build the job' }), wrong('/api/demos/platform/jobs', { label: 'No credentials, bad host' })],
    pitfalls: [
      {
        mistake: 'Creating a WorkspaceClient where no credentials are configured (CI runners, containers, a new laptop)',
        symptom: 'ValueError: default auth: cannot configure default credentials, please check https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication to configure credentials for your preferred authentication method.',
        why: 'Unified auth tried env vars, ~/.databrickscfg profiles, OAuth caches and cloud identities and found none.',
        fix: 'databricks auth login --host … locally; in CI set DATABRICKS_HOST plus DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET of a service principal.',
        avoid: 'Use OAuth service principals for automation; never commit tokens. The error lists which config it saw.',
        reproduce: 'mistake',
      },
      {
        mistake: 'Pointing the SDK at a mistyped or unreachable host',
        symptom: 'WorkspaceClient() hangs for about 5 minutes, then logs: Failed to automatically resolve config from host metadata: Failed to fetch host metadata from https://…/.well-known/databricks-config: Timed out',
        why: 'databricks-sdk 0.155 fetches host metadata while constructing the config, with the default 300 s retry budget.',
        fix: 'Correct the host; pass Config(host=..., retry_timeout_seconds=10) to fail fast.',
        avoid: 'Validate configuration at startup with a short timeout (the lab uses 3 s).',
        lang: 'python',
        wrongCode: 'w = WorkspaceClient(host="https://dbc-typo.cloud.databricks.com")   # blocks ~300 s',
        rightCode: 'from databricks.sdk.core import Config\nw = WorkspaceClient(config=Config(host=host, retry_timeout_seconds=10))',
        reproduce: 'mistake',
      },
      {
        mistake: 'Building jobs by hand in the UI and copying them between workspaces',
        symptom: 'Dev and prod jobs drift apart; nobody knows which settings production runs with',
        why: 'UI-defined jobs are not versioned or reviewed.',
        fix: 'Export with databricks bundle generate job --existing-job-id … and manage it as code.',
        avoid: 'Asset Bundles in git with per-target overrides, deployed by CI.',
      },
    ],
    visual: 'jobs',
    test: { method: 'POST', path: '/api/demos/platform/jobs' },
  },

  // ---- Common mistakes ---------------------------------------------------------------------
  {
    id: 'data-skew',
    category: 'Common mistakes',
    title: 'Data skew in joins',
    tagline: 'One hot key makes one task do all the work',
    description: [
      'A shuffle sends all rows with the same key to the same task. If one key (a giant customer, a NULL, a default value like 0) holds most rows, one task gets most of the data while the others finish in seconds and wait. The stage is as slow as its slowest task, and that task may spill to disk or run out of memory.',
      'Press Run: 600,000 orders are joined with customers, and most orders belong to customer 0. Each lane is one task of the join stage, sized by the records it read (from the Spark UI API). With skew handling off, one lane dwarfs the rest. With AQE\'s skew join on, Spark detects the oversized partition at runtime and splits it across several tasks. The answer is identical.',
      'The lab lowers AQE\'s thresholds (256 MB by default) so the effect shows on lab-sized data.',
    ],
    keyApis: ['spark.sql.adaptive.skewJoin.enabled', 'skewedPartitionFactor / ThresholdInBytes', 'broadcast()', 'salting'],
    useCases: ['Joins on customer, tenant or country', 'NULL-heavy join keys', 'Slow "last task" stages'],
    snippet: {
      lang: 'python', name: 'mistakes.py',
      code: `orders = spark.range(0, 600_000, 1, 16).select(
    when(col("id") % 100 < 80, lit(0)).otherwise(col("id") % 997 + 1).alias("customer_id"), ...)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")      # force a shuffle join
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")    # one huge task
orders.join(customers, "customer_id").groupBy("name").agg(sum("amount")).collect()
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")     # split the hot partition`,
    },
    inputs: [{ name: 'hot_share', label: '% of orders for customer 0 (10–95)', type: 'number', default: 80, min: 10, max: 95 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/mistakes/skew'],
      ['api', 'spark', 'join, skewJoin off'],
      ['spark', 'spark', '1 task reads the hot key'],
      ['api', 'spark', 'join, skewJoin on'],
      ['spark', 'spark', 'split hot partition'],
      ['spark', 'api', 'per-task records (Spark UI)'],
      ['api', 'ui', 'task lanes'],
    ],
    actions: [run('/api/demos/mistakes/skew', { label: 'Join with a hot key', params: (i) => ({ hot_share: i.hot_share }) })],
    pitfalls: [
      {
        mistake: 'Joining or aggregating on a key where one value dominates (often NULL or a default)',
        symptom: 'A stage sits at 199/200 tasks for most of its runtime; the Spark UI shows one task with far more shuffle read and spill than the median',
        why: 'Hash partitioning sends every row of a key to one task.',
        fix: 'Keep AQE skew join on (default on Databricks), filter NULL keys before joining, broadcast the small side, or salt the hot key (add a random suffix and replicate the other side).',
        avoid: 'Profile key distributions (groupBy(key).count().orderBy(desc)) before designing joins.',
        lang: 'python',
        wrongCode: 'orders.join(customers, "customer_id")      # 80% of rows on one key',
        rightCode: 'spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")\norders.join(broadcast(customers), "customer_id")   # or salt the hot key',
      },
    ],
    visual: 'skew',
    test: { method: 'POST', path: '/api/demos/mistakes/skew?hot_share=80' },
  },
  {
    id: 'row-by-row',
    category: 'Common mistakes',
    title: 'Row-by-row writes',
    tagline: 'A loop of INSERTs costs a commit and a file per row',
    description: [
      'Code ported from an application database often writes one row at a time. On Delta every INSERT is a full transaction: plan the query, write a Parquet file, write a commit to the log. That is tens to hundreds of milliseconds per row, and a new tiny file each time.',
      'Press Run: the same rows are written once in a loop of INSERT statements and once as one DataFrame write. Compare the time, the commits and the files. OPTIMIZE can compact the small files afterwards, but it cannot give back the time.',
      'Collect rows into a DataFrame (or a staging table) and write in batches; for event streams, use Structured Streaming with a sensible trigger.',
    ],
    keyApis: ['spark.createDataFrame(rows)', 'df.write.mode("append")', 'OPTIMIZE', 'delta.autoOptimize.optimizeWrite'],
    useCases: ['Porting application code', 'Writing from loops or APIs', 'Event ingestion'],
    snippet: {
      lang: 'python', name: 'mistakes.py',
      code: `for i in range(20):                                             # don't
    spark.sql(f"INSERT INTO events_loop VALUES ({i}, 'click')")

spark.createDataFrame([(i, "click") for i in range(20)], "id INT, kind STRING") \\
     .write.mode("append").saveAsTable("events_batch")            # do: one commit`,
    },
    inputs: [{ name: 'rows', label: 'Rows (5–40)', type: 'number', default: 20, min: 5, max: 40 }],
    flow: [
      ['ui', 'api', 'POST /api/demos/mistakes/row-by-row'],
      ['api', 'spark', 'N × INSERT (one commit each)'],
      ['spark', 'store', 'N tiny files + N commits'],
      ['api', 'spark', '1 batch write'],
      ['spark', 'store', '1 file + 1 commit'],
      ['api', 'ui', 'time, commits, files'],
    ],
    actions: [run('/api/demos/mistakes/row-by-row', { label: 'Loop vs batch', params: (i) => ({ rows: i.rows }) })],
    pitfalls: [
      {
        mistake: 'Inserting rows one at a time from a loop, an API handler or a UDF',
        symptom: 'Writing a few thousand rows takes minutes; the table has thousands of tiny files and a long _delta_log',
        why: 'Each statement is a separate Delta transaction with its own planning, file and commit.',
        fix: 'Batch: build a DataFrame (or stage rows in a queue / file) and append once; then OPTIMIZE the damaged table.',
        avoid: 'Use OLTP databases for per-request writes and land them in Delta in batches or via CDC.',
        lang: 'python',
        wrongCode: 'for row in rows:\n    spark.sql(f"INSERT INTO t VALUES ({row.id}, \'{row.kind}\')")',
        rightCode: 'spark.createDataFrame(rows).write.mode("append").saveAsTable("t")',
      },
      {
        mistake: 'Running many concurrent writers that update the same Delta table',
        symptom: 'Writers fail with ConcurrentAppendException / ConcurrentDeleteReadException ([DELTA_CONCURRENT_APPEND] Files were added to the root of the table by a concurrent update.)',
        why: 'Optimistic concurrency: two transactions that touch the same files conflict, and one must retry.',
        fix: 'Retry with backoff, partition or cluster the table so writers touch different files, or funnel writes through one job.',
        avoid: 'Enable row-level concurrency (deletion vectors + liquid clustering on Databricks) and design for few, larger writers.',
      },
    ],
    visual: 'rowByRow',
    test: { method: 'POST', path: '/api/demos/mistakes/row-by-row?rows=12' },
  },
  {
    id: 'collect-to-driver',
    category: 'Common mistakes',
    title: 'Pulling data to the driver',
    tagline: 'toPandas() on a big table vs aggregating on the cluster',
    description: [
      'collect() and toPandas() copy every row into the memory of one process: your notebook\'s driver or the Databricks Connect client. It is tempting because pandas is familiar, but it turns a cluster into a single machine and is the most common cause of driver out-of-memory errors.',
      'Press Run: average fare per pickup zip, computed twice. Once by pulling all 100,000 trips into pandas and grouping there, once by grouping in Spark and pulling the 40-row answer. Same result; compare the memory and time bars.',
      'Push work down to the cluster (filter, aggregate, join), and bring back only what you will display or plot.',
    ],
    keyApis: ['df.toPandas()', 'df.collect()', 'spark.driver.maxResultSize', 'pyspark.pandas'],
    useCases: ['Plotting and reporting', 'Feature engineering', 'Exporting results'],
    snippet: {
      lang: 'python', name: 'mistakes.py',
      code: `pdf = spark.read.table("samples.nyctaxi.trips").toPandas()          # every row to the driver
pdf.groupby("pickup_zip")["fare_amount"].mean()

(spark.read.table("samples.nyctaxi.trips")                            # aggregate on the cluster
      .groupBy("pickup_zip").agg(avg("fare_amount").alias("avg_fare"))
      .toPandas())                                                    # 40 rows`,
    },
    flow: [
      ['ui', 'api', 'POST /api/demos/mistakes/collect'],
      ['api', 'spark', 'toPandas() everything'],
      ['spark', 'api', '100,000 rows over the wire'],
      ['api', 'spark', 'groupBy on the cluster'],
      ['spark', 'api', '40 rows'],
      ['api', 'ui', 'memory + time'],
    ],
    actions: [run('/api/demos/mistakes/collect', { label: 'Compare both ways' })],
    pitfalls: [
      {
        mistake: 'Calling collect() or toPandas() on a large DataFrame',
        symptom: 'org.apache.spark.SparkException: Job aborted due to stage failure: Total size of serialized results of N tasks (… MiB) is bigger than spark.driver.maxResultSize (… MiB), or the driver restarts with "The spark driver has stopped unexpectedly and is restarting" (out of memory)',
        why: 'All rows are materialized in one JVM and then in Python on the driver.',
        fix: 'Aggregate or limit before collecting; write large results to a table or files; use pandas UDFs or pyspark.pandas for pandas-style code that stays distributed.',
        avoid: 'Treat collect()/toPandas() as "display only" calls and add .limit() during exploration.',
      },
    ],
    visual: 'collect',
    test: { method: 'POST', path: '/api/demos/mistakes/collect' },
  },
];
