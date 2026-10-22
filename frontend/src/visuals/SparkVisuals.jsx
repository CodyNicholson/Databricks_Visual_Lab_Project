import React from 'react';
import { Bar, Empty, Stat, formatBytes, formatMs, formatNumber, useStepper } from '../components/common.jsx';
import { RealErrors, RowsTable, Pre } from './shared.jsx';

export function ConnectVisual({ result }) {
  if (!result) return <Empty>Press “Run on the cluster” to send a query plan to the Spark Connect server.</Empty>;
  const d = result.data;
  const max = Math.max(d.rawBytes, d.planBytes || 0);
  return (
    <div>
      <div className="connect-wire">
        <div className="wire-end">
          <b>Your code</b>
          <span className="muted">pyspark {d.clientVersion} (thin client)</span>
        </div>
        <div className="wire">
          <span className="wire-packet wire-plan">plan · {formatBytes(d.planBytes)}</span>
          <span className="wire-packet wire-result">result · {d.resultBytes} B</span>
        </div>
        <div className="wire-end wire-server">
          <b>Spark {d.serverVersion}</b>
          <span className="muted">{d.remote} · {d.conf['spark.master']}</span>
        </div>
      </div>
      <h4>What crossed the network vs what the cluster processed</h4>
      <Bar label="data the cluster scanned" value={d.rawBytes} max={max} tone="store" text={formatBytes(d.rawBytes)} />
      {d.planBytes && <Bar label="plan sent by the client" value={d.planBytes} max={max} tone="accent" text={formatBytes(d.planBytes)} />}
      <Bar label="answer sent back" value={d.resultBytes} max={max} tone="ok" text={`${d.resultBytes} B`} />
      <div className="stat-row">
        <Stat label={`sum of ${formatNumber(d.rows)} ids`} value={formatNumber(d.total)} tone="accent" />
        <Stat label="server round trip" value={formatMs(d.serverMs)} />
        <Stat label="Spark jobs" value={d.jobs.length} hint={d.jobs.map((j) => `job ${j.jobId}: ${j.tasks} tasks`).join(', ')} />
        <Stat label="session" value={d.sessionId.slice(0, 8)} hint="one gRPC session per client" />
      </div>
      <RealErrors result={result} title="Classic APIs that do not exist over Spark Connect" />
    </div>
  );
}

export function LazyVisual({ result }) {
  const steps = result?.data.steps || [];
  const shown = useStepper(steps.length, result?.receivedAt, 380);
  if (!result) return <Empty>Build the query step by step: watch the job counter stay at zero until the action.</Empty>;
  const d = result.data;
  const max = Math.max(...steps.map((s) => s.ms), 1);
  return (
    <div>
      <ol className="lazy-steps">
        {steps.slice(0, shown).map((s, i) => (
          <li key={i} className={`lazy-step pop-in lazy-${s.kind}`}>
            <code>{s.code}</code>
            <span className="lazy-kind">{s.kind}</span>
            <span className="lazy-ms">{formatMs(s.ms)}</span>
            <span className={`lazy-jobs ${s.jobs ? 'lazy-jobs-hot' : ''}`}>{s.jobs} job{s.jobs === 1 ? '' : 's'}</span>
            <div className="lazy-bar" style={{ width: `${Math.max(2, (s.ms / max) * 100)}%` }} />
          </li>
        ))}
      </ol>
      {shown >= steps.length && (
        <>
          <div className="two-col">
            <div className="optim-box slide-in">
              <h4>Column pruning</h4>
              <code>{d.readSchema.replace(/^\(\d+\)\s*/, '')}</code>
            </div>
            <div className="optim-box slide-in">
              <h4>Predicate pushdown</h4>
              <code>{d.pushedFilters}</code>
            </div>
          </div>
          <RowsTable rows={d.rows} caption={`Top pickup zips for trips over ${d.minDistance} miles`} />
          <Pre title="The optimized physical plan (top.explain('formatted'))" text={d.plan.split('\n\n')[0]} />
        </>
      )}
      {d.cacheRuns && <CacheRuns runs={d.cacheRuns} />}
    </div>
  );
}

function CacheRuns({ runs }) {
  const max = Math.max(...runs.map((r) => r.inputRecords), 1);
  return (
    <div className="cache-runs">
      <h4>Reusing a DataFrame for two actions: rows read from the table each time</h4>
      {runs.map((r, i) => (
        <Bar key={i} label={`${r.label} · ${r.action}`} value={r.inputRecords} max={max} tone={r.label === 'cached' ? 'ok' : 'bad'}
          text={`${formatNumber(r.inputRecords)} rows · ${formatMs(r.ms)}`} />
      ))}
    </div>
  );
}

export function SqlVisual({ result }) {
  if (!result) return <Empty>Run the same question as SQL and as a DataFrame: they compile to the same plan.</Empty>;
  const d = result.data;
  const same = JSON.stringify(d.sql) === JSON.stringify(d.dataframe);
  return (
    <div>
      <div className="two-col">
        <div className="lang-card slide-in">
          <span className="chip">spark.sql(...)</span>
          <RowsTable rows={d.sql} />
        </div>
        <div className="lang-card slide-in" style={{ animationDelay: '120ms' }}>
          <span className="chip">DataFrame API</span>
          <RowsTable rows={d.dataframe} />
        </div>
      </div>
      <div className="stat-row">
        <Stat label="same answer" value={same ? 'yes ✓' : 'no ✗'} tone={same ? 'ok' : 'bad'} />
        <Stat label="temp view rows" value={formatNumber(d.viewRows)} hint="trips_v, queried from SQL" />
        <Stat label="join strategy" value={d.sqlPlan.includes('BroadcastHashJoin') ? 'broadcast' : 'shuffle'} hint="identical in both plans" />
      </div>
      {d.injection && (
        <div className="injection">
          <h4>User input: <code>{d.injection.input}</code></h4>
          <Bar label="f-string into the SQL text" value={d.injection.unsafeRows} max={40} tone="bad" text={`${d.injection.unsafeRows} of 40 zones match`} />
          <Bar label="passed as a value (b=...)" value={d.injection.safeRows} max={40} tone="ok" text={`${d.injection.safeRows} rows`} />
          <Bar label="DataFrame filter" value={d.injection.filterRows} max={40} tone="ok" text={`${d.injection.filterRows} rows`} />
        </div>
      )}
      <RealErrors result={result} />
    </div>
  );
}

export function ShuffleVisual({ result }) {
  if (!result) return <Empty>Run the same aggregation with adaptive query execution off and on, and watch the reduce tasks.</Empty>;
  const d = result.data;
  const [off, on] = d.runs;
  return (
    <div>
      <div className="two-col">
        {[off, on].map((run) => (
          <div key={String(run.aqe)} className="task-grid-card slide-in">
            <h4>AQE {run.aqe ? 'on' : 'off'} · {run.reduceTasks} reduce tasks · {formatMs(run.ms)}</h4>
            <div className="task-grid">
              {Array.from({ length: Math.min(run.reduceTasks, 400) }).map((_, i) => (
                <span key={i} className={`task-cell ${run.aqe ? 'task-on' : 'task-off'}`} style={{ animationDelay: `${Math.min(i, 200) * 4}ms` }} />
              ))}
            </div>
            <span className="muted">shuffle written: {formatBytes(run.shuffleBytes)} · {run.groups} groups</span>
          </div>
        ))}
      </div>
      <h4>Join strategy chosen by the optimizer</h4>
      {d.joins.map((j) => (
        <div key={j.threshold} className={`join-row slide-in ${j.strategy === 'BroadcastHashJoin' ? 'join-good' : 'join-slow'}`}>
          <code>autoBroadcastJoinThreshold = {j.threshold}</code>
          <b>{j.strategy}</b>
          <span>{formatMs(j.ms)}</span>
        </div>
      ))}
    </div>
  );
}

export function UdfVisual({ result }) {
  if (!result) return <Empty>Race four ways to compute the same column: built-in functions, pandas UDFs and Python UDFs.</Empty>;
  const d = result.data;
  const max = Math.max(...d.results.map((r) => r.ms));
  const tones = ['ok', 'accent', 'warn', 'bad'];
  return (
    <div>
      <h4>{formatNumber(d.rows)} rows, same fare buckets, time on the cluster</h4>
      {d.results.map((r, i) => <Bar key={r.label} label={r.label} value={r.ms} max={max} tone={tones[i]} text={formatMs(r.ms)} />)}
      <div className="stat-row">
        {Object.entries(d.results[0].counts).sort().map(([bucket, n]) => <Stat key={bucket} label={`bucket “${bucket}”`} value={formatNumber(n)} />)}
        <Stat label="built-in vs pickled UDF" value={`${Math.max(1, Math.round(d.results[3].ms / d.results[0].ms))}× faster`} tone="ok" />
      </div>
    </div>
  );
}
