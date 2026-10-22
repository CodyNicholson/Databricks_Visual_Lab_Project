import React from 'react';
import { Bar, Empty, Stat, formatBytes, formatMs, formatNumber } from '../components/common.jsx';

function TaskLanes({ run, max }) {
  return (
    <div className="lanes">
      {run.records.map((n, i) => (
        <div key={i} className="lane">
          <span className="lane-label">task {i + 1}</span>
          <div className="lane-track"><div className={`lane-fill ${n > 4 * run.median ? 'lane-hot' : ''}`} style={{ width: `${Math.max(0.6, (n / max) * 100)}%`, animationDelay: `${i * 25}ms` }} /></div>
          <span className="lane-n">{formatNumber(n)}</span>
        </div>
      ))}
    </div>
  );
}

export function SkewVisual({ result }) {
  if (!result) return <Empty>Join 600,000 orders where one customer owns most of them, with and without AQE's skew-join handling.</Empty>;
  const d = result.data;
  const [off, on] = d.runs;
  const max = Math.max(off.max, on.max);
  return (
    <div>
      <p className="hint">Each lane is one task of the join stage; its length is the shuffle records it had to read. {d.hotShare}% of the orders belong to customer 0, so they all hash to the same partition.</p>
      <div className="two-col">
        <div><h4 className="bad">skewJoin off · {off.tasks} tasks · {formatMs(off.ms)}</h4><TaskLanes run={off} max={max} /></div>
        <div><h4 className="ok">skewJoin on · {on.tasks} tasks · {formatMs(on.ms)}</h4><TaskLanes run={on} max={max} /></div>
      </div>
      <div className="stat-row">
        <Stat label="busiest task before" value={formatNumber(off.max)} tone="bad" hint={`median ${formatNumber(off.median)}`} />
        <Stat label="busiest task after" value={formatNumber(on.max)} tone="ok" />
        <Stat label="same totals" value={off.total === on.total ? 'yes ✓' : 'no ✗'} tone={off.total === on.total ? 'ok' : 'bad'} />
      </div>
    </div>
  );
}

export function RowByRowVisual({ result }) {
  if (!result) return <Empty>Write the same rows one INSERT at a time and as one batch; compare time, commits and files.</Empty>;
  const d = result.data;
  const loop = d.results['one INSERT per row'];
  const batch = d.results['one batch'];
  const max = Math.max(loop.ms, batch.ms);
  return (
    <div>
      <Bar label={`${d.rows} INSERT statements`} value={loop.ms} max={max} tone="bad" text={formatMs(loop.ms)} />
      <Bar label="1 batch write" value={batch.ms} max={max} tone="ok" text={formatMs(batch.ms)} />
      <div className="two-col">
        <div>
          <h4 className="bad">Loop: {loop.commits} commits, {loop.files} files</h4>
          <div className="file-cells">{Array.from({ length: loop.files }).map((_, i) => <span key={i} className="file-cell file-old pop-in" style={{ animationDelay: `${i * 35}ms` }} />)}</div>
          <p className="muted">each INSERT {formatMs(Math.min(...loop.timings))}–{formatMs(Math.max(...loop.timings))}; OPTIMIZE later compacts {loop.files} → {loop.filesAfterOptimize}</p>
        </div>
        <div>
          <h4 className="ok">Batch: {batch.commits} commit, {batch.files} file</h4>
          <div className="file-cells">{Array.from({ length: batch.files }).map((_, i) => <span key={i} className="file-cell file-live pop-in" />)}</div>
        </div>
      </div>
      <div className="stat-row"><Stat label="slower row by row" value={`${Math.round(loop.ms / batch.ms)}×`} tone="bad" /></div>
    </div>
  );
}

export function CollectVisual({ result }) {
  if (!result) return <Empty>Average fares per zip two ways: pull the table into pandas, or aggregate on the cluster and pull the answer.</Empty>;
  const d = result.data;
  const max = Math.max(d.wrong.bytes, d.right.bytes);
  return (
    <div>
      <h4>Driver memory used by the pandas DataFrame</h4>
      <Bar label={`toPandas() everything · ${formatNumber(d.wrong.rows)} rows`} value={d.wrong.bytes} max={max} tone="bad" text={formatBytes(d.wrong.bytes)} />
      <Bar label={`groupBy on the cluster · ${formatNumber(d.right.rows)} rows`} value={d.right.bytes} max={max} tone="ok" text={formatBytes(d.right.bytes)} />
      <div className="stat-row">
        <Stat label="pull everything" value={formatMs(d.wrong.ms)} tone="bad" />
        <Stat label="aggregate first" value={formatMs(d.right.ms)} tone="ok" />
        <Stat label="less data moved" value={`${Math.round(d.wrong.bytes / Math.max(1, d.right.bytes))}×`} tone="accent" />
      </div>
      <div className="zip-chips">{d.sample.map((s) => <span key={s.zip} className="chip chip-soft">{s.zip}: ${s.avg.toFixed(2)}</span>)}</div>
      <p className="hint">100,000 rows fit, so it works here. At 100 million rows the same line fails the driver (out of memory or spark.driver.maxResultSize) while the aggregate still returns 40 rows.</p>
    </div>
  );
}
