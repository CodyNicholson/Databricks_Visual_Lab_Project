import React from 'react';
import { Empty, Stat, formatMs, useStepper } from '../components/common.jsx';
import { RealErrors, RowsTable } from './shared.jsx';

const NODES = {
  'landing JSON': { x: 70, y: 70, kind: 'source' },
  orders_raw: { x: 230, y: 70, kind: 'streaming table' },
  orders_clean: { x: 390, y: 70, kind: 'streaming table' },
  revenue_by_store: { x: 560, y: 30, kind: 'materialized view' },
  top_customers: { x: 560, y: 115, kind: 'materialized view' },
};

/** The pipeline's dataflow graph; each flow lights up in the order the event log reported. */
export function PipelineVisual({ result }) {
  const events = result?.data.run.events || [];
  const shown = useStepper(events.length, result?.receivedAt, 260);
  if (!result) return <Empty>Drop a file of orders in the landing folder and run the pipeline: only the new file is processed.</Empty>;
  const d = result.data;
  const state = {};
  events.slice(0, shown).forEach((e) => { state[e.flow] = e.state; });
  return (
    <div>
      <svg viewBox="0 0 660 160" className="dag" role="img" aria-label="pipeline dataflow graph">
        {d.graph.map(([from, to]) => {
          const a = NODES[from], b = NODES[to];
          return <path key={from + to} className={`dag-edge ${state[to] ? 'dag-edge-on' : ''}`} d={`M ${a.x + 62} ${a.y} C ${a.x + 100} ${a.y}, ${b.x - 100} ${b.y}, ${b.x - 62} ${b.y}`} />;
        })}
        {Object.entries(NODES).map(([name, n]) => (
          <g key={name} transform={`translate(${n.x - 62} ${n.y - 20})`} className={`dag-node dag-${(state[name] || (n.kind === 'source' ? 'SOURCE' : 'IDLE')).toLowerCase()}`}>
            <rect width="124" height="40" rx="9" />
            <text x="62" y="17" textAnchor="middle" className="dag-name">{name}</text>
            <text x="62" y="31" textAnchor="middle" className="dag-kind">{state[name]?.toLowerCase() || n.kind}</text>
          </g>
        ))}
      </svg>
      <div className="stat-row">
        <Stat label="new orders this run" value={d.processedThisRun} tone="accent" hint={`${d.files.length} files in landing`} />
        {Object.entries(d.counts).map(([t, n]) => <Stat key={t} label={t} value={n} />)}
        <Stat label="pipeline run" value={formatMs(d.run.ms)} hint={`exit code ${d.run.returncode}`} />
      </div>
      {d.revenue.length > 0 && <RowsTable rows={d.revenue} caption="revenue_by_store (materialized view)" />}
      <RealErrors result={result} title="Real errors from spark-pipelines dry-run" />
    </div>
  );
}

export function CdcVisual({ result }) {
  const states = result?.data.states || [];
  const shown = useStepper(states.length, result?.receivedAt, 900);
  if (!result) return <Empty>Apply two batches of change events (with a duplicate key and a late event) as SCD type 1.</Empty>;
  return (
    <div>
      {states.slice(0, shown).map((st) => (
        <div key={st.batch} className="cdc-batch pop-in">
          <h4>Batch {st.batch}: {st.events.length} events → updated {st.metrics.num_updated_rows}, deleted {st.metrics.num_deleted_rows}, inserted {st.metrics.num_inserted_rows}</h4>
          <div className="two-col">
            <RowsTable rows={st.events} caption="change events" rowClass={(e) => (e.op === 'DELETE' ? 'row-gone' : st.batch === 2 && e.seq === 2 ? 'row-fading' : '')} />
            <RowsTable rows={st.table} caption="customers_scd1 after the batch" />
          </div>
        </div>
      ))}
      {shown >= states.length && <p className="hint">The orange event (seq 2) arrived late: its sequence number is older than the stored row's, so the MERGE ignored it.</p>}
      <RealErrors result={result} title="Real error: create_auto_cdc_flow on Spark 4.2 + Delta 4.4" />
    </div>
  );
}
