import React, { useState } from 'react';
import { Empty, Stat, useStepper } from '../components/common.jsx';
import { RealErrors, RowsTable } from './shared.jsx';

/** _delta_log as a stack of commit files; each file lists the data files it added or removed. */
export function DeltaLogVisual({ result }) {
  const commits = result?.data.commits || [];
  const shown = useStepper(commits.length, result?.receivedAt, 520);
  if (!result) return <Empty>Run four writes and watch each one append a JSON commit to _delta_log.</Empty>;
  const d = result.data;
  return (
    <div>
      <div className="delta-log">
        {commits.slice(0, shown).map((c) => (
          <div key={c.version} className="commit pop-in">
            <div className="commit-head">
              <code>{c.file}</code>
              <span className="chip">v{c.version} · {c.operation}</span>
            </div>
            <div className="commit-actions">
              {c.protocol && <span className="action action-meta">protocol r{c.protocol.minReaderVersion}/w{c.protocol.minWriterVersion}</span>}
              {c.metaData && <span className="action action-meta">metaData (schema)</span>}
              {c.remove.map((r) => <span key={r.path} className="action action-remove" title={r.path}>− {r.path.slice(0, 14)}</span>)}
              {c.add.map((a) => (
                <span key={a.path} className={`action action-add ${a.dv ? 'action-dv' : ''}`} title={a.path}>
                  + {a.path.slice(0, 14)} · {a.numRecords} rows{a.dv ? ' · DV' : ''}
                </span>
              ))}
            </div>
          </div>
        ))}
      </div>
      <h4>After each write: rows, files the log points to, files on disk</h4>
      <div className="timeline-cols">
        {d.timeline.map((t) => (
          <div key={t.label} className="timeline-col slide-in">
            <b>{t.label}</b>
            <span>v{t.version}</span>
            <span>{t.rows} rows</span>
            <span className="ok">{t.liveFiles} live</span>
            <span className="muted">{t.diskFiles} on disk</span>
          </div>
        ))}
      </div>
      <p className="hint">{d.deletionVectors
        ? 'Deletion vectors on: UPDATE and DELETE re-add the same file with a small bitmap of deleted rows instead of rewriting it.'
        : 'Copy-on-write: UPDATE and DELETE remove the old file from the log and add a rewritten one. The old file stays on disk for time travel until VACUUM.'}</p>
      {d.mistake && (
        <div className="stat-row">
          <Stat label="file deleted by hand" value={d.mistake.deleted.slice(0, 14)} tone="bad" />
          <Stat label="rows that survived the repair" value={d.mistake.survivors} tone="warn" />
        </div>
      )}
      <RealErrors result={result} />
    </div>
  );
}

export function TimeTravelVisual({ result }) {
  const [pick, setPick] = useState(null);
  if (!result) return <Empty>Make four changes to a price list, then read any version back and restore one.</Empty>;
  const d = result.data;
  const version = pick ?? d.snapshots.length - 1;
  const snap = d.snapshots[version];
  return (
    <div>
      <div className="version-slider">
        {d.snapshots.map((s) => (
          <button key={s.version} type="button" className={`version-dot ${s.version === version ? 'version-active' : ''} ${s.version === d.restoredTo ? 'version-restored' : ''}`}
            onClick={() => setPick(s.version)}>
            <b>v{s.version}</b><span>{s.operation}</span>
          </button>
        ))}
        <div className="version-dot version-new"><b>v{d.latestVersion}</b><span>RESTORE → v{d.restoredTo}</span></div>
      </div>
      <div className="two-col">
        <div>
          <h4><code>SELECT * FROM prices VERSION AS OF {version}</code></h4>
          <div className="price-list" key={version}>
            {snap.rows.length === 0 && <p className="muted">(empty table)</p>}
            {snap.rows.map((r, i) => (
              <div key={r.product} className="price-item pop-in" style={{ animationDelay: `${i * 70}ms` }}>
                <span>{r.product}</span><b>{r.price.toFixed(2)}</b>
              </div>
            ))}
          </div>
        </div>
        <div>
          <h4>Current table after <code>RESTORE TO VERSION AS OF {d.restoredTo}</code></h4>
          <div className="price-list">
            {d.afterRestore.map((r, i) => (
              <div key={r.product} className="price-item price-restored pop-in" style={{ animationDelay: `${i * 70}ms` }}>
                <span>{r.product}</span><b>{r.price.toFixed(2)}</b>
              </div>
            ))}
          </div>
        </div>
      </div>
      <p className="hint">TIMESTAMP AS OF '{d.byTimestamp.timestamp}' returned the same rows as version 2. RESTORE wrote version {d.latestVersion}: history only grows.</p>
      <RealErrors result={result} />
    </div>
  );
}

export function MergeVisual({ result }) {
  if (!result) return <Empty>Apply five change events to a customer table in one MERGE: updates, a delete and inserts.</Empty>;
  const d = result.data;
  const ops = Object.fromEntries(d.changes.map((c) => [c.id, c.op]));
  const beforeIds = new Set(d.before.map((r) => r.id));
  const rowClass = (r) => (!beforeIds.has(r.id) ? 'row-new' : ops[r.id] ? 'row-changed' : '');
  return (
    <div>
      <div className="merge-grid">
        <RowsTable rows={d.before} caption="target (before)" columns={['id', 'name', 'tier']} rowClass={(r) => (ops[r.id] === 'DELETE' ? 'row-gone' : ops[r.id] ? 'row-fading' : '')} />
        <RowsTable rows={d.changes} caption="source: change events" columns={['id', 'name', 'tier', 'op']} rowClass={(r) => (r.op === 'DELETE' ? 'row-gone' : 'row-new')} />
        <RowsTable rows={d.after} caption="target (after MERGE)" columns={['id', 'name', 'tier']} rowClass={rowClass} />
      </div>
      <div className="stat-row">
        <Stat label="updated" value={d.metrics.num_updated_rows} tone="warn" />
        <Stat label="deleted" value={d.metrics.num_deleted_rows} tone="bad" />
        <Stat label="inserted" value={d.metrics.num_inserted_rows} tone="ok" />
        <Stat label="commits" value="1" hint="all or nothing" tone="accent" />
      </div>
      {d.mistake && <p className="hint">After de-duplicating the source, id 1's tier is <b>{d.mistake.tierAfterFix}</b> (the latest event won).</p>}
      <RealErrors result={result} />
    </div>
  );
}

export function SchemaVisual({ result }) {
  if (!result) return <Empty>Append data with a new column: rejected by default, accepted with mergeSchema.</Empty>;
  const d = result.data;
  return (
    <div>
      <div className="schema-steps">
        {d.schemas.map((s, i) => (
          <div key={s.step} className="schema-card pop-in" style={{ animationDelay: `${i * 140}ms` }}>
            <span className="chip">{s.step}</span>
            {s.columns.map(([name, type]) => (
              <div key={name} className={`schema-col ${i > 0 && !d.schemas[i - 1].columns.some((c) => c[0] === name) ? 'schema-new' : ''}`}>
                <code>{name}</code><span className="muted">{type}</span>
              </div>
            ))}
          </div>
        ))}
      </div>
      <RowsTable rows={d.rows} caption="Rows: older rows read the new columns as NULL" />
      <RealErrors result={result} title="Schema enforcement at work (real errors)" />
    </div>
  );
}

function FileStrip({ files, range, title }) {
  const max = 10000;
  return (
    <div className="file-strip">
      <h4>{title} · {files.length} files</h4>
      {files.map((f, i) => {
        const hit = f.min <= range[1] && f.max >= range[0];
        return (
          <div key={f.file + i} className="file-range-row">
            <div className="file-range-track">
              <div className={`file-range ${hit ? 'file-hit' : ''}`} style={{ left: `${(f.min / max) * 100}%`, width: `${Math.max(0.8, ((f.max - f.min) / max) * 100)}%`, animationDelay: `${i * 30}ms` }} />
              <div className="file-probe" style={{ left: `${(range[0] / max) * 100}%`, width: `${((range[1] - range[0]) / max) * 100}%` }} />
            </div>
            <span className="muted">{f.rows} rows</span>
          </div>
        );
      })}
    </div>
  );
}

export function OptimizeVisual({ result }) {
  if (!result) return <Empty>Write many small files, then OPTIMIZE: see each file's min/max key range and how many files a range query must open.</Empty>;
  const d = result.data;
  return (
    <div>
      <p className="hint">Each bar is one data file, spanning the min..max of <code>key</code> from its statistics. The shaded column is the query <code>WHERE key BETWEEN {d.probe.range[0]} AND {d.probe.range[1]}</code>; orange files must be read.</p>
      <div className="two-col">
        <FileStrip files={d.before} range={d.probe.range} title="Before" />
        <FileStrip files={d.after} range={d.probe.range} title={`After OPTIMIZE (${d.strategy})`} />
      </div>
      <div className="stat-row">
        <Stat label="files read before" value={d.probe.before.files_read} tone="bad" />
        <Stat label="files read after" value={d.probe.after.files_read} tone="ok" />
        <Stat label="matching rows" value={d.probe.after.n} hint="identical before and after" />
        <Stat label="files removed / added" value={`${d.metrics.numFilesRemoved} → ${d.metrics.numFilesAdded}`} />
      </div>
    </div>
  );
}

export function VacuumVisual({ result }) {
  if (!result) return <Empty>Update a table twice, then VACUUM: files no longer referenced are deleted for good.</Empty>;
  const d = result.data;
  const cells = (n, live, tone) => Array.from({ length: n }).map((_, i) => (
    <span key={i} className={`file-cell pop-in ${i < live ? 'file-live' : tone}`} style={{ animationDelay: `${i * 40}ms` }} />
  ));
  return (
    <div>
      <div className="two-col">
        <div><h4>Before VACUUM: {d.before.disk} files on disk, {d.before.live} referenced</h4><div className="file-cells">{cells(d.before.disk, d.before.live, 'file-old')}</div></div>
        <div><h4>After VACUUM RETAIN 0 HOURS: {d.after.disk} on disk</h4><div className="file-cells">{cells(d.after.disk, d.after.live, 'file-old')}</div></div>
      </div>
      <div className="stat-row">
        <Stat label="DRY RUN (7-day default)" value={`${d.dryRun} files`} hint="too recent to delete" />
        <Stat label="table versions" value={d.versions} />
        <Stat label="time travel to v1" value={d.timeTravelError ? 'broken ✗' : 'ok'} tone={d.timeTravelError ? 'bad' : 'ok'} />
      </div>
      <RealErrors result={result} />
    </div>
  );
}

const CHANGE_TONE = { insert: 'ok', update_preimage: 'muted', update_postimage: 'warn', delete: 'bad' };

export function CdfVisual({ result }) {
  const changes = result?.data.changes || [];
  const shown = useStepper(changes.length, result?.receivedAt, 220);
  if (!result) return <Empty>Insert, update, delete and merge on a table with the Change Data Feed on, then read the feed.</Empty>;
  return (
    <div>
      <div className="cdf-tape">
        {changes.slice(0, shown).map((c, i) => (
          <div key={i} className={`cdf-event pop-in cdf-${CHANGE_TONE[c._change_type]}`}>
            <span className="cdf-version">v{c._commit_version}</span>
            <b>{c.sku}</b><span>qty {c.qty}</span>
            <code>{c._change_type}</code>
          </div>
        ))}
      </div>
      <div className="stat-row">
        {['insert', 'update_preimage', 'update_postimage', 'delete'].map((t) => (
          <Stat key={t} label={t} value={changes.filter((c) => c._change_type === t).length} tone={CHANGE_TONE[t] === 'muted' ? undefined : CHANGE_TONE[t]} />
        ))}
      </div>
      <RealErrors result={result} />
    </div>
  );
}

export function ConstraintsVisual({ result }) {
  if (!result) return <Empty>Insert good and bad payments into a table with NOT NULL, CHECK, IDENTITY and a generated column.</Empty>;
  const d = result.data;
  return (
    <div>
      <div className="gate">
        {[['acc-1, 25.00', true], ['acc-2, 99.90', true], ['acc-3, 10.00 + acc-4, −5.00 (one batch)', false], ['NULL, 5.00', false]].map(([label, ok], i) => (
          <div key={label} className={`gate-row slide-in ${ok ? 'gate-pass' : 'gate-block'}`} style={{ animationDelay: `${i * 150}ms` }}>
            <code>{label}</code><span>{ok ? 'committed ✓' : 'rejected ✗'}</span>
          </div>
        ))}
      </div>
      <RowsTable rows={d.rows.map((r) => ({ ...r, paid_at: String(r.paid_at).slice(0, 16), paid_date: String(r.paid_date) }))} caption="The table: ids from IDENTITY, paid_date generated from paid_at" />
      <div className="stat-row">
        {Object.entries(d.constraints).map(([k, v]) => <Stat key={k} label={k} value={v} />)}
      </div>
      <RealErrors result={result} />
    </div>
  );
}
