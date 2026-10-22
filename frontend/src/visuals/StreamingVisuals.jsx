import React from 'react';
import { Empty, Stat, formatMs, useStepper } from '../components/common.jsx';
import { RealErrors, RowsTable } from './shared.jsx';

export function FileStreamVisual({ result }) {
  const runs = result?.data.runs || [];
  const shown = useStepper(runs.length, result?.receivedAt, 700);
  if (!result) return <Empty>Land JSON files, run the stream three times and watch the checkpoint skip what it already read.</Empty>;
  const d = result.data;
  const seen = new Set(runs.slice(0, shown).flatMap((r) => r.newFiles));
  return (
    <div>
      <div className="landing">
        <h4>landing/clicks/</h4>
        <div className="landing-files">
          {d.files.map((f) => <span key={f} className={`landing-file ${seen.has(f) ? 'landing-done' : ''}`}>{f}</span>)}
        </div>
      </div>
      <div className="stream-runs">
        {runs.slice(0, shown).map((r, i) => (
          <div key={i} className="stream-run pop-in">
            <b>Run {i + 1}: {r.label}</b>
            <span>{r.newFiles.length ? `new: ${r.newFiles.join(', ')}` : 'no new files'}</span>
            <span className={r.inputRows ? 'ok' : 'muted'}>+{r.inputRows} rows read</span>
            <span>table now {r.totalRows} rows · {formatMs(r.ms)}</span>
          </div>
        ))}
      </div>
      <div className="checkpoint">
        <h4>checkpoints/clicks_bronze/</h4>
        {Object.entries(d.checkpoint).map(([dir, files]) => (
          <div key={dir} className="checkpoint-dir"><code>{dir}/</code>{files.map((f) => <span key={f} className="chip chip-soft">{f}</span>)}</div>
        ))}
      </div>
      <RealErrors result={result} />
    </div>
  );
}

export function DeltaSourceVisual({ result }) {
  if (!result) return <Empty>Append rows to a bronze Delta table and stream them into silver, one increment at a time.</Empty>;
  const d = result.data;
  return (
    <div>
      <div className="stream-runs">
        {d.runs.map((r, i) => (
          <div key={i} className="stream-run pop-in" style={{ animationDelay: `${i * 200}ms` }}>
            <b>{r.label}</b>
            <span className="ok">{r.inputRows} new rows read</span>
            <span>silver: {r.silverRows} rows</span>
            <span>{formatMs(r.ms)}</span>
          </div>
        ))}
      </div>
      <RowsTable rows={d.silver} caption="orders_silver (negative amounts filtered, tax added)" />
      {d.mistake && <p className="hint">With skipChangeCommits the stream skipped the UPDATE commit ({d.mistake.afterFix.inputRows} rows read) and kept going. The updated value never reaches silver: use the Change Data Feed when updates matter.</p>}
      <RealErrors result={result} />
    </div>
  );
}

export function MedallionVisual({ result }) {
  const shown = useStepper(4, result?.receivedAt, 650);
  if (!result) return <Empty>Push 10 messy raw lines through bronze → silver → gold, with a quarantine for bad records.</Empty>;
  const d = result.data;
  const layers = [
    ['bronze', 'every raw line, untouched', d.counts.bronze, 'layer-bronze'],
    ['silver', 'parsed, valid, de-duplicated', d.counts.silver, 'layer-silver'],
    ['quarantine', 'bad records + reason', d.counts.quarantine, 'layer-quarantine'],
    ['gold', 'business aggregate', d.counts.gold, 'layer-gold'],
  ];
  return (
    <div>
      <div className="medallion">
        {layers.slice(0, shown).map(([name, text, count, cls]) => (
          <div key={name} className={`layer pop-in ${cls}`}>
            <b>{name}</b><span className="layer-count">{count}</span><span className="muted">{text}</span>
          </div>
        ))}
      </div>
      {shown >= 4 && (
        <div className="two-col">
          <div>
            <h4>Raw lines</h4>
            <ul className="raw-lines">
              {d.raw.map((line, i) => {
                const bad = d.quarantine.find((q) => q.raw === line);
                return <li key={i} className={bad ? 'raw-bad' : ''}><code>{line}</code>{bad && <span className="chip chip-miss">{bad.reason}</span>}</li>;
              })}
            </ul>
          </div>
          <div>
            <RowsTable rows={d.gold} caption="gold: revenue per store" />
            <div className="stat-row"><Stat label="silver events" value={d.counts.silver} tone="accent" /><Stat label="quarantined" value={d.counts.quarantine} tone="warn" /></div>
          </div>
        </div>
      )}
    </div>
  );
}
