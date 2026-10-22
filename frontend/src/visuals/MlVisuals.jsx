import React from 'react';
import { Bar, Empty, Stat, formatMs } from '../components/common.jsx';
import { RealErrors, RowsTable } from './shared.jsx';

export function TrackingVisual({ result }) {
  if (!result) return <Empty>Train four models on taxi trips; every run's params and metrics land on the MLflow tracking server.</Empty>;
  const d = result.data;
  const byId = Object.fromEntries(d.runs.map((r) => [r.runId, r]));
  const ranked = d.ranking.map((id) => byId[id]).filter(Boolean);
  const max = Math.max(...d.runs.map((r) => r.mae));
  return (
    <div>
      <h4>mlflow.search_runs(order_by=["metrics.mae ASC"]) · experiment {d.experiment.name}</h4>
      <div className="leaderboard">
        {ranked.map((r, i) => (
          <div key={r.runId} className={`leader-row ${r.runId === d.best ? 'leader-best' : ''}`} style={{ transitionDelay: `${i * 80}ms` }}>
            <span className="leader-rank">#{i + 1}</span>
            <div className="leader-name"><b>{r.name}</b><code>{r.runId.slice(0, 8)}</code></div>
            <div className="leader-params">{Object.entries({ model: r.model, ...r.params }).map(([k, v]) => <span key={k} className="chip chip-soft">{k}={v}</span>)}</div>
            <div className="leader-bar"><Bar label="MAE ($)" value={r.mae} max={max} tone={i === 0 ? 'ok' : 'accent'} text={r.mae.toFixed(3)} /></div>
            <span className="muted">r² {r.r2.toFixed(3)} · fit {formatMs(r.fit_ms)}</span>
          </div>
        ))}
      </div>
      <p className="hint">The fares in the sample data are linear in distance, so plain linear regression wins: comparing runs, not guessing, is what tracking is for.</p>
      <RealErrors result={result} />
    </div>
  );
}

export function RegistryVisual({ result }) {
  if (!result) return <Empty>Log two models with signatures and register them as versions of main.ml.fare_model in Unity Catalog.</Empty>;
  const d = result.data;
  return (
    <div>
      <div className="registry">
        <div className="registry-model">
          <span className="chip">MODEL · Unity Catalog</span>
          <b>{d.model.name}</b>
          <code className="path">{d.model.storage}</code>
        </div>
        <div className="registry-versions">
          {d.versions.map((v, i) => {
            const uc = d.ucVersions.find((u) => u.version === v.version);
            return (
              <div key={v.version} className={`registry-version pop-in ${v.version === d.best.version ? 'registry-best' : ''}`} style={{ animationDelay: `${i * 200}ms` }}>
                <b>version {v.version}</b>
                <span>max_depth={v.depth} · MAE {v.mae.toFixed(3)}</span>
                <span className="chip chip-soft">{uc?.status}</span>
                <code>{uc?.source}</code>
                {v.version === d.best.version && <span className="chip chip-hit">serving: models:/{d.model.name}/{v.version}</span>}
              </div>
            );
          })}
        </div>
      </div>
      <h4>Signature (enforced at predict time)</h4>
      <div className="features">{d.signature.map((c) => <span key={c.name} className="chip chip-code">{c.name}: {c.type.replace('DataType.', '')}</span>)}</div>
      <RowsTable rows={d.predictions} caption="Batch scoring → main.ml.fare_predictions" />
      <div className="stat-row">
        <Stat label="mean abs error of these 5" value={`$${(d.predictions.reduce((s, p) => s + Math.abs(p.predicted - p.actual), 0) / d.predictions.length).toFixed(2)}`} tone="accent" />
      </div>
      <RealErrors result={result} />
    </div>
  );
}
