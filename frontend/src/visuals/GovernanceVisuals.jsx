import React from 'react';
import { Empty, Stat, formatBytes } from '../components/common.jsx';
import { RealErrors, RowsTable } from './shared.jsx';

export function NamespaceVisual({ result }) {
  if (!result) return <Empty>Walk the metastore: catalog → schema → table, through the Unity Catalog REST API.</Empty>;
  const d = result.data;
  const target = d.table.full_name.split('.');
  return (
    <div>
      <div className="uc-tree">
        {d.tree.map((cat, ci) => (
          <div key={cat.name} className="uc-catalog slide-in" style={{ animationDelay: `${ci * 120}ms` }}>
            <div className={`uc-node uc-cat ${cat.name === d.current.catalog ? 'uc-current' : ''}`}>
              <span className="uc-icon">▣</span><b>{cat.name}</b><span className="muted">{cat.comment}</span>
            </div>
            {cat.schemas.map((sch) => (
              <div key={sch.name} className="uc-schema">
                <div className={`uc-node ${cat.name === d.current.catalog && sch.name === d.current.schema ? 'uc-current' : ''}`}>
                  <span className="uc-icon">◫</span><b>{sch.name}</b>
                </div>
                {sch.tables.map((t) => (
                  <div key={t.name} className={`uc-node uc-table ${cat.name === target[0] && sch.name === target[1] && t.name === target[2] ? 'uc-hit' : ''}`}>
                    <span className="uc-icon">▤</span><code>{t.name}</code><span className="chip chip-soft">{t.type.toLowerCase()} · {t.format}</span>
                  </div>
                ))}
              </div>
            ))}
          </div>
        ))}
      </div>
      <div className="stat-row">
        <Stat label="current catalog.schema" value={`${d.current.catalog}.${d.current.schema}`} tone="accent" />
        <Stat label="table" value={d.table.full_name} hint={`${d.table.type} · ${d.table.format}`} />
        <Stat label="columns stored in UC" value={d.table.columns.join(', ')} />
      </div>
      <RealErrors result={result} />
    </div>
  );
}

export function ManagedExternalVisual({ result }) {
  if (!result) return <Empty>Create a managed and an external table with the same data, then DROP both and look at the files.</Empty>;
  const d = result.data;
  const kinds = [
    ['managed', d.details.managed, d.afterDrop.managedFilesOnDisk, d.afterDrop.managedRegistered],
    ['external', d.details.external, d.afterDrop.externalFilesOnDisk, d.afterDrop.externalRegistered],
  ];
  return (
    <div>
      <div className="two-col">
        {kinds.map(([kind, info, filesAfter, registered]) => (
          <div key={kind} className={`table-kind slide-in kind-${kind}`}>
            <h4>{info.name}</h4>
            <span className="chip">{info.type}</span>
            <code className="path">{info.location}</code>
            <div className="kind-life">
              <span>registered in UC: <b>{registered ? 'yes' : 'no (dropped)'}</b></span>
              <span>data files on disk after DROP: <b className={filesAfter ? 'ok' : 'bad'}>{filesAfter}</b></span>
            </div>
            {info.features?.length > 0 && <div className="features">{info.features.map((f) => <span key={f} className="chip chip-soft">{f}</span>)}</div>}
          </div>
        ))}
      </div>
      <p className="hint">The external table's files survived DROP, so <code>CREATE TABLE … LOCATION</code> re-registered it with {d.reattachedRows} rows.
        {d.afterDrop.managedFilesOnDisk > 0
          ? ` Here the managed table's ${d.afterDrop.managedFilesOnDisk} files are still on disk, but nothing points at them any more.`
          : ' The managed table\'s files are gone with it.'}
        {' '}On Databricks, a dropped managed table's data is purged after 7 days, and UNDROP TABLE brings it back until then.</p>
      <RealErrors result={result} />
    </div>
  );
}

export function VolumesVisual({ result }) {
  if (!result) return <Empty>Drop CSV files into a Unity Catalog volume, read the folder with Spark and save it as a table.</Empty>;
  const d = result.data;
  return (
    <div>
      <div className="volume-flow">
        <div className="volume-box">
          <span className="chip">VOLUME · {d.volume.type}</span>
          <code>{d.volume.path}/exports/</code>
          {d.files.map((f, i) => (
            <div key={f.name} className="volume-file pop-in" style={{ animationDelay: `${i * 140}ms` }}>
              📄 {f.name} <span className="muted">{formatBytes(f.bytes)}</span>
            </div>
          ))}
        </div>
        <div className="volume-arrow">spark.read.csv →</div>
        <div className="volume-box">
          <span className="chip">TABLE · main.gov.signups</span>
          <RowsTable rows={d.rows} />
        </div>
      </div>
      <p className="hint">Storage behind the volume: <code>{d.volume.storage}</code></p>
      <RealErrors result={result} />
    </div>
  );
}

export function FunctionsVisual({ result }) {
  if (!result) return <Empty>Register a masking function in Unity Catalog and apply it to email addresses.</Empty>;
  const d = result.data;
  return (
    <div>
      <div className="fn-card">
        <code>{d.function.name}(email STRING) RETURNS STRING · LANGUAGE {d.function.language.toUpperCase()}</code>
        <pre className="code">{d.function.body}</pre>
        <span className="muted">functions in main.gov: {d.listed.join(', ')}</span>
      </div>
      <div className="mask-rows">
        {d.rows.map((r, i) => (
          <div key={r.name} className="mask-row slide-in" style={{ animationDelay: `${i * 150}ms` }}>
            <b>{r.name}</b><code className="mask-before">{r.email}</code><span>→</span><code className="mask-after">{r.masked}</code>
          </div>
        ))}
      </div>
      <RealErrors result={result} title="Real errors: what OSS Spark cannot do with UC functions" />
    </div>
  );
}
