import React from 'react';

/** The newest result of one button, from the demo's history. */
export const lastOf = (history, actionId) => [...history].reverse().find((h) => h.actionId === actionId);

/** The genuine errors a "Do it wrong" run captured: every command line the backend marked with "->". */
export function RealErrors({ result, title = 'The real errors this run hit' }) {
  const lines = (result?.commands || [])
    .map((c, i) => [c, result.commands[i - 1]])
    .filter(([c]) => c.startsWith('  -> ') && !c.includes('exit code 0'));
  if (!lines.length) return null;
  return (
    <div className="real-errors">
      <h4>{title}</h4>
      {lines.map(([line, cmd], i) => (
        <div key={i} className="real-error slide-in" style={{ animationDelay: `${i * 120}ms` }}>
          {cmd && !cmd.startsWith('  ') && <code className="real-error-cmd">{cmd}</code>}
          <pre className="pitfall-symptom">{line.slice(5)}</pre>
        </div>
      ))}
    </div>
  );
}

/** A compact table of rows (objects), with optional highlighting per row. */
export function RowsTable({ rows, columns, rowClass, caption, max = 12 }) {
  if (!rows?.length) return <p className="muted">(no rows)</p>;
  const cols = columns || Object.keys(rows[0]);
  return (
    <div className="table-wrap">
      {caption && <h4>{caption}</h4>}
      <table>
        <thead><tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr></thead>
        <tbody>
          {rows.slice(0, max).map((r, i) => (
            <tr key={i} className={`slide-in ${rowClass ? rowClass(r) : ''}`} style={{ animationDelay: `${i * 50}ms` }}>
              {cols.map((c) => <td key={c}>{r[c] == null ? <span className="muted">NULL</span> : String(r[c])}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > max && <p className="muted">… {rows.length - max} more rows</p>}
    </div>
  );
}

/** A labelled pre block for plans, YAML and JSON. */
export function Pre({ title, text, className = '' }) {
  if (!text) return null;
  return (
    <div className={`pre-box ${className}`}>
      {title && <h4>{title}</h4>}
      <pre className="code">{text}</pre>
    </div>
  );
}
