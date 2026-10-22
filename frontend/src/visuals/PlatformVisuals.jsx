import React, { useState } from 'react';
import { Empty, Stat, useStepper } from '../components/common.jsx';
import { Pre, RealErrors } from './shared.jsx';

const TYPE_LABEL = { notebook_task: 'notebook', pipeline_task: 'pipeline', spark_python_task: 'python file' };

export function JobsVisual({ result }) {
  const waves = result?.data.waves || [];
  const shown = useStepper(waves.length, result?.receivedAt, 650);
  const [tab, setTab] = useState('json');
  if (!result) return <Empty>Build a four-task job with the Databricks SDK and see the exact Jobs API request and bundle file.</Empty>;
  const d = result.data;
  const tasks = Object.fromEntries(d.tasks.map((t) => [t.key, t]));
  return (
    <div>
      <div className="job-dag">
        {waves.map((wave, i) => (
          <React.Fragment key={i}>
            {i > 0 && <span className={`job-arrow ${i < shown ? 'job-arrow-on' : ''}`}>→</span>}
            <div className="job-wave">
              {wave.map((key) => {
                const t = tasks[key];
                return (
                  <div key={key} className={`job-task ${i < shown ? 'job-task-on' : ''} ${t.runIf !== 'ALL_SUCCESS' ? 'job-task-alert' : ''}`}>
                    <b>{key}</b>
                    <span className="muted">{TYPE_LABEL[t.type]}{t.retries ? ` · ${t.retries} retries` : ''}</span>
                    {t.runIf !== 'ALL_SUCCESS' && <span className="chip chip-miss">run_if {t.runIf}</span>}
                  </div>
                );
              })}
            </div>
          </React.Fragment>
        ))}
      </div>
      <div className="stat-row">
        <Stat label="schedule" value={d.schedule.quartz_cron_expression} hint={`${d.schedule.timezone_id} · ${d.schedule.pause_status}`} />
        <Stat label="job cluster" value={`${d.cluster.new_cluster.num_workers} × ${d.cluster.new_cluster.node_type_id}`} hint={d.cluster.new_cluster.spark_version} />
        <Stat label="databricks-sdk" value={d.sdkVersion} />
        <Stat label="workspace" value={d.live.enabled ? (d.live.user || (d.live.rateLimited ? 'rate-limited' : 'live')) : 'offline'} tone={d.live.enabled ? 'ok' : undefined}
          hint={d.live.enabled ? 'read-only calls' : 'set DATABRICKS_HOST + TOKEN for live'} />
      </div>
      <div className="tabs">
        <button type="button" className={`btn btn-small ${tab === 'json' ? 'btn-accent' : 'btn-ghost'}`} onClick={() => setTab('json')}>POST /api/2.2/jobs/create body</button>
        <button type="button" className={`btn btn-small ${tab === 'yaml' ? 'btn-accent' : 'btn-ghost'}`} onClick={() => setTab('yaml')}>databricks.yml (Asset Bundle)</button>
      </div>
      {tab === 'json' ? <Pre text={JSON.stringify(d.body, null, 2)} /> : <Pre text={d.bundle} />}
      {d.mistake && <p className="hint">The client without credentials waited {d.mistake.waited} s for an unreachable host before failing, with retry_timeout_seconds=3. The default is 300 s.</p>}
      <RealErrors result={result} />
    </div>
  );
}
