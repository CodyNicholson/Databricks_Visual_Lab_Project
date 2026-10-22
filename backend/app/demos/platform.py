"""The Databricks platform layer, through the official Databricks SDK for Python: a multi-task job
(Lakeflow Jobs) built with the SDK's typed classes, the exact Jobs API request it sends, the same
job as an Asset Bundle, and unified authentication. Offline by default; with DATABRICKS_HOST and
DATABRICKS_TOKEN in .env it also makes read-only calls to that workspace, rate-limited."""

from __future__ import annotations

import os
import threading
import time
from collections import deque

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk import version as sdk_version
from databricks.sdk.core import Config
from databricks.sdk.service import compute, jobs
from fastapi import APIRouter

from ..lab import Demo

router = APIRouter(prefix="/api/demos/platform")
OFFLINE_HOST = "https://dbc-lab-offline.cloud.databricks.com"  # never resolves: nothing leaves the lab
PER_MINUTE = int(os.environ.get("LIVE_RUNS_PER_MINUTE", "5"))
PER_HOUR = int(os.environ.get("LIVE_RUNS_PER_HOUR", "60"))
_live_calls: deque[float] = deque()
_live_lock = threading.Lock()


def live_enabled() -> bool:
    return bool(os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"))


def live_allowed() -> bool:
    """A sliding-window rate limit: the site is public and the workspace is someone's real one."""
    now = time.time()
    with _live_lock:
        while _live_calls and now - _live_calls[0] > 3600:
            _live_calls.popleft()
        if len(_live_calls) >= PER_HOUR or sum(1 for t in _live_calls if now - t < 60) >= PER_MINUTE:
            return False
        _live_calls.append(now)
        return True


def build_job() -> jobs.JobSettings:
    """A daily taxi job: ingest (notebook) -> refresh the pipeline -> train, plus an alert task."""
    cluster = compute.ClusterSpec(spark_version="17.3.x-scala2.13", node_type_id="i3.xlarge", num_workers=2,
                                  data_security_mode=compute.DataSecurityMode.SINGLE_USER)
    return jobs.JobSettings(
        name="taxi-daily",
        tags={"team": "data-eng", "lab": "databricks"},
        max_concurrent_runs=1,
        timeout_seconds=3600,
        schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="UTC", pause_status=jobs.PauseStatus.PAUSED),
        parameters=[jobs.JobParameterDefinition(name="run_date", default="{{job.start_time.iso_date}}")],
        job_clusters=[jobs.JobCluster(job_cluster_key="etl", new_cluster=cluster)],
        email_notifications=jobs.JobEmailNotifications(on_failure=["oncall@example.com"]),
        tasks=[
            jobs.Task(task_key="ingest", job_cluster_key="etl", max_retries=2, min_retry_interval_millis=60_000,
                      notebook_task=jobs.NotebookTask(notebook_path="/Workspace/lab/ingest_trips", source=jobs.Source.WORKSPACE,
                                                      base_parameters={"date": "{{job.parameters.run_date}}"})),
            jobs.Task(task_key="refresh_pipeline", depends_on=[jobs.TaskDependency(task_key="ingest")],
                      pipeline_task=jobs.PipelineTask(pipeline_id="0000-orders-pipeline", full_refresh=False)),
            jobs.Task(task_key="train_model", depends_on=[jobs.TaskDependency(task_key="refresh_pipeline")], job_cluster_key="etl",
                      spark_python_task=jobs.SparkPythonTask(python_file="/Workspace/lab/train.py", parameters=["--model", "main.ml.fare_model"])),
            jobs.Task(task_key="notify_failure", depends_on=[jobs.TaskDependency(task_key="train_model")], run_if=jobs.RunIf.AT_LEAST_ONE_FAILED,
                      job_cluster_key="etl", notebook_task=jobs.NotebookTask(notebook_path="/Workspace/lab/alert", source=jobs.Source.WORKSPACE)),
        ],
    )


def topological_order(settings: jobs.JobSettings) -> list[list[str]]:
    """Group tasks into waves: a task runs once every task it depends on has finished."""
    remaining = {t.task_key: {dep.task_key for dep in (t.depends_on or [])} for t in settings.tasks}
    waves, done = [], set()
    while remaining:
        ready = sorted(k for k, deps in remaining.items() if deps <= done)
        if not ready:
            raise ValueError(f"cycle between {sorted(remaining)}")
        waves.append(ready)
        done |= set(ready)
        for k in ready:
            del remaining[k]
    return waves


def offline_config(**kwargs) -> Config:
    # No ~/.databrickscfg, no env credentials, and a 3 s cap instead of the default 300 s retry budget
    return Config(host=OFFLINE_HOST, retry_timeout_seconds=3, config_file="/nonexistent/.databrickscfg", **kwargs)


@router.post("/jobs")
def jobs_with_sdk(mistake: bool = False):
    with Demo("sdk-jobs") as d:
        settings = build_job()
        d.command(f"from databricks.sdk.service import jobs, compute  # databricks-sdk {sdk_version.__version__}")
        d.command("settings = jobs.JobSettings(name='taxi-daily', job_clusters=[...], tasks=[jobs.Task(task_key='ingest', ...), ...])")
        body = settings.as_dict()
        d.command(f"settings.as_dict()  -> the JSON body of POST /api/2.2/jobs/create ({len(str(body))} chars)")
        roundtrip = jobs.JobSettings.from_dict(body)
        waves = topological_order(settings)
        d.command(f"run order from depends_on: {' -> '.join('[' + ', '.join(w) + ']' for w in waves)}")
        bundle = {"bundle": {"name": "taxi_lab"},
                  "resources": {"jobs": {"taxi_daily": body}},
                  "targets": {"dev": {"mode": "development", "default": True}, "prod": {"mode": "production"}}}
        bundle_yaml = yaml.safe_dump(bundle, sort_keys=False, width=120)
        d.command("yaml.safe_dump({'resources': {'jobs': {'taxi_daily': body}}})  -> databricks.yml (deploy with `databricks bundle deploy -t dev`)")
        tasks = [{"key": t.task_key, "type": next(k for k in ("notebook_task", "pipeline_task", "spark_python_task") if getattr(t, k)),
                  "dependsOn": [dep.task_key for dep in (t.depends_on or [])], "runIf": t.run_if.value if t.run_if else "ALL_SUCCESS",
                  "retries": t.max_retries or 0} for t in settings.tasks]
        live = {"enabled": live_enabled()}
        if live_enabled():
            if live_allowed():
                w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
                me, ms = d.timed("w.current_user.me()  # live, read-only", w.current_user.me)
                listed, ms2 = d.timed("w.jobs.list(limit=5)  # live, read-only", lambda: [j.settings.name for j in w.jobs.list(limit=5)])
                live.update(user=me.user_name, jobs=listed[:5])
            else:
                live["rateLimited"] = True
                d.command(f"live call skipped: rate limit ({PER_MINUTE}/min, {PER_HOUR}/h)")
        d.data.update(sdkVersion=sdk_version.__version__, tasks=tasks, waves=waves, body=body, bundle=bundle_yaml, live=live,
                      schedule=body["schedule"], cluster=body["job_clusters"][0])
        d.check("The SDK's typed classes serialize to the Jobs API JSON and back without loss", roundtrip.as_dict() == body, f"{len(body['tasks'])} tasks")
        d.check("depends_on turns the tasks into a DAG with 4 sequential waves", waves == [["ingest"], ["refresh_pipeline"], ["train_model"], ["notify_failure"]], waves)
        d.check("The alert task runs only when something upstream failed", tasks[-1]["runIf"] == "AT_LEAST_ONE_FAILED", tasks[-1]["runIf"])
        d.check("The bundle file carries the same job definition", yaml.safe_load(bundle_yaml)["resources"]["jobs"]["taxi_daily"] == body, "databricks.yml")
        if live_enabled() and "user" in live:
            d.check("Live: the token authenticated against the workspace", bool(live["user"]), live["user"])
        if mistake:
            started = time.perf_counter()
            _, no_auth = d.attempt(f"WorkspaceClient(host='{OFFLINE_HOST}')  # no token, no profile, no env vars",
                                   lambda: WorkspaceClient(config=offline_config()))
            waited = round(time.perf_counter() - started, 1)
            _, cycle = d.attempt("tasks with depends_on: a -> b -> a", lambda: topological_order(jobs.JobSettings(tasks=[
                jobs.Task(task_key="a", depends_on=[jobs.TaskDependency(task_key="b")]),
                jobs.Task(task_key="b", depends_on=[jobs.TaskDependency(task_key="a")])])))
            client = WorkspaceClient(config=offline_config(token="dapi-lab-placeholder"))
            d.command(f"WorkspaceClient(config=Config(host=..., token=..., retry_timeout_seconds=3))  -> auth_type={client.config.auth_type}")
            d.check("Without credentials the SDK's unified auth fails with 'cannot configure default credentials'",
                    no_auth and "cannot configure default credentials" in no_auth, no_auth)
            d.check("An unreachable host blocks the constructor until retry_timeout_seconds (3 s here, 300 s by default)",
                    2.5 <= waited < 30, f"waited {waited} s")
            d.check("With a token (from env or a secret store, never in code) the client configures PAT auth", client.config.auth_type == "pat", client.config.auth_type)
            d.check("A dependency cycle cannot be scheduled", cycle and "cycle" in cycle, cycle)
            d.data["mistake"] = {"noAuth": no_auth, "waited": waited, "cycle": cycle}
    return d.result()
