"""Machine learning on the lakehouse with MLflow 3 (built into Databricks): experiment tracking,
and the model registry in Unity Catalog (`registry_uri = "uc:..."`, three-level model names)."""

from __future__ import annotations

import logging
import time

import mlflow
import numpy as np
from fastapi import APIRouter
from mlflow.models import infer_signature
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

from .. import core
from ..lab import Demo, clamp
from ..seed import TRIPS

router = APIRouter(prefix="/api/demos/ml")
EXPERIMENT = "/Shared/lab/taxi-fares"
MODEL = "main.ml.fare_model"
FEATURES = ["trip_distance", "pickup_zip"]
# MLflow 3 saves scikit-learn models with skops (no pickle) and refuses types it does not know
TRUSTED = ["sklearn.tree._tree.Tree"]
logging.getLogger("mlflow").setLevel(logging.WARNING)


def setup() -> None:
    mlflow.set_tracking_uri(core.MLFLOW_URL)
    mlflow.set_registry_uri(f"uc:{core.UC_URL}")  # what Databricks sets by default: models live in Unity Catalog
    while mlflow.active_run():  # a previous request that failed half-way must not leak its run
        mlflow.end_run()


def training_data(d: Demo, rows: int):
    """A deterministic sample of the trips table, pulled to the driver as pandas for scikit-learn."""
    df = core.spark().read.table(TRIPS).select(*FEATURES, "fare_amount").orderBy("tpep_pickup_datetime").limit(rows)
    pdf, ms = d.timed(f"spark.read.table('{TRIPS}').select({', '.join(FEATURES)}, 'fare_amount').limit({rows}).toPandas()", df.toPandas)
    return train_test_split(pdf[FEATURES], pdf["fare_amount"], test_size=0.25, random_state=7)


@router.post("/tracking")
def experiment_tracking(rows: int = 4000, mistake: bool = False):
    n = clamp(rows, 500, 20_000)
    setup()
    with Demo("mlflow-tracking") as d:
        X_train, X_test, y_train, y_test = training_data(d, n)
        experiment = mlflow.set_experiment(EXPERIMENT)
        d.command(f"mlflow.set_tracking_uri('{core.MLFLOW_URL}'); mlflow.set_experiment('{EXPERIMENT}')")
        grid = [("linear", {}), ("forest", {"n_estimators": 20, "max_depth": 3}),
                ("forest", {"n_estimators": 20, "max_depth": 6}), ("forest", {"n_estimators": 40, "max_depth": 10})]
        tag = f"lab-{int(time.time() * 1000)}"
        runs = []
        for kind, params in grid:
            with mlflow.start_run(run_name=f"{kind}-{params.get('max_depth', 'ols')}") as run:
                model = LinearRegression() if kind == "linear" else RandomForestRegressor(random_state=7, n_jobs=1, **params)
                started = time.perf_counter()
                model.fit(X_train, y_train)
                fit_ms = round((time.perf_counter() - started) * 1000, 1)
                pred = model.predict(X_test)
                metrics = {"mae": round(float(mean_absolute_error(y_test, pred)), 4), "r2": round(float(r2_score(y_test, pred)), 4), "fit_ms": fit_ms}
                mlflow.log_params({"model": kind, **params, "rows": n})
                mlflow.log_metrics(metrics)
                mlflow.set_tag("lab_batch", tag)
                d.command(f"with mlflow.start_run():  fit {kind}{params or ''}; log_params(...); log_metrics(mae={metrics['mae']}, r2={metrics['r2']})")
                runs.append({"runId": run.info.run_id, "name": run.info.run_name, "model": kind, "params": params, **metrics})
        found = mlflow.search_runs([experiment.experiment_id], filter_string=f"tags.lab_batch = '{tag}'", order_by=["metrics.mae ASC"], output_format="list")
        d.command(f"mlflow.search_runs(filter_string=\"tags.lab_batch = '{tag}'\", order_by=['metrics.mae ASC'])  -> {len(found)} runs")
        best = found[0]
        stored = mlflow.get_run(best.info.run_id)
        d.command(f"mlflow.get_run('{best.info.run_id[:8]}…').data.metrics -> {stored.data.metrics}")
        d.data.update(experiment={"name": EXPERIMENT, "id": experiment.experiment_id}, runs=runs, rows=n,
                      ranking=[r.info.run_id for r in found], best=best.info.run_id)
        d.check("Every training run was recorded on the tracking server", len(found) == len(grid), f"{len(found)} runs")
        d.check("search_runs ranks runs by a metric (lowest MAE first)",
                [r.data.metrics["mae"] for r in found] == sorted(r["mae"] for r in runs), [r.data.metrics["mae"] for r in found])
        d.check("Params and metrics are stored with the run, not just printed",
                stored.data.params.get("model") in ("linear", "forest") and "mae" in stored.data.metrics, stored.data.params)
        depth = {r["params"].get("max_depth"): r["mae"] for r in runs if r["model"] == "forest"}
        d.check("Comparing runs exposes underfitting: the depth-3 forest is worse than depth 6", depth[3] > depth[6],
                {r["name"]: r["mae"] for r in runs})
        if mistake:
            outer = mlflow.start_run(run_name="forgot-to-end")
            d.command("run = mlflow.start_run()  # ...and the code never calls end_run() (no `with` block)")
            try:
                _, active_error = d.attempt("mlflow.start_run()  # the next training run", lambda: mlflow.start_run())
                mlflow.log_param("max_depth", 3)
                d.command("mlflow.log_param('max_depth', 3)")
                _, param_error = d.attempt("mlflow.log_param('max_depth', 6)  # same run, new value", lambda: mlflow.log_param("max_depth", 6))
            finally:
                mlflow.end_run()
                d.command("mlflow.end_run()")
            with mlflow.start_run(run_name="fixed", nested=False) as fixed:
                mlflow.log_param("max_depth", 6)
            d.command("with mlflow.start_run(): ...  # the `with` block always ends the run")
            mlflow.delete_run(outer.info.run_id)
            mlflow.delete_run(fixed.info.run_id)
            d.check("Starting a run while one is still active fails", active_error and "already active" in active_error, active_error)
            d.check("A param cannot change value within a run (log a metric, or start a new run)",
                    param_error and "Changing param values is not allowed" in param_error, param_error)
            d.check("`with mlflow.start_run()` closes the run, so the next one starts cleanly", mlflow.active_run() is None, "no active run")
            d.data["mistake"] = {"active": active_error, "param": param_error}
    return d.result()


def drop_model(d: Demo) -> None:
    """Start each run from an empty registered model: delete its versions, then the model."""
    if core.uc("GET", f"/models/{MODEL}").status_code == 404:
        return
    for v in core.uc_json("GET", f"/models/{MODEL}/versions", None).get("model_versions", []):
        core.uc("DELETE", f"/models/{MODEL}/versions/{v['version']}")
    core.uc("DELETE", f"/models/{MODEL}", d, params={"force": "true"})


@router.post("/registry")
def model_registry(mistake: bool = False):
    setup()
    s = core.spark()
    with Demo("uc-model-registry") as d:
        core.ensure_schema(d, "ml")
        drop_model(d)
        X_train, X_test, y_train, y_test = training_data(d, 4000)
        mlflow.set_experiment(EXPERIMENT)
        versions = []
        for depth in (3, 8):
            with mlflow.start_run(run_name=f"register-depth-{depth}") as run:
                model = RandomForestRegressor(n_estimators=20, max_depth=depth, random_state=7, n_jobs=1).fit(X_train, y_train)
                mae = round(float(mean_absolute_error(y_test, model.predict(X_test))), 4)
                mlflow.log_metric("mae", mae)
                signature = infer_signature(X_train, model.predict(X_train))
                info, ms = d.timed(f"mlflow.sklearn.log_model(model, name='model', signature=signature, registered_model_name='{MODEL}', skops_trusted_types=TRUSTED)  # depth {depth}",
                                   lambda: mlflow.sklearn.log_model(model, name="model", signature=signature, registered_model_name=MODEL,
                                                                    input_example=X_train.head(3), skops_trusted_types=TRUSTED))
                versions.append({"version": int(info.registered_model_version), "depth": depth, "mae": mae, "runId": run.info.run_id})
        uc_model = core.uc_json("GET", f"/models/{MODEL}", d)
        uc_versions = core.uc_json("GET", f"/models/{MODEL}/versions", d).get("model_versions", [])
        best = min(versions, key=lambda v: v["mae"])
        loaded = mlflow.pyfunc.load_model(f"models:/{MODEL}/{best['version']}")
        d.command(f"mlflow.pyfunc.load_model('models:/{MODEL}/{best['version']}')")
        sample = X_test.head(5)
        predictions = [round(float(p), 2) for p in loaded.predict(sample)]
        d.command(f"model.predict(5 test rows) -> {predictions}")
        # Batch scoring: predictions written back to a governed Delta table next to the model
        scored = s.createDataFrame([(float(a), int(b), float(p), float(y)) for (a, b), p, y in
                                    zip(sample.itertuples(index=False), predictions, y_test.head(5))],
                                   "trip_distance DOUBLE, pickup_zip INT, predicted_fare DOUBLE, actual_fare DOUBLE")
        scored.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("main.ml.fare_predictions")
        d.command("predictions.write.mode('overwrite').saveAsTable('main.ml.fare_predictions')")
        signature = loaded.metadata.get_input_schema()
        d.data.update(model={"name": MODEL, "storage": uc_model.get("storage_location")}, versions=versions,
                      ucVersions=[{"version": v["version"], "status": v.get("status"), "source": v.get("source", "")[:80],
                                   "storage": v.get("storage_location")} for v in uc_versions],
                      best=best, predictions=[{"distance": float(a), "zip": int(b), "predicted": p, "actual": float(y)}
                                              for (a, b), p, y in zip(sample.itertuples(index=False), predictions, y_test.head(5))],
                      signature=[{"name": c.name, "type": str(c.type)} for c in signature.inputs])
        d.check("Both models registered as versions 1 and 2 of one UC model", sorted(v["version"] for v in versions) == [1, 2], versions)
        d.check("Unity Catalog lists the versions as READY", len(uc_versions) == 2 and all(v.get("status") == "READY" for v in uc_versions),
                [v.get("status") for v in uc_versions])
        d.check("The model's files live in governed UC storage", str(uc_model.get("storage_location", "")).startswith("file:///lakehouse"),
                uc_model.get("storage_location"))
        d.check("The loaded model enforces the logged signature (2 inputs)", [c["name"] for c in d.data["signature"]] == FEATURES, d.data["signature"])
        d.check("Predictions were written to a Delta table", core.scalar(d, "SELECT count(*) FROM main.ml.fare_predictions") == 5, "5 rows")
        if mistake:
            client = mlflow.MlflowClient()
            _, alias_error = d.attempt(f"client.set_registered_model_alias('{MODEL}', 'champion', {best['version']})",
                                       lambda: client.set_registered_model_alias(MODEL, "champion", best["version"]))
            sig = infer_signature(X_train, model.predict(X_train))
            with mlflow.start_run(run_name="mistakes"):
                _, untrusted = d.attempt("mlflow.sklearn.log_model(forest, name='model', signature=sig)  # no skops_trusted_types",
                                         lambda: mlflow.sklearn.log_model(model, name="model", signature=sig))
                _, name_error = d.attempt("mlflow.sklearn.log_model(..., registered_model_name='fare_model')  # no catalog.schema",
                                          lambda: mlflow.sklearn.log_model(model, name="model", signature=sig, skops_trusted_types=TRUSTED,
                                                                           registered_model_name="fare_model"))
            pinned = mlflow.pyfunc.load_model(f"models:/{MODEL}/{best['version']}")
            d.command(f"mlflow.pyfunc.load_model('models:/{MODEL}/{best['version']}')  # pin the version until aliases are available")
            d.check("MLflow 3 refuses to serialize a type it has not been told to trust", untrusted and "untrusted types" in untrusted, untrusted)
            d.check("Model names in Unity Catalog must be catalog.schema.model", name_error and "expected 3" in name_error, name_error)
            d.check("Aliases (@champion) are a Databricks UC feature: OSS UC 0.7's MLflow store raises NotImplementedError",
                    alias_error and "NotImplementedError" in alias_error, alias_error)
            d.check("Loading by explicit version works everywhere", np.allclose(pinned.predict(X_test.head(5)), predictions, atol=0.01), "same predictions")
            d.data["mistake"] = {"alias": alias_error, "twoLevel": name_error, "untrusted": untrusted}
    return d.result()
