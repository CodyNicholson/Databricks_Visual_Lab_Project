"""Offline tests run in the image's `test` stage (no Spark, UC or MLflow). The `live` tests run every
demo against the real stack, from inside the compose network:

    docker compose exec backend python -m pytest -q -m live
"""

import json

import pytest
from fastapi.testclient import TestClient

from app import core, seed
from app.demos import pipelines, platform
from app.lab import Demo, clamp, short_error
from app.main import app

client = TestClient(app)
# FastAPI 0.159 keeps included routers nested, so read the demo routes from the OpenAPI schema
SPEC = app.openapi()["paths"]
DEMO_ROUTES = sorted((path, method.upper()) for path, ops in SPEC.items() if path.startswith("/api/demos/") for method in ops)


def has_mistake_mode(path: str) -> bool:
    return any(p["name"] == "mistake" for op in SPEC[path].values() for p in op.get("parameters", []))


# ---- offline -------------------------------------------------------------------------------


def test_every_area_registers_demos():
    prefixes = {path.split("/")[3] for path, _ in DEMO_ROUTES}
    assert prefixes == {"basics", "delta", "uc", "stream", "pipelines", "ml", "platform", "mistakes"}
    assert len(DEMO_ROUTES) >= 25


def test_health_reports_starting_until_seeded():
    seed.state.update(ready=False, error="Spark still starting")
    response = client.get("/api/health")
    assert response.status_code == 503 and response.json()["status"] == "starting"


def test_clamp_bounds_visitor_numbers():
    assert clamp(10**9, 1, 50) == 50 and clamp(-3, 1, 50) == 1 and clamp(7, 1, 50) == 7


def test_short_error_drops_the_jvm_stack_trace():
    exc = RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] The table `x` cannot be found.\n\nJVM stacktrace:\norg.apache.spark...\n\tat a.b.C")
    assert short_error(exc) == "[TABLE_OR_VIEW_NOT_FOUND] The table `x` cannot be found."


def test_attempt_records_the_real_error_and_continues():
    d = Demo("t")
    value, error = d.attempt("1 / 0", lambda: 1 / 0)
    assert value is None and error.startswith("ZeroDivisionError")
    assert d.commands == ["1 / 0", f"  -> {error}"]


def test_delta_log_reader(tmp_path):
    log = tmp_path / "_delta_log"
    log.mkdir()
    stats = json.dumps({"numRecords": 2, "minValues": {"key": 1}, "maxValues": {"key": 9}})
    (log / "00000000000000000000.json").write_text("\n".join(json.dumps(a) for a in [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}, {"metaData": {}}, {"commitInfo": {"operation": "CREATE TABLE"}}]))
    (log / "00000000000000000001.json").write_text("\n".join(json.dumps(a) for a in [
        {"add": {"path": "part-0.parquet", "size": 10, "stats": stats}}, {"commitInfo": {"operation": "WRITE", "isBlindAppend": True}}]))
    commits = core.delta_log(tmp_path)
    assert [c["operation"] for c in commits] == ["CREATE TABLE", "WRITE"]
    assert commits[0]["metaData"] and commits[1]["add"][0]["numRecords"] == 2


def test_job_dag_waves_and_roundtrip():
    settings = platform.build_job()
    assert platform.topological_order(settings) == [["ingest"], ["refresh_pipeline"], ["train_model"], ["notify_failure"]]
    body = settings.as_dict()
    assert type(settings).from_dict(body).as_dict() == body


def test_pipeline_event_parser():
    line = "2026-10-21 18:01:02: Flow 'main.sdp.orders_raw' is RUNNING."
    m = pipelines.EVENT.match(line)
    assert m and m.group(2).split(".")[-1] == "orders_raw" and m.group(3) == "RUNNING"


def test_live_mode_is_off_without_a_token(monkeypatch):
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    assert platform.live_enabled() is False


# ---- live: the real stack -------------------------------------------------------------------


@pytest.fixture(scope="module")
def seeded():
    seed.seed()
    seed.state.update(ready=True, error=None)


@pytest.mark.live
def test_health(seeded):
    body = client.get("/api/health").json()
    assert body["status"] == "ok" and body["spark"].startswith("4.2")


@pytest.mark.live
@pytest.mark.parametrize("path,method", DEMO_ROUTES)
def test_every_demo_passes_its_own_checks(seeded, path, method):
    """Mirrors the UI's "Run all demos": every route under /api/demos must pass its self-checks."""
    response = client.request(method, path)
    assert response.status_code == 200, response.text[:500]
    body = response.json()
    assert body["checks"], path
    failed = [c for c in body["checks"] if not c["passed"]]
    assert not failed, f"{path}: {failed}"


@pytest.mark.live
@pytest.mark.parametrize("path,method", [r for r in DEMO_ROUTES if has_mistake_mode(r[0])])
def test_pitfall_mode_shows_the_real_error(seeded, path, method):
    """Pitfall demos run the wrong way for real (a captured error, or a measured slowdown) and verify it
    with extra checks of their own, which must pass like the rest."""
    normal = client.request(method, path).json()
    body = client.request(method, path, params={"mistake": True}).json()
    assert len(body["checks"]) > len(normal["checks"]), f"{path}: mistake mode verified nothing extra"
    failed = [c for c in body["checks"] if not c["passed"]]
    assert not failed, f"{path}: {failed}"
