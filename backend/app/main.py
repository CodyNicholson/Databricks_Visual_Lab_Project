"""Databricks Visual Lab API. One endpoint per feature under /api/demos/*, each returning a DemoResult."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from . import core, seed
from .demos import basics, delta, governance, mistakes, ml, pipelines, platform, streaming
from .lab import short_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(_: FastAPI):
    if os.environ.get("LAB_SKIP_SEED") != "1":
        seed.seed_in_background()
    yield


app = FastAPI(title="Databricks Visual Lab", docs_url="/api/docs", openapi_url="/api/openapi.json", lifespan=lifespan)

for module in (basics, delta, governance, streaming, pipelines, ml, platform, mistakes):
    app.include_router(module.router)


@app.middleware("http")
async def one_demo_at_a_time(request: Request, call_next):
    """Demos rewrite shared tables on a single-node cluster, so they run one after another."""
    if not request.url.path.startswith("/api/demos/"):
        return await call_next(request)
    acquired = await run_in_threadpool(core.DEMO_LOCK.acquire, True, core.LOCK_WAIT_S)
    if not acquired:
        return JSONResponse({"error": "The lab cluster is busy with other visitors' demos. Try again in a minute."}, status_code=503)
    try:
        return await call_next(request)
    finally:
        core.DEMO_LOCK.release()


@app.exception_handler(Exception)
async def unexpected_error(_: Request, exc: Exception):
    if "Connection refused" in str(exc) or "UNAVAILABLE" in str(exc):
        core.reset_session()
    return JSONResponse({"error": f"{type(exc).__name__}: {short_error(exc, 400)}"}, status_code=500)


@app.get("/api/health")
def health():
    if not seed.state["ready"]:
        return JSONResponse({"status": "starting", "detail": seed.state["error"]}, status_code=503)
    return {"status": "ok", "spark": core.spark().version}


@app.get("/api/status")
def status():
    """Versions and live/offline mode for the header."""
    return {"ready": seed.state["ready"], "spark": core.spark().version if seed.state["ready"] else None,
            "delta": "4.4.1", "unityCatalog": "0.7.0", "mlflow": "3.17.2",
            "databricksLive": platform.live_enabled()}
