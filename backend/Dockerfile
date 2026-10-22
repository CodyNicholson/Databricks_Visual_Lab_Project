# Python 3.10 matches the Python inside the apache/spark 4.2.0 image: Spark Connect sends Python
# UDFs to the server to run, and both sides must use the same minor version.
FROM python:3.10-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    GIT_PYTHON_REFRESH=quiet MLFLOW_TELEMETRY_DISABLED=true DO_NOT_TRACK=true
WORKDIR /app
COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements.txt -r requirements-dev.txt

# Test stage: `docker build --target test ./backend` runs the offline unit tests. The live tests
# (every demo against the real stack) run inside the compose network; see the README.
FROM base AS test
COPY app ./app
COPY tests ./tests
COPY pytest.ini .
RUN LAB_SKIP_SEED=1 python -m pytest -q -m "not live"

FROM base AS runtime
COPY app ./app
COPY tests ./tests
COPY pytest.ini .
# uid 185 = the spark user in the Spark image: both write to the shared /lakehouse volume
RUN groupadd --gid 185 lab && useradd --uid 185 --gid 185 --create-home lab
USER lab
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
