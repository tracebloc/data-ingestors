# syntax=docker/dockerfile:1.7
#
# Official tracebloc data-ingestor image (#45).
#
# The image entrypoint is the declarative `tracebloc-ingest` console script
# registered by setup.py (#44). The Helm subchart (client#86) mounts the
# customer's ingest.yaml at the path pointed to by INGEST_CONFIG. No customer
# Python script lives in the image; no customer-built Dockerfile is needed.

# ---- Builder: produce a wheel from the source in this repo so the image
#       ships the exact code being released (not whatever's on PyPI). ----
FROM python:3.11-slim AS builder
WORKDIR /build

COPY setup.py requirements.txt Readme.md ./
COPY tracebloc_ingestor/ tracebloc_ingestor/

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip wheel --no-deps --wheel-dir /wheels .

# ---- Runtime ----
FROM python:3.11-slim
WORKDIR /app

# Runtime deps:
#   netcat-traditional — docker-entrypoint.sh uses `nc -z` to wait for MySQL.
RUN apt-get update \
 && apt-get install -y --no-install-recommends netcat-traditional \
 && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cacheable layer), then the wheel built above.
# Using --no-deps on the wheel avoids re-resolving against PyPI; deps come
# from requirements.txt so the resolution is reproducible at release time.
COPY requirements.txt /tmp/requirements.txt
COPY --from=builder /wheels/*.whl /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
 && pip install --no-cache-dir --no-deps /tmp/*.whl \
 && rm /tmp/*.whl /tmp/requirements.txt

ENV CUDA_VISIBLE_DEVICES=-1
ENV TF_CPP_MIN_LOG_LEVEL=2

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Run as non-root. The process needs read access to /custom/ (processor
# scripts mounted via ConfigMap by client#86) and write access to whatever
# PVC the Helm chart mounts at the configured DEST_PATH.
USER nobody

ENTRYPOINT ["docker-entrypoint.sh"]
