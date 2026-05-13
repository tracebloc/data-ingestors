[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE) [![PyPI](https://img.shields.io/pypi/v/tracebloc-ingestor.svg)](https://pypi.org/project/tracebloc-ingestor/) [![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://python.org) [![Platform](https://img.shields.io/badge/platform-tracebloc-00C9A7.svg)](https://ai.tracebloc.io)

# Data Ingestors 📊

Move your data into the [tracebloc](https://tracebloc.io/) training environment — validated, clean, and ready for model evaluation. **Your raw data never leaves your infrastructure.**

## How it works

```
Your raw data
     │
     ▼
┌──────────────────┐     ┌──────────────────────────────────┐
│  Data ingestor   │────►│  Your Kubernetes cluster         │
│                  │     │                                  │
│  Validates       │     │  Validated dataset               │
│  Preprocesses    │     │  (ready for training)            │
│  Transfers       │     │                                  │
└──────────────────┘     └──────────────┬───────────────────┘
                                        │
                               Metadata only
                                        │
                                        ▼
                         ┌──────────────────────────┐
                         │  tracebloc web app       │
                         │  (dataset management UI) │
                         └──────────────────────────┘
```

Only metadata (schema, statistics, structure) syncs to the web app. Raw data stays put.

## Supported data types

| Type | Templates |
|---|---|
| **Image** | [`image_classification`](templates/image_classification), [`object_detection`](templates/object_detection) |
| **Text / NLP** | [`text_classification`](templates/text_classification) |
| **Tabular** | [`tabular_classification`](templates/tabular_classification), [`tabular_regression`](templates/tabular_regression) |
| **Time series** | [`time_series_forecasting`](templates/time_series_forecasting), [`time_to_event_prediction`](templates/time_to_event_prediction) |

Each template is a runnable starting point — copy it, point it at your data, ship it.

## Quickstart

### 1. Install

```bash
pip install tracebloc-ingestor
```

### 2. Pick a template

```bash
cp templates/image_classification/ingestor.py .
```

Each template builds on the same primitives — `BaseIngestor`, `CSVIngestor`, validators — and overrides the parts that vary by data type.

### 3. Deploy as a Kubernetes Job

The ingestor runs *inside* your cluster, next to a tracebloc client. The provided [`Dockerfile`](Dockerfile) and [`ingestor-job.yaml`](ingestor-job.yaml) are the canonical pattern:

```bash
docker build -t <your-registry>/<image-name>:latest .
docker push <your-registry>/<image-name>:latest
kubectl apply -f ingestor-job.yaml
```

The Job needs these environment variables (set in [`ingestor-job.yaml`](ingestor-job.yaml)):

| Variable | What it is |
|---|---|
| `CLIENT_ID`, `CLIENT_PASSWORD` | Tracebloc client credentials |
| `CLIENT_PVC` | PVC name shared with the client (must match `values.yaml`) |
| `MYSQL_HOST` | Hostname of the client's MySQL service |
| `SRC_PATH` | Where your raw data is mounted in the ingestor pod |
| `LABEL_FILE` | Path to labels (e.g. `Xy_train.csv`) |
| `TABLE_NAME` | Destination table name in the client database |
| `TITLE` | *(optional)* Human-readable dataset name |
| `LOG_LEVEL` | *(optional)* `INFO`, `WARNING`, `ERROR` |

### Running under Pod Security Standards (`restricted`)

If the namespace you're deploying into enforces the [`restricted`](https://kubernetes.io/docs/concepts/security/pod-security-standards/) Pod Security Standard (OpenShift, hardened clusters, many managed-Kubernetes namespaces), the stock [`Dockerfile`](Dockerfile) and [`ingestor-job.yaml`](ingestor-job.yaml) won't admit. Two changes are needed.

Check first:

```bash
kubectl get ns <namespace> -o jsonpath='{.metadata.labels}' | jq
```

Look for `pod-security.kubernetes.io/enforce: restricted`. If absent, the stock files admit fine and you can skip this section.

**1. `Dockerfile` — drop root.** Append before `ENTRYPOINT`:

```dockerfile
# OpenShift-compatible: grant group write via GID 0
RUN chgrp -R 0 /app && chmod -R g=u /app
USER 1001
```

**2. `ingestor-job.yaml` — add a hardened `securityContext`.** Both pod-level and container-level:

```yaml
spec:
  template:
    spec:
      securityContext:                    # pod-level
        runAsNonRoot: true
        runAsUser: 1001
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: api
        # ... existing container spec ...
        securityContext:                  # container-level
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
```

## Writing a custom ingestor

For data that doesn't fit a template, subclass `BaseIngestor`:

```python
from tracebloc_ingestor import BaseIngestor, FileTypeValidator

class MyIngestor(BaseIngestor):
    validators = [FileTypeValidator(allowed=[".parquet"])]

    def transform(self, record):
        # your preprocessing
        return record

if __name__ == "__main__":
    MyIngestor().ingest()
```

The package exports `BaseIngestor`, `CSVIngestor`, `JSONIngestor`, plus validators (`FileTypeValidator`, `ImageResolutionValidator`, `TableNameValidator`) and the `Database` / `APIClient` helpers. See [`examples/`](examples) for working scripts.

## Prerequisites

- Python 3.8+
- A [tracebloc account](https://ai.tracebloc.io/signup)
- A running [tracebloc client](https://github.com/tracebloc/client) on your infrastructure

## Links

[Platform](https://ai.tracebloc.io/) · [Docs](https://docs.tracebloc.io/) · [Data preparation guide](https://docs.tracebloc.io/create-use-case/prepare-dataset) · [Discord](https://discord.gg/tracebloc)

## License

Apache 2.0 — see [LICENSE](LICENSE).

**Questions?** [support@tracebloc.io](mailto:support@tracebloc.io) or [open an issue](https://github.com/tracebloc/data-ingestors/issues).
