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

| Type | Categories |
|---|---|
| **Image** | [`image_classification`](templates/image_classification), [`object_detection`](templates/object_detection), [`keypoint_detection`](templates/keypoint_detection), [`semantic_segmentation`](templates/semantic_segmentation) |
| **Text / NLP** | [`text_classification`](templates/text_classification), [`masked_language_modeling`](templates/masked_language_modeling) |
| **Tabular** | [`tabular_classification`](templates/tabular_classification), [`tabular_regression`](templates/tabular_regression) |
| **Time series** | [`time_series_forecasting`](templates/time_series_forecasting), [`time_to_event_prediction`](templates/time_to_event_prediction) |

Each template ships a sample dataset and an [example `ingest.yaml`](examples/yaml/) you can copy as a starting point.

## Quickstart — declarative YAML (recommended)

Describe your dataset in ~8 lines of YAML, then `helm install`. The official ingestor image (this package, signed + SBOM-attested, published as `ghcr.io/tracebloc/ingestor`) runs it. No Dockerfile, no Python script.

**1. One-time: add the chart repo on your workstation.**

```bash
helm repo add tracebloc https://tracebloc.github.io/client
helm repo update
```

The `tracebloc/client` parent chart bootstraps the cluster (jobs-manager, MySQL, RBAC). The `tracebloc/ingestor` subchart submits per-dataset ingestion runs against it.

> **Already installed the client via the one-liner (`bash <(curl -fsSL https://tracebloc.io/i.sh)`)?** Use `--reset-then-reuse-values` so the helm upgrade doesn't drop the values the installer applied:
>
> ```bash
> helm upgrade <workspace> tracebloc/client -n <namespace> --reset-then-reuse-values
> ```
>
> Append `--version <version-number>` to pin a specific chart version.

**2. Stage your data on the cluster's shared PVC.**

The chart **doesn't transport data into the cluster** — it points at data already accessible to the cluster's shared PVC (`client-pvc` by default, mounted at `/data/shared/` inside the ingestor Pod). Before installing, get your raw files there. The simplest pattern for a small dataset is a throwaway `kubectl cp` Pod that mounts the PVC; for production you'd typically use an init container with cloud-storage sync. Full staging recipe + manifests → [`tracebloc/client/ingestor/README.md#stage-your-data-on-the-shared-pvc`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md#stage-your-data-on-the-shared-pvc).

**3. Write your `ingest.yaml`.**

The example below is for `image_classification`. **Other categories require different fields** — e.g. `tabular_classification` has no `images:` and instead needs a typed `schema:` block. Don't copy this one blindly; grab the matching file from [`examples/yaml/`](examples/yaml/) (one per category) and edit from there. Per-category sample data and READMEs live under [`templates/`](https://github.com/tracebloc/data-ingestors/tree/master/templates).

```yaml
apiVersion: tracebloc.io/v1
kind: IngestConfig
category: image_classification
table: cats_dogs_train
intent: train
csv: /data/shared/cats-dogs/labels.csv
images: /data/shared/cats-dogs/images/
label: label
```

The top-level shape (`apiVersion`, `kind`, `category`, `table`, `intent`, `label`) is the same for every category; the `category` field picks the validator set, file-extension defaults, and column conventions, and the data-source fields (`csv:`, `images:`, `schema:`, …) vary per category. The paths are *paths inside the ingestor Pod*, which is the PVC mount you populated in step 2.

**4. Install once per dataset.**

```bash
helm install my-cats-dogs tracebloc/ingestor \
  --namespace tracebloc \
  --set-file ingestConfig=./ingest.yaml
```

The ingestor runs once: validates your data, copies files into the destination directory on the PVC, inserts rows into MySQL, sends metadata to the tracebloc backend, then exits. Repeat per dataset. Customers never build an image, never write a Dockerfile, never track digest versions — the cluster's auto-upgrade flow keeps the official image current.

Full chart docs (data-staging recipe, schema, every category, update model, verification, override knobs) → **[`tracebloc/client/ingestor/README.md`](https://github.com/tracebloc/client/blob/develop/ingestor/README.md)**.

## Advanced: custom processors (legacy Python pattern)

Use this when the declarative schema can't express what your data needs — typically when you have non-trivial preprocessing logic, a custom validator, or a `BaseProcessor` subclass.

**1. Install the package.**

```bash
pip install tracebloc-ingestor
```

**2. Pick a template + adapt the script.**

```bash
cp templates/image_classification/image_classification.py .
```

The package exports `BaseIngestor`, `CSVIngestor`, `JSONIngestor`, plus validators (`FileTypeValidator`, `ImageResolutionValidator`, `TableNameValidator`, etc.) and the `Database` / `APIClient` helpers. See [`examples/`](examples) for working scripts.

**3. Build + deploy as a Kubernetes Job.**

The legacy [`Dockerfile`](Dockerfile) and [`ingestor-job.yaml`](ingestor-job.yaml) remain the canonical pattern for custom-processor flows:

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

### Running custom-processor flows under Pod Security Standards (`restricted`)

If the namespace you're deploying into enforces the [`restricted`](https://kubernetes.io/docs/concepts/security/pod-security-standards/) Pod Security Standard (OpenShift, hardened clusters, many managed-Kubernetes namespaces), the stock [`Dockerfile`](Dockerfile) and [`ingestor-job.yaml`](ingestor-job.yaml) won't admit. (The declarative path's image is already PSA-restricted-compatible; this section only applies to custom Dockerfiles built from this repo.) Two changes are needed.

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

### Subclassing BaseIngestor

For data that doesn't fit any of the existing templates, subclass `BaseIngestor`:

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

## Prerequisites

- Python 3.8+
- A [tracebloc account](https://ai.tracebloc.io/signup)
- A running [tracebloc client](https://github.com/tracebloc/client) on your infrastructure

## Links

[Platform](https://ai.tracebloc.io/) · [Docs](https://docs.tracebloc.io/) · [Data preparation guide](https://docs.tracebloc.io/create-use-case/prepare-dataset) · [Discord](https://discord.gg/tracebloc)

## License

Apache 2.0 — see [LICENSE](LICENSE).

**Questions?** [support@tracebloc.io](mailto:support@tracebloc.io) or [open an issue](https://github.com/tracebloc/data-ingestors/issues).
