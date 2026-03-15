[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE) [![PyPI](https://img.shields.io/pypi/v/tracebloc-ingestor.svg)](https://pypi.org/project/tracebloc-ingestor/) [![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://python.org) [![Platform](https://img.shields.io/badge/platform-tracebloc-00C9A7.svg)](https://ai.tracebloc.io)

# tracebloc Data Ingestors 📊

Get your data into the [tracebloc](https://tracebloc.io/) training environment — validated, clean, and ready for model evaluation.

These pipelines handle the full data preparation workflow: validation, preprocessing, and secure transfer into your Kubernetes cluster. A metadata representation syncs to the tracebloc web app so you can manage datasets visually. Your raw data never leaves your infrastructure.

## How it works

```
Your raw data
     │
     ▼
┌─────────────────┐     ┌──────────────────────────────────┐
│  Data ingestor   │────►│  Your Kubernetes cluster          │
│                  │     │                                   │
│  Validates       │     │  Validated dataset                │
│  Preprocesses    │     │  (ready for training)             │
│  Transfers       │     │                                   │
└─────────────────┘     └──────────────┬────────────────────┘
                                       │
                              Metadata only
                                       │
                                       ▼
                        ┌──────────────────────────┐
                        │  tracebloc web app        │
                        │  (dataset management UI)  │
                        └──────────────────────────┘
```

Data stays on your infrastructure. Only metadata (structure, schema, statistics) syncs to the web app for dataset management and vendor guidance.

## Supported data types

| Type | Examples |
|---|---|
| **Image** | Classification, detection, segmentation datasets |
| **Text / NLP** | Document classification, sentiment, named entities |
| **Tabular** | Structured CSV data, feature tables |
| **Time series** | Sequential measurements, forecasting datasets |

## Install

```bash
pip install tracebloc-ingestor
```

## Prerequisites

- Python 3.8+
- A [tracebloc account](https://ai.tracebloc.io/signup) with an active use case
- A running [tracebloc client](https://github.com/tracebloc/client) on your infrastructure

For step-by-step data preparation instructions → [Prepare Data guide](https://docs.tracebloc.io/create-use-case/prepare-dataset)

## Links

[Platform](https://ai.tracebloc.io/) · [Docs](https://docs.tracebloc.io/) · [Data preparation guide](https://docs.tracebloc.io/create-use-case/prepare-dataset) · [Discord](https://discord.gg/tracebloc)

## License

Apache 2.0 — see [LICENSE](LICENSE).

**Questions?** [support@tracebloc.io](mailto:support@tracebloc.io) or [open an issue](https://github.com/tracebloc/data-ingestors/issues).
