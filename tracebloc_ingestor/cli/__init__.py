"""Command-line entry point for the declarative ``ingest.yaml`` flow.

The official ingestor image (Ticket #45) runs ``tracebloc_ingestor.cli.run:main``
as its entrypoint. The flow:

    1. Read INGEST_CONFIG env (path to the YAML body mounted by the Helm
       subchart from Ticket client#86).
    2. Validate against ``schema/ingest.v1.json``; fail fast with line-numbered
       errors before any DB connection or network call.
    3. Resolve convention defaults from ``category`` (validators, sidecar
       patterns, default columns, default file extensions) — see ``conventions``.
    4. Dispatch to ``CSVIngestor`` or ``JSONIngestor`` based on the source type.
    5. Apply ``label.policy`` bucketing in ``APIClient.send_batch`` for
       regression-class tasks.

Customers no longer write Python or build Docker images for the dominant case.
"""
