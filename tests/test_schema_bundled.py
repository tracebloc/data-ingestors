"""Regression tests for #103 — bundled schema file must ship in the
wheel.

Before the fix, ``tracebloc_ingestor/schema/`` was not a Python package
(no ``__init__.py``), so ``find_packages()`` ignored it and the
``package_data`` declaration in ``setup.py`` referencing
``tracebloc_ingestor.schema`` was a silent no-op. The built wheel
shipped without ``ingest.v1.json`` and the released image crashed on
first call to ``cli.run._load_schema`` with:

    FileNotFoundError: [Errno 2] No such file or directory:
    '/usr/local/lib/python3.11/site-packages/tracebloc_ingestor/schema/
     ingest.v1.json'

These tests pin two things:

1. ``tracebloc_ingestor.schema`` is importable as a Python package
   (i.e. the marker ``__init__.py`` is present).
2. ``cli.run._load_schema`` resolves the schema file and returns a
   well-formed dict — meaning the file is discoverable through the
   same path the production code uses, not just on disk.

These run against the installed package (``pip install -e .`` or
the built wheel), so a future regression in the wheel-build pipeline
would surface here, not three weeks later in cluster validation.
"""

from __future__ import annotations

import importlib

import pytest


def test_schema_subpackage_is_importable():
    """The marker __init__.py exists and the subpackage imports cleanly.

    The wheel-bundling bug was caused by the absence of this marker —
    setuptools silently skipped data files attached to a directory it
    didn't see as a package. Asserting the import succeeds is the
    cheapest possible canary for that regression.
    """
    mod = importlib.import_module("tracebloc_ingestor.schema")
    assert mod is not None
    # Sanity: the module's __file__ points at the directory's
    # __init__.py, not somewhere unexpected.
    assert mod.__file__ is not None
    assert mod.__file__.endswith("__init__.py")


def test_load_schema_returns_v1_definition():
    """End-to-end: the production code path can load the schema.

    This is the exact call site that crashed in cluster validation.
    If the wheel ships without the JSON file, this raises
    FileNotFoundError; if the JSON is present but malformed, it
    raises json.JSONDecodeError. Both would be regressions.
    """
    from tracebloc_ingestor.cli.run import _load_schema

    schema = _load_schema()
    assert isinstance(schema, dict)
    # The v1 schema has well-known top-level keys; pin the most
    # invariant ones so a future schema bump doesn't silently
    # invalidate this test.
    assert schema.get("$schema") == "http://json-schema.org/draft-07/schema#"
    assert schema.get("title") == "tracebloc IngestConfig (v1)"
    assert "apiVersion" in schema.get("properties", {})
    assert "category" in schema.get("properties", {})
