"""Regression tests for #93: CSVIngestor must not clobber the cleaned
``file_options["schema"]`` that ``BaseIngestor.__init__`` injects.

Before the fix, ``CSVIngestor.__init__`` re-assigned
``self.file_options = file_options or {}`` after ``super().__init__``.
On the new YAML path with tabular/time-series categories the caller
passes ``file_options={}``, so the subclass replaced the dict base had
populated with a fresh empty dict — the cleaned schema (and
``number_of_columns``) were silently dropped before they reached
``map_validators`` and the metadata API call.
"""

from unittest.mock import MagicMock

from tracebloc_ingestor.ingestors.csv_ingestor import CSVIngestor


def _make_ingestor(schema, file_options, label_column=None):
    """Build a CSVIngestor with mock DB/API so ``BaseIngestor.__init__``
    runs end-to-end (including ``database.create_table``)."""
    database = MagicMock()
    api_client = MagicMock()
    return CSVIngestor(
        database=database,
        api_client=api_client,
        table_name="t",
        schema=schema,
        file_options=file_options,
        label_column=label_column,
    )


def test_yaml_path_empty_file_options_keeps_cleaned_schema():
    """YAML path: caller passes file_options={}. The cleaned schema base
    injects must survive subclass init."""
    schema = {"feature_a": "str", "feature_b": "int", "label": "str"}
    ing = _make_ingestor(schema=schema, file_options={}, label_column="label")

    assert "schema" in ing.file_options, (
        "subclass init wiped the schema base injected"
    )
    assert ing.file_options["schema"] == {"feature_a": "str", "feature_b": "int"}
    # number_of_columns must reflect the cleaned schema even when the YAML
    # path passes file_options={} (i.e. without a pre-seeded key).
    assert ing.file_options["number_of_columns"] == 2


def test_template_path_existing_file_options_sanitized_and_resized():
    """Old template path: caller passes file_options with its own schema
    and ``number_of_columns``. Base must overwrite ``schema`` with the
    cleaned version and update ``number_of_columns`` to match."""
    schema = {"feature_a": "str", "feature_b": "int", "label": "str"}
    file_options = {
        "schema": schema,  # not yet cleaned — includes label_column
        "number_of_columns": 3,
        "custom_flag": True,  # unrelated keys must be preserved
    }
    ing = _make_ingestor(
        schema=schema, file_options=file_options, label_column="label"
    )

    assert ing.file_options["schema"] == {"feature_a": "str", "feature_b": "int"}
    assert ing.file_options["number_of_columns"] == 2
    assert ing.file_options["custom_flag"] is True


def test_file_options_none_keeps_cleaned_schema():
    """Defensive: caller passes file_options=None (the default)."""
    schema = {"feature_a": "str", "label": "str"}
    ing = _make_ingestor(schema=schema, file_options=None, label_column="label")

    assert ing.file_options["schema"] == {"feature_a": "str"}
