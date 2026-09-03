"""Every `ingest.yaml` a template README tells the user to write must validate.

A template README is the copy-paste path: a user stages their data, pastes the
block, and runs `helm install`. When the schema moves and the README does not,
that paste fails validation — and it fails in the user's cluster, against a
config the repo itself handed them.

This has now happened twice on one change. data-ingestors#552 made
`object_detection` reject `csv:` and `label:` outright (records are enumerated
from `annotations/*.xml`), and correctly updated the schema, the ingestor, the
template's own Python entrypoint and `examples/yaml/object_detection.yaml` — but
left `templates/object_detection/README.md` telling users to write exactly the
two keys the schema now rejects. Writing this guard also turned up a second,
unrelated staleness in `templates/keypoint_detection/README.md`, whose block
predates `target_size`/`number_of_keypoints` becoming required.

`examples/yaml/` was already pinned by `test_schema_validation.py`; the README
blocks were the copy of the same information that nothing checked. That is the
whole failure: the drift was invisible because the *authoritative* copy was
guarded and the *user-facing* one was not.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO_ROOT / "tracebloc_ingestor" / "schema" / "ingest.v1.json"
TEMPLATES_DIR = REPO_ROOT / "templates"

# ```yaml fenced blocks. Only those declaring `kind: IngestConfig` are configs;
# a README may legitimately fence other YAML (a Helm values snippet, a k8s
# manifest) and those are not this schema's business.
YAML_BLOCK = re.compile(r"```yaml\n(.*?)```", re.DOTALL)


@pytest.fixture(scope="module")
def validator() -> Draft7Validator:
    return Draft7Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))


def _ingest_configs(readme: Path) -> list[tuple[int, dict]]:
    blocks = YAML_BLOCK.findall(readme.read_text(encoding="utf-8"))
    out = []
    for index, block in enumerate(blocks):
        if "kind: IngestConfig" not in block:
            continue
        out.append((index, yaml.safe_load(block)))
    return out


def _readmes() -> list[Path]:
    return sorted(TEMPLATES_DIR.glob("*/README.md"))


@pytest.mark.parametrize("readme", _readmes(), ids=lambda p: p.parent.name)
def test_readme_ingest_config_validates(validator, readme):
    configs = _ingest_configs(readme)
    assert configs, (
        f"{readme.relative_to(REPO_ROOT)} declares no `kind: IngestConfig` block. "
        f"Every template README documents the declarative quickstart, so a "
        f"missing block means either the README lost it or the fence markers "
        f"changed and this guard has stopped reading it."
    )
    for index, config in configs:
        errors = sorted(validator.iter_errors(config), key=lambda e: list(e.path))
        assert not errors, (
            f"{readme.relative_to(REPO_ROOT)} block #{index} does not validate "
            f"against ingest.v1.json — a user pasting it would be rejected:\n  "
            + "\n  ".join(f"{list(e.path)}: {e.message}" for e in errors)
        )


def test_every_template_is_covered():
    """Guard the guard: the parametrisation must actually have found READMEs.

    `glob` returning nothing would collect zero parametrised cases and report
    a clean run, so the count is pinned against the template directories that
    exist. A new category adds a directory and a README; this fails until the
    README is written, which is the point.
    """
    template_dirs = {p.name for p in TEMPLATES_DIR.iterdir() if p.is_dir()}
    with_readme = {p.parent.name for p in _readmes()}
    assert (
        template_dirs == with_readme
    ), f"templates without a README: {sorted(template_dirs - with_readme)}"
    assert len(with_readme) >= 16, f"only {len(with_readme)} template READMEs found"
