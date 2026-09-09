"""A survival template's sample must be fittable by a Cox proportional-hazards model.

The template samples are what a first-time user ingests, and what any automated
"does this task train?" check ingests too. For the survival (time-to-event)
template the reference baseline is lifelines' ``CoxPHFitter``, which fits every
column except the time and event columns as a covariate and inverts the Hessian
of the partial likelihood at each Newton step. That inversion needs the covariate
matrix to have FULL COLUMN RANK: a constant column, a duplicated column, or any
exact linear combination among the covariates makes the Hessian singular and the
fit dies with ``ConvergenceError: Convergence halted due to matrix inversion
problems``.

That is exactly how the previous sample failed. Its 30 rows were 15 distinct
covariate vectors, each written twice, and the 11 covariate columns spanned only
8 dimensions -- so the very first training step on it could never succeed, and
nothing here said so. Tiling or jittering rows downstream cannot add rank, so
the property has to hold in the file that ships.

DERIVED, NOT RESTATED. Which templates are survival templates, which column is
the time and which the event, all come from each template's own entrypoint
(``"time_column": ...`` and ``label_column=...``), never from a list kept here.
Zero survival templates found is a failure, not a pass: it would mean the
declarations moved and this guard stopped reading them.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

import numpy as np
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = REPO_ROOT / "templates"

_TIME_COLUMN = re.compile(r"""["']time_column["']\s*:\s*["']([^"']+)["']""")
_LABEL_COLUMN = re.compile(r"""label_column\s*=\s*["']([^"']+)["']""")


def _survival_templates() -> list[tuple[Path, str, str]]:
    """Every template whose entrypoint declares a ``time_column``: (dir, time, event)."""
    found = []
    for entry in sorted(TEMPLATES_DIR.glob("*/*.py")):
        source = entry.read_text(encoding="utf-8")
        time_match = _TIME_COLUMN.search(source)
        if not time_match:
            continue
        label_match = _LABEL_COLUMN.search(source)
        assert label_match, (
            f"{entry.relative_to(REPO_ROOT)} declares a time_column but no "
            f"label_column; a survival template needs both, and this guard "
            f"cannot tell the event column apart from a covariate without it."
        )
        found.append((entry.parent, time_match.group(1), label_match.group(1)))
    return found


def _covariate_matrix(sample: Path, time_column: str, event_column: str):
    with sample.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows, f"{sample.relative_to(REPO_ROOT)} has no data rows"
    covariates = [c for c in rows[0].keys() if c not in (time_column, event_column)]
    assert covariates, f"{sample.relative_to(REPO_ROOT)} has no covariate columns"
    matrix = np.array([[float(row[c]) for c in covariates] for row in rows], dtype=float)
    return covariates, matrix


SURVIVAL_TEMPLATES = _survival_templates()


def test_at_least_one_survival_template_is_declared():
    assert SURVIVAL_TEMPLATES, (
        "no template entrypoint under templates/*/ declares a time_column. Either "
        "the survival template is gone or its declaration changed shape and this "
        "guard no longer reads it -- both are findings, not a pass."
    )


@pytest.mark.parametrize(
    "template_dir,time_column,event_column",
    SURVIVAL_TEMPLATES,
    ids=[t[0].name for t in SURVIVAL_TEMPLATES],
)
def test_survival_sample_covariates_have_full_column_rank(
    template_dir: Path, time_column: str, event_column: str
):
    samples = sorted(template_dir.glob("*.csv"))
    assert len(samples) == 1, (
        f"{template_dir.relative_to(REPO_ROOT)} ships {len(samples)} CSV file(s); "
        f"expected exactly one sample to check."
    )
    covariates, matrix = _covariate_matrix(samples[0], time_column, event_column)

    # A Cox fit needs more observations than covariates before rank is even a
    # question; a sample that fails this is too small to train on at all.
    assert matrix.shape[0] > len(covariates), (
        f"{samples[0].name}: {matrix.shape[0]} rows for {len(covariates)} "
        f"covariates -- a Cox PH fit needs more rows than covariates."
    )

    # lifelines centres the covariates before fitting, so rank is judged on the
    # centred matrix: a constant column contributes nothing and counts as missing.
    centred = matrix - matrix.mean(axis=0)
    rank = int(np.linalg.matrix_rank(centred))
    constant = [c for c, s in zip(covariates, centred.std(axis=0)) if s == 0]
    assert rank == len(covariates), (
        f"{samples[0].name}: the {len(covariates)} covariate columns span only "
        f"{rank} dimensions (constant: {constant or 'none'}), so the Cox PH "
        f"Hessian is singular and the reference model cannot fit this sample. "
        f"Fix the sample: every covariate needs spread of its own, and no column "
        f"may be a copy or an exact combination of the others."
    )
