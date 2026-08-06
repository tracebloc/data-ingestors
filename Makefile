# Makefile for tracebloc/data-ingestors — uniform entry points (backend#1606).
#
# Every active tracebloc repo exposes the SAME three targets, so "run
# your tests before you push" stops being a rule you can only obey with
# per-repo tribal knowledge:
#
#   make check      lint + fast tests.   Budget: under 60 s.
#   make check-all  everything CI runs (bar the CI-only heavy suites).
#   make setup      install what those targets need.
#
# This file is a THIN WRAPPER over tests.yml, e2e.yml and
# code-quality-caller.yml. It introduces no new tool, no new config and
# no new rule. When a workflow changes, change the matching line here.
#
# Uses whatever python/pytest is on PATH, i.e. your active virtualenv.

.DEFAULT_GOAL := help

PYTHON ?= python3
PYTEST ?= $(PYTHON) -m pytest

# Lint tools are invoked through $(PYTHON) -m, not as bare commands on
# PATH. `setup` pip-installs them into $(PYTHON)'s environment at a
# pinned version; a bare `ruff` would resolve to whatever happens to be
# first on PATH — a homebrew build, another venv — and the pin the
# comment promises would silently not be the thing that ran. (Bugbot,
# tracebloc/data-ingestors#461.)

# Pinned to the version the org code-quality gate runs
# (tracebloc/.github code-quality.yml, `ruff-version`). ruff's rule set
# moves between releases, so an unpinned local install and CI can
# disagree about the same file. Bump this with the workflow, not apart
# from it.
RUFF_VERSION ?= 0.15.20

.PHONY: help
help:
	@echo "tracebloc/data-ingestors — make targets"
	@echo
	@echo "  check       lint + the unit suite (~11 s) — run this before every push"
	@echo "  check-all   everything CI runs, including the 95% coverage gate"
	@echo "  setup       pip install -r requirements-dev.txt && pip install -e ."
	@echo
	@echo "  individual: lint test coverage e2e"
	@echo
	@echo "  e2e needs a real MySQL on MYSQL_HOST/MYSQL_PORT — it is not part of"
	@echo "  check or check-all for that reason. CI provides one as a service."

# ---- check: the pre-push tier ------------------------------------
#
# The unit suite mocks the database and the API, so it needs nothing
# running and finishes in ~11 s (measured, macOS). The coverage gate is
# the only thing held back, and only because a floor is a merge concern
# rather than a pre-push one.
.PHONY: check
check: lint test
	@echo "==> check: green (the coverage gate is in 'make check-all')"

.PHONY: check-all
check-all: lint coverage
	@echo "==> check-all: green"

# setup: dependencies only. No pre-commit / pre-push hook is installed
# here — that is a later step of backend#1606.
.PHONY: setup
setup:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt
	$(PYTHON) -m pip install -e .
	$(PYTHON) -m pip install "ruff==$(RUFF_VERSION)"
	@echo "==> setup: dependencies installed; run 'make check'"

# ---- individual targets ------------------------------------------

# lint: the org code-quality gate's ruff job, using this repo's own
# ruff.toml (which is what the shared workflow does when one exists).
#
# NOT here: black. The caller does set `format: true`, but the shared
# gate scans the PR DIFF, and the tree as a whole is not black-clean
# (72 files at the time of writing). A whole-tree `black --check` in
# `check` would be red on a clean checkout, which is worse than not
# running it. Format the files you touch; `pre-commit` already does.
.PHONY: lint
lint:
	$(PYTHON) -m ruff check .

# test: tests.yml's suite without the coverage machinery.
#
# Scoped to tests/ ON PURPOSE, even though tests.yml invokes bare
# `pytest`. There is no `testpaths` setting, so a bare invocation also
# collects e2e/ — which in CI is harmless (no MySQL is listening, so
# e2e/conftest.py skips collection) and on a developer machine is not: a
# laptop with a MySQL up, or with DB_NAME/DB_USER exported, would have
# `make check` quietly run real ingestion that creates and drops tables
# in that database. A pre-push check must not touch anyone's data.
# (Bugbot, #461.) `make e2e` runs that suite deliberately.
.PHONY: test
test:
	$(PYTEST) tests/ -q

# coverage: tests.yml exactly — the 95% floor included.
.PHONY: coverage
coverage:
	$(PYTEST) tests/ --cov=tracebloc_ingestor --cov-report=term --cov-report=xml --cov-fail-under=95

# e2e: e2e.yml — real ingestion against a real MySQL with an in-process
# mock backend. Needs a database, hence the explicit target rather than
# a place in check-all.
.PHONY: e2e
e2e:
	$(PYTEST) e2e/ -v
