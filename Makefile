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
	@echo "  install-hooks  (re)install the git pre-push hook that runs 'make check'"
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
# guard-toolchain: can this shell actually run `make check`? A GUI/IDE git client
# (VS Code, Tower, GitKraken) launches the pre-push hook on a thin PATH that has
# system `make` and often a bare `python3`, but NOT the virtualenv these checks
# were installed into — so `make check` hard-fails on "No module named ruff" with
# no --no-verify to escape (backend#1749; the make-vs-toolchain shape of
# backend#1995). `check` depends on this and the hook runs it first, so a push
# from such a client degrades to a skip instead of a hard block.
#
# It asks the TOOLS, not the DEPENDENCIES: whether $(PYTHON) can run the linters
# and test runner check invokes, not whether every package is installed. A
# genuinely broken venv still fails `make check` when run from a real shell.
.PHONY: guard-toolchain
guard-toolchain:
	@command -v "$(PYTHON)" >/dev/null 2>&1 || { \
	  echo "not on PATH: $(PYTHON) — activate your virtualenv, then run: make setup"; \
	  exit 1; }
	@missing=''; \
	for m in ruff pytest; do \
	  "$(PYTHON)" -m "$$m" --version >/dev/null 2>&1 || missing="$$missing $$m"; \
	done; \
	[ -z "$$missing" ] || { \
	  echo "$(PYTHON) cannot run:$$missing — activate your virtualenv, then run: make setup"; \
	  exit 1; }

.PHONY: check
check: guard-toolchain lint test
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
	@$(MAKE) --no-print-directory install-hooks

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

# coverage: tests.yml's pytest job exactly — the 95% floor and the HTML report
# it uploads as an artifact.
#
# "Exactly" is now STRUCTURAL: tests.yml calls this target (backend#1606). It
# claimed "exactly" before and was not — the workflow invoked a bare `pytest`
# and added --cov-report=html, so the two differed in both scope and output
# while a comment here asserted they did not.
#
# THE tests/ SCOPE NOW APPLIES TO CI TOO, and that is the right direction of
# travel rather than a concession. Scoping is the deliberate decision from
# `test` above (Bugbot, #461): a bare `pytest` also collects e2e/, which on a
# developer machine with a MySQL up would run real ingestion that creates and
# drops tables. The Makefile must not widen to match CI.
#
# Narrowing CI loses nothing measurable: e2e/ skips itself in that job for want
# of a MySQL, and the e2e suite has its own REQUIRED check (`e2e (real MySQL)`,
# e2e.yml) that runs it against a real database. A unit-coverage job measuring
# a suite that is skipped by construction was never the intent.
.PHONY: coverage
coverage:
	$(PYTEST) tests/ --cov=tracebloc_ingestor --cov-report=term --cov-report=xml \
	  --cov-report=html --cov-fail-under=95

# e2e: e2e.yml — real ingestion against a real MySQL with an in-process
# mock backend. Needs a database, hence the explicit target rather than
# a place in check-all.
.PHONY: e2e
e2e:
	$(PYTEST) e2e/ -v

# install-hooks: put a pre-push hook in place that runs `make check`, so the
# canon's "run the tests before you push" is carried by the tooling rather than
# by memory. Factored out of `setup` so it is independently runnable and
# testable, and so a contributor who only wants the hook need not rerun the
# full `make setup`.
#
# Honest by design: the hook catches FORGETTING, not defiance — `git push
# --no-verify` skips it and always will. And it refuses to clobber a pre-push
# hook that is already there and not ours (e.g. one the pre-commit framework
# manages), rather than silently stomping a contributor's setup.
#
# `git rev-parse --git-path hooks` (not a hard-coded `.git/hooks`) so it lands
# in the right place inside a linked worktree or a submodule, where the git dir
# is not `.git`.
# test-hooks: run the install-hooks / pre-push-hook behaviour suite on demand.
# Not a `check` dependency: it runs real `make` in throwaway repos, so an
# environment quirk (old git, noexec /tmp) could block a local push on something
# CI never sees. Run it directly with `make test-hooks` (backend#1749).
.PHONY: test-hooks
test-hooks:
	@sh scripts/tests/test-pre-push-hook.sh

.PHONY: install-hooks
install-hooks:
	@if ! git rev-parse --git-dir >/dev/null 2>&1; then \
	  echo "note: not a git checkout — skipping pre-push hook install"; \
	elif hp="$$(git config --get core.hooksPath 2>/dev/null || true)"; [ -n "$$hp" ] && { \
	       hd="$$(git rev-parse --git-path hooks)"; \
	       case "$$hd" in /*) hdd="$$hd";; *) hdd="$$PWD/$$hd";; esac; \
	       hdx="$$hdd"; \
	       while [ ! -d "$$hdx" ] && [ "$$hdx" != "$$(dirname "$$hdx")" ]; do \
	         hdx="$$(dirname "$$hdx")"; \
	       done; \
	       chd="$$(cd "$$hdx" 2>/dev/null && pwd -P || true)"; \
	       ctop="$$(cd "$$(git rev-parse --show-toplevel)" && pwd -P)"; \
	       cgd="$$(cd "$$(git rev-parse --git-common-dir)" 2>/dev/null && pwd -P || true)"; \
	       inr=0; \
	       case "$$chd/" in "$$ctop/"*) inr=1;; esac; \
	       if [ -n "$$cgd" ]; then case "$$chd/" in "$$cgd/"*) inr=1;; esac; fi; \
	       [ -z "$$chd" ] || [ "$$inr" = 0 ]; \
	     }; then \
	  echo "note: core.hooksPath is set to '$$hp' (resolves to '$$chd'), outside this repo — skipping."; \
	  echo "      That is a shared hooks dir; installing here would run 'make check' from every repo you push."; \
	  echo "      Add 'make check' to that hook by hand if you want it everywhere."; \
	else \
	  hook="$$(git rev-parse --git-path hooks)/pre-push"; \
	  if [ -e "$$hook" ] && ! grep -q 'tracebloc pre-push hook' "$$hook" 2>/dev/null; then \
	    echo "note: $$hook already exists and is not ours — leaving it untouched."; \
	    echo "      add 'make check' to it, or remove it and re-run 'make install-hooks'."; \
	  else \
	    mkdir -p "$$(dirname "$$hook")" && \
	    printf '%s\n' \
	      '#!/bin/sh' \
	      '# tracebloc pre-push hook installed by make setup (backend#1606).' \
	      '# Runs make check so a push that would be red in CI is caught locally first.' \
	      '# It catches forgetting, not defiance: git push --no-verify skips it.' \
	      '#' \
	      '# Nothing to check on a delete/no-op push: a branch delete streams a' \
	      '# local sha of all-zeros on stdin (no new commits). Skip so a red tree' \
	      '# cannot block "git push --delete", and cleanup pushes stay free.' \
	      'z=0000000000000000000000000000000000000000' \
	      'had_update=0' \
	      'while read -r _ local_sha _ _; do' \
	      '  [ "$$local_sha" != "$$z" ] && had_update=1' \
	      'done' \
	      '[ "$$had_update" = 0 ] && exit 0' \
	      '#' \
	      '# Degrade gracefully when the toolchain is absent: GUI/IDE git clients' \
	      '# (Tower, GitKraken, VS Code) launch hooks with a minimal PATH, so make' \
	      '# may be missing — and several do not expose --no-verify. Skipping beats' \
	      '# hard-blocking every push with "make: command not found".' \
	      'command -v make >/dev/null 2>&1 || exit 0' \
	      '#' \
	      '# Git exports GIT_DIR/GIT_WORK_TREE/etc into hook processes; a nested git' \
	      '# invocation (from a test, tool, or setuptools-scm) then fails in a linked' \
	      '# worktree with exit status 128. Clear them so the make runs below behave' \
	      '# as they do from an ordinary shell.' \
	      'unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_PREFIX GIT_COMMON_DIR GIT_OBJECT_DIRECTORY' \
	      '#' \
	      '# Guarding on make alone was not enough (backend#1749): a GUI/IDE client' \
	      '# commonly has system make on a thin PATH but not the virtualenv these' \
	      '# checks run in, so the skip above passed and the push then hard-failed on' \
	      '# "No module named ruff" — the exact outcome the skip exists to prevent,' \
	      '# with no --no-verify to escape. The hook delegates to guard-toolchain,' \
	      '# the single place the tool list lives, rather than restating it here — so' \
	      '# the hook cannot drift from it. guard-toolchain probes the TOOLS check' \
	      '# runs, not installed packages (a broken venv still fails make check in a shell).' \
	      'make guard-toolchain >/dev/null 2>&1 || exit 0' \
	      'exec make check' > "$$hook" && \
	    chmod +x "$$hook" && \
	    echo "==> pre-push hook installed at $$hook" && \
	    echo "    'make check' now runs before each push (skip once with: git push --no-verify)"; \
	  fi; \
	fi
