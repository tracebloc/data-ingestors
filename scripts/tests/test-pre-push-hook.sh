#!/bin/sh
# test-pre-push-hook.sh — behaviour tests for `make install-hooks` and the
# generated pre-push hook, driven by running real `make` against a copy of the
# Makefile in a throwaway git repo. Added with the fleet-wide hook fix
# (backend#1749). Pure POSIX sh, no bats/pytest dependency, so `make test-hooks`
# runs it anywhere.
#
# Covers the branches a future edit could silently break:
#   * fresh install writes an executable, ours-marked hook
#   * re-install is idempotent (ours -> rewrite, no error)
#   * a foreign pre-push hook is left untouched
#   * core.hooksPath outside the repo is refused (no shared-dir stomp)
#   * core.hooksPath=.. is refused too — no pre-push written into the parent
#     (the dirname-of-hooksdir guard missed a bare `..`; backend#1749)
#   * the hook skips delete-only / no-op pushes (all-zero local sha)
#   * the hook degrades gracefully when `make` is off PATH (GUI clients)
#   * the hook soft-passes when the venv toolchain is absent (guard-toolchain
#     fails) rather than hard-blocking a GUI push that cannot --no-verify (#1749)
#   * guard-toolchain fails on a tool it cannot run and passes when it can
set -eu

# Hermetic: ignore the developer's global/system git config so an ambient
# core.hooksPath (corp dotfiles, husky) can't make install-hooks skip and fail
# the fresh-install/reinstall assertions. Every git here sees only local config.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null

REPO_ROOT=$(unset CDPATH; cd -- "$(dirname -- "$0")/../.." && pwd)
MAKEFILE="$REPO_ROOT/Makefile"
Z=0000000000000000000000000000000000000000
fails=0
check() { if [ "$1" = "$2" ]; then echo "  ok: $3"; else echo "  FAIL: $3 (got '$1', want '$2')"; fails=$((fails + 1)); fi; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"
git init -q .
git config user.email t@t; git config user.name t
cp "$MAKEFILE" ./Makefile
hook=.git/hooks/pre-push

# 1) fresh install
make -s install-hooks >/dev/null
check "$( [ -x "$hook" ] && echo yes )" "yes" "fresh install writes an executable hook"
check "$(grep -c 'tracebloc pre-push hook' "$hook")" "1" "hook carries the ours-marker"

# 2) idempotent re-install
make -s install-hooks >/dev/null
check "$(grep -c 'tracebloc pre-push hook' "$hook")" "1" "re-install stays idempotent"

# 3) foreign hook left untouched
printf '#!/bin/sh\necho FOREIGN\n' > "$hook"
make -s install-hooks >/dev/null
check "$(grep -c FOREIGN "$hook")" "1" "foreign hook is preserved"
rm -f "$hook"

# 4) core.hooksPath pointing OUTSIDE the repo is refused, in every escaping form
# (absolute, relative, and .. segments that string-prefix as inside). Assert the
# resolved shared dir stays empty — checking only .git/hooks would miss a hook
# written into the configured dir, and a string-prefix guard misses .. escapes.
# The parent of each form is a real, canonicalisable dir outside $work.
outside=$(mktemp -d)                       # a sibling of $work, definitely outside
for form in "$outside" "../$(basename "$outside")" "$work/../$(basename "$outside")"; do
  git config core.hooksPath "$form"
  make -s install-hooks >/dev/null
  check "$( [ -e "$outside/pre-push" ] && echo present || echo absent )" "absent" "core.hooksPath outside repo refused: $form"
  rm -f "$outside/pre-push"
  git config --unset core.hooksPath
done
rm -rf "$outside"

# 4b) core.hooksPath INSIDE the repo is honoured (install proceeds there).
mkdir -p "$work/.githooks"
git config core.hooksPath .githooks
make -s install-hooks >/dev/null
check "$(grep -c 'tracebloc pre-push hook' "$work/.githooks/pre-push" 2>/dev/null || echo 0)" "1" "core.hooksPath inside repo: hook installed there"
git config --unset core.hooksPath

# 4c) core.hooksPath escapes to a shared dir OUTSIDE the worktree, in the shapes
# earlier guards missed: a bare `..`/`./..` (the dirname guard resolved it back
# inside), and a `..` hidden behind a not-yet-created prefix — `missing/../..`,
# `missing/../../shared` — where the ancestor-walk climbed past the missing
# prefix and read the worktree as the target. Both wrote pre-push above the
# worktree, the stomp the guard exists to prevent (backend#1749). A `..` in the
# not-yet-existing suffix cannot be resolved until the prefix exists, so the
# guard refuses it (Lukas, backend#2714). Assert nothing lands outside the repo.
esc=$(mktemp -d)
mkdir -p "$esc/repo"
( cd "$esc/repo" && git init -q . && git config user.email t@t && git config user.name t \
  && cp "$MAKEFILE" ./Makefile )
for form in ".." "./.." "missing/../.." "missing/../../shared"; do
  ( cd "$esc/repo" && git config core.hooksPath "$form" && make -s install-hooks >/dev/null )
  check "$(find "$esc" -name pre-push -not -path "$esc/repo/*" 2>/dev/null | head -1 | grep -q . && echo present || echo absent)" "absent" "core.hooksPath parent-escape refused: $form"
  find "$esc" -name pre-push -not -path "$esc/repo/*" -delete 2>/dev/null
  ( cd "$esc/repo" && rm -rf missing shared )
done
rm -rf "$esc"

# 4d) a not-yet-created IN-REPO hooks dir still INSTALLS — the fresh-clone case
# the ancestor-walk exists for, and the arm the suffix-`..` refusal must not
# over-block (Lukas, backend#2714). No `..`, so it is unambiguously in-repo.
git config core.hooksPath freshhooks
make -s install-hooks >/dev/null
check "$(grep -c 'tracebloc pre-push hook' "$work/freshhooks/pre-push" 2>/dev/null || echo 0)" "1" "core.hooksPath in-repo but not-yet-created: hook installed"
git config --unset core.hooksPath
rm -rf "$work/freshhooks"

# reinstall a clean ours-hook for the behavioural cases
make -s install-hooks >/dev/null

# 5) delete-only push (all-zero local sha) is skipped without running make
if printf 'refs/heads/x %s refs/heads/x %s\n' "$Z" deadbeef | sh "$hook"; then rc=0; else rc=$?; fi
check "$rc" "0" "delete-only push is skipped (exit 0)"

# 6) real push but make absent -> graceful skip, not 'command not found'
if printf 'refs/heads/x deadbeef refs/heads/x 000\n' | env PATH= /bin/sh "$hook"; then rc=0; else rc=$?; fi
check "$rc" "0" "missing make degrades to skip (exit 0)"

# 7) a genuine (non-delete) push RUNS make check. Stub make to touch a sentinel
# only for `check` (the hook now runs `make guard-toolchain` first); a no-op hook
# would leave the sentinel missing.
sent="$(mktemp -u)"; stub="$(mktemp -d)"
printf '#!/bin/sh\n[ "$1" = check ] && : > %s\nexit 0\n' "$sent" > "$stub/make"; chmod +x "$stub/make"
printf 'refs/heads/x deadbeef refs/heads/x 000\n' | env PATH="$stub:$PATH" sh "$hook" >/dev/null 2>&1 || true
check "$([ -f "$sent" ] && echo ran || echo missing)" "ran" "genuine push runs make check"
rm -rf "$stub"; rm -f "$sent"

# 8) an incomplete venv toolchain (system make present, but ruff/pytest absent)
# must SOFT-PASS, not hard-block: GUI/IDE clients push on a thin PATH and cannot
# pass --no-verify (backend#1749). The hook asks `make guard-toolchain` and skips
# itself when it fails, so `make check` must not run. Stub make so guard-toolchain
# fails and check would touch a sentinel; assert exit 0 and the sentinel absent.
sent="$(mktemp -u)"; stub="$(mktemp -d)"
printf '#!/bin/sh\ncase "$1" in guard-toolchain) exit 1 ;; check) : > %s ;; esac\nexit 0\n' "$sent" > "$stub/make"; chmod +x "$stub/make"
if printf 'refs/heads/x deadbeef refs/heads/x 000\n' | env PATH="$stub:$PATH" sh "$hook"; then rc=0; else rc=$?; fi
check "$rc" "0" "missing venv toolchain soft-passes (exit 0)"
check "$([ -f "$sent" ] && echo ran || echo skipped)" "skipped" "soft-pass does not run make check"
rm -rf "$stub"; rm -f "$sent"

# 9) guard-toolchain itself: fails when a tool cannot run, passes when all can.
# Driven with a stub PYTHON so it needs no real venv and stays fast.
gtbin="$(mktemp -d)"
printf '#!/bin/sh\nexit 0\n' > "$gtbin/py-ok"; chmod +x "$gtbin/py-ok"
printf '#!/bin/sh\nexit 1\n' > "$gtbin/py-bad"; chmod +x "$gtbin/py-bad"
if make -s guard-toolchain PYTHON="$gtbin/py-ok" >/dev/null 2>&1; then rc=0; else rc=$?; fi
check "$rc" "0" "guard-toolchain passes when the toolchain runs"
if make -s guard-toolchain PYTHON="$gtbin/py-bad" >/dev/null 2>&1; then rc=0; else rc=$?; fi
check "$( [ "$rc" != 0 ] && echo nonzero || echo zero )" "nonzero" "guard-toolchain fails when a tool cannot run"
rm -rf "$gtbin"

# 9b) the installed hook runs against whatever Makefile the pushed branch has
# (it lives in .git/hooks, shared across branches). make exits 2 for a MISSING
# guard-toolchain target just as for a failed recipe, so the hook must not read
# "no such target" as "toolchain absent" and skip — it probes with -n and falls
# through to make check (Bugbot, backend#1749). Stub make as an OLD Makefile:
# guard-toolchain has no rule (exit 2), check touches a sentinel.
sent="$(mktemp -u)"; stub="$(mktemp -d)"
printf '#!/bin/sh\nfor a in "$@"; do case "$a" in guard-toolchain) exit 2 ;; check) : > %s ;; esac; done\nexit 0\n' "$sent" > "$stub/make"; chmod +x "$stub/make"
printf 'refs/heads/x deadbeef refs/heads/x 000\n' | env PATH="$stub:$PATH" sh "$hook" >/dev/null 2>&1 || true
check "$([ -f "$sent" ] && echo ran || echo skipped)" "ran" "old Makefile (no guard-toolchain) still runs make check"
rm -rf "$stub"; rm -f "$sent"

# 9c) an even older branch may lack `check` itself, or the Makefile entirely.
# `make -n guard-toolchain` fails there too, so control would reach `exec make
# check` and hard-block on "No rule to make target check" — with no --no-verify
# in the GUI clients this family protects. The hook probes `check` too and
# soft-passes (Lukas, backend#2714). Stub make: -n check has no rule (exit 2).
sent="$(mktemp -u)"; stub="$(mktemp -d)"
printf '#!/bin/sh\nfor a in "$@"; do case "$a" in check) exit 2 ;; esac; done\n: > %s\nexit 0\n' "$sent" > "$stub/make"; chmod +x "$stub/make"
if printf 'refs/heads/x deadbeef refs/heads/x 000\n' | env PATH="$stub:$PATH" sh "$hook"; then rc=0; else rc=$?; fi
check "$rc" "0" "missing check target soft-passes (exit 0, no hard-block)"
check "$([ -f "$sent" ] && echo ran || echo skipped)" "skipped" "soft-pass does not run make check"
rm -rf "$stub"; rm -f "$sent"

echo "--- pre-push hook tests: $( [ "$fails" -eq 0 ] && echo ALL GREEN || echo "$fails FAILED" ) ---"
[ "$fails" -eq 0 ]
