## Summary
<!-- 1–3 sentences. What does this PR do and why? -->

## Related
<!-- Same repo: Closes #123 · Cross-repo: Fixes tracebloc/backend#456 (owner-qualified — a bare backend#456 closes nothing). develop IS this repo's default branch, so a same-repo keyword DOES fire on merge — measured 12 for 12. -->

## Type of change
- [ ] Feature
- [ ] Bug fix
- [ ] Tech-debt / refactor
- [ ] Docs
- [ ] Security / hardening
- [ ] Breaking change

## Test plan
<!-- What did you test? `make check` (ruff + `pytest tests/`) is the local gate. Which extra commands or manual steps? -->

## Screenshots / recordings
<!-- For UI changes. Remove if N/A. -->

## Deployment notes
<!-- Env vars, migrations, rollout order, feature flags. Remove if N/A. -->

## Checklist
- [ ] `make check` passes locally (ruff + `pytest tests/`)
- [ ] Tests added / updated for this change
- [ ] Docs updated if behavior or config changed
- [ ] No secrets / credentials in the diff
- [ ] For security-sensitive paths: appropriate reviewer requested
- [ ] Cross-repo issues use `Fixes tracebloc/<repo>#N` — a bare `repo#N` closes nothing
- [ ] If this depends on a change in another repo: shipped **expand-then-contract** (additive first, consumers adopt later), or **Breaking change** ticked above with the rollout order in *Deployment notes* — repos promote independently, so the other change may not ship with this one
- [ ] If this touches published paths (`tracebloc_ingestor/*`): version bumped in `tracebloc_ingestor/__init__.py` — the **Version bump gate** hard-fails an already-released version
