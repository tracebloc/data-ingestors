# Releasing data-ingestors

How to cut a new release of `tracebloc-ingestor` (PyPI package) and `ghcr.io/tracebloc/ingestor` (signed image). Aimed at maintainers; assumes you have push access and a `gh` token with `repo` + `workflow` + `packages` scopes.

## What publishes from what

| Trigger | Workflow | Artifact |
|---|---|---|
| Push to `develop` | `.github/workflows/publish-dev.yml` | sdist + wheel → GitHub Packages (dev pre-release) |
| Push to `master` | `.github/workflows/publish-master.yml` | sdist + wheel → PyPI / GitHub Packages |
| Push of tag `v*.*.*` (any branch) | `.github/workflows/release-image.yml` | Signed multi-tag image at `ghcr.io/tracebloc/ingestor`, plus a GitHub Release |

Each publisher runs the schema-load smoke probe (`tracebloc_ingestor.cli.run._load_schema()`) before publishing. If the bundled `schema/ingest.v1.json` ever goes missing from the artifact (the v0.3.0-rc1 bug), the workflow aborts and nothing ships.

The PyPI package can release without the image, but **not the other way round**: `release-image.yml` opens with a `verify-published` gate that polls PyPI for the exact version in the tag and fails closed if it never appears, so an image is only ever cut for a version that actually published. A `master` merge without a tag still produces a PyPI release with no image. Doing the bump *inside* the sync PR (step 2) keeps them aligned.

Note the gate proves the version *string* is on PyPI, not that the tagged commit is the one that uploaded it. On the release-train path those are the same commit; a hand-cut tag on some other commit would still clear the gate.

## Pre-flight

Make sure you're not above the WIP limit on the [kanban](https://github.com/orgs/tracebloc/projects/2/views/1), then:

```bash
# Always release from a clean tree.
cd /Volumes/VPPD/projects/tracebloc/data-ingestors
git fetch origin
git checkout develop && git pull --ff-only

# Sanity-check what's about to ship.
git log --oneline origin/master..origin/develop
```

If that log is empty, there's nothing to release — stop here.

## 1. Verify the dev publish is green

The last commit on `develop` should already have a successful `Publish Dev Package` run. If not, something in the dev pipeline is broken and you should fix that before pinning a release version.

```bash
gh run list --repo tracebloc/data-ingestors \
  --workflow publish-dev.yml --branch develop --limit 5
```

## 2. Bump the version in `tracebloc_ingestor/__init__.py`

The version is **single-sourced** in `tracebloc_ingestor/__init__.py` (`__version__`); `setup.py` parses that literal at build time, so this is the only file you edit and the two can no longer drift. (They drifted once — `setup.py` 0.3.5 vs `__version__` 0.3.4, #171/#175 — because the bump touched only one file; single-sourcing is the fix.)

Pick the next SemVer per [semver.org](https://semver.org/) — patch for fixes, minor for backwards-compatible features, major for breaking changes. Export it once so the rest of this doc copy-pastes cleanly:

```bash
export VERSION=X.Y.Z   # e.g. 0.3.1
git checkout -b release/v${VERSION} origin/develop
# Edit tracebloc_ingestor/__init__.py: __version__ = "X.Y.Z"   (do NOT touch setup.py)
git diff tracebloc_ingestor/__init__.py   # confirm only the version string changed
python setup.py --version                 # must print X.Y.Z — proves setup.py picked it up
git add tracebloc_ingestor/__init__.py
git commit -m "chore(release): bump version to ${VERSION}"
git push -u origin release/v${VERSION}

gh pr create --base develop \
  --title "chore(release): bump version to ${VERSION}" \
  --body "Version bump ahead of v${VERSION} release. Companion sync PR will follow."
```

On the release ticket, this collapses the old two-line acceptance item (`setup.py` **and** `__init__.py` bumped) into a single check — **`__version__` bumped in `tracebloc_ingestor/__init__.py`** (`setup.py` derives it; verified by `tests/test_version_single_source.py`).

Get it reviewed and merged into `develop` like any other PR.

## 3. Open the develop → master sync PR

Branch convention is `sync/develop-to-master-v${VERSION}` (see #109 for prior art). **This is the only kind of PR that targets `master` directly.**

```bash
git fetch origin
git checkout -b sync/develop-to-master-v${VERSION} origin/develop

gh pr create --base master --head sync/develop-to-master-v${VERSION} \
  --title "Sync develop → master for v${VERSION} release" \
  --body "Promotes \`develop\` to \`master\` for the v${VERSION} release. CI on merge will publish to PyPI; the v${VERSION} tag (created after merge) will trigger the signed image build."
```

You may need to push the branch first if `gh pr create` complains:

```bash
git push -u origin sync/develop-to-master-v${VERSION}
```

## 4. Merge the sync PR

Merge as a **merge commit** (not squash) so `master` keeps the develop history. Once merged, `publish-master.yml` fires automatically:

```bash
# Tail the run while it goes.
gh run watch --repo tracebloc/data-ingestors \
  $(gh run list --repo tracebloc/data-ingestors \
      --workflow publish-master.yml --branch master --limit 1 \
      --json databaseId --jq '.[0].databaseId')
```

If the smoke probe inside that workflow fails, the package will not be uploaded. Fix the regression and reopen a new sync PR — do **not** force the upload.

## 5. Tag the release (automatic — the release train does it)

**This step now happens on its own, from outside this repo.** The [release train](https://github.com/tracebloc/release-train) reads the single-sourced `__version__` on the prod hop and pushes `vX.Y.Z` on the commit it just merged. That tag push starts `release-image.yml`, which waits for the version on PyPI and then builds the signed image + GitHub Release. So a normal release needs no manual tag: merge the sync PR, watch the master publish go green, and the image follows.

The tag is cut by the train's token (which acts as Lukas), **not** by `github-actions[bot]`. That is deliberate — this repo's `Protect v* release tags` ruleset does not let the bot bypass tag creation, so the previous repo-local `auto-release-on-master.yml` would have failed on the first real release; it was retired in favour of the train (tracebloc/backend#1345). A bot-pushed tag also would not have started `release-image.yml` at all, whereas the train's does.

If the version already has a tag the train skips it, and it refuses to go backwards (a version file below the latest released tag is a hard stop).

**Manual fallback.** If the train is unavailable, or you're re-cutting an image for an existing tag, tag by hand after the master publish is green — from an account in `release-managers`, since the ruleset blocks everyone else:

```bash
git checkout master && git pull --ff-only
git tag -a v${VERSION} -m "v${VERSION}"
git push origin v${VERSION}
```

> **Don't tag from `develop`.** The tag must sit on the master commit that PyPI published, or the signed image's pip-reported version leads the PyPI package until the sync lands (v0.7.6–v0.7.8 were tagged off develop this way). The automation always tags the published master commit; hand-tagging should too.

Either path triggers `release-image.yml`, which:

1. builds the image,
2. runs the digest-level smoke probes (`_load_schema()` and a bare `tracebloc-ingest` invocation via `--entrypoint`),
3. cosign-signs the digest keyless via OIDC,
4. attaches SBOM + SLSA provenance,
5. creates the GitHub Release at `v${VERSION}` with the digest and verify command in the notes.

Tail it:

```bash
gh run watch --repo tracebloc/data-ingestors \
  $(gh run list --repo tracebloc/data-ingestors \
      --workflow release-image.yml --limit 1 \
      --json databaseId --jq '.[0].databaseId')
```

## 6. Verify the published image

The release notes embed the exact verify command. Reproduce it locally:

```bash
# Pull the digest the workflow published.
DIGEST=$(gh release view v${VERSION} --repo tracebloc/data-ingestors \
  --json body --jq '.body' | grep -oE 'sha256:[a-f0-9]{64}' | head -1)
IMAGE=ghcr.io/tracebloc/ingestor

# Cosign keyless verify.
cosign verify ${IMAGE}@${DIGEST} \
  --certificate-identity-regexp 'https://github.com/tracebloc/data-ingestors/.github/workflows/release-image.yml@.*' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com'

# Optional: inspect SBOM / provenance.
docker buildx imagetools inspect ${IMAGE}@${DIGEST} --format '{{ json .SBOM }}'
docker buildx imagetools inspect ${IMAGE}@${DIGEST} --format '{{ json .Provenance }}'

# Optional: re-run the smoke probes locally.
docker run --rm --entrypoint python ${IMAGE}@${DIGEST} \
  -c "from tracebloc_ingestor.cli.run import _load_schema; print(_load_schema()['title'])"
```

## 7. Confirm the rollout (existing installs roll themselves forward)

**Existing installs pick up the new image with no chart change and no `helm upgrade` — this step only *confirms* that from the registry.** jobs-manager spawns each ingestion run as a short-lived Job from the floating tag `images.ingestor.tag` (`"0.3"`) with `imagePullPolicy: Always`, exactly the way training pods are spawned. The kubelet re-resolves the tag's current digest at **every job spawn** (a cheap registry manifest check — already-present layers are reused), so once the step-5 release moves `:0.3` to the new digest, the next ingestion run on any install runs it. Nothing rewrites a Deployment env, and there is no convergence window to wait on. (Authoritative sources: the `images.ingestor` comment block in [`client/values.yaml`](https://github.com/tracebloc/client/blob/develop/client/values.yaml), the `INGESTOR_IMAGE_*` env wiring in `client/templates/jobs-manager-deployment.yaml`, and `submit_ingestion_run._build_image_reference` in client-runtime — the mechanism landed in `client-runtime#40` / `client#125`.)

> **Corrects an earlier version of this doc.** Older text here described an image-refresh CronJob that polled `:0.3` and rewrote an `INGESTOR_IMAGE_DIGEST` env on the `*-jobs-manager` Deployment (via `kubectl set env`) within ~15 min. That "class-2" pass was **retired**: a `helm upgrade` that reset the env could revert the ingestor to a stale baseline — a regression a customer hit (a newer ingestor's fix disappeared on upgrade and resurfaced as a "non-numeric" validation error on data the new image handled correctly). The `INGESTOR_IMAGE_DIGEST` env on the jobs-manager Deployment is now **empty/unused by default**; spawning by floating tag is revert-proof and needs no env reconcile. `INGESTOR_IMAGE_DIGEST` survives only as an *opt-in* cluster-wide pin (see step 8).

Because the rollout signal now lives entirely in the registry, you can verify it **without cluster access** — confirm the floating `:0.3` tag resolves to the digest you just released:

```bash
IMAGE=ghcr.io/tracebloc/ingestor

# The digest the release published (the same sha256 surfaced in step 6).
RELEASE_DIGEST=$(gh release view v${VERSION} --repo tracebloc/data-ingestors \
  --json body --jq '.body' | grep -oE 'sha256:[a-f0-9]{64}' | head -1)

# What :0.3 currently points at — the multi-arch index digest.
TAG_DIGEST=$(docker buildx imagetools inspect ${IMAGE}:0.3 --format '{{ .Manifest.Digest }}')
# (equivalent one-liner if you have crane:  crane digest ${IMAGE}:0.3 )

[ "$TAG_DIGEST" = "$RELEASE_DIGEST" ] \
  && echo "OK        :0.3 -> ${RELEASE_DIGEST}; every new ingestion Job runs v${VERSION}" \
  || echo "MISMATCH  :0.3 is ${TAG_DIGEST}, release is ${RELEASE_DIGEST} (floating tag did not move)"
```

`release-image.yml` pushes `:X.Y.Z`, `:X.Y`, and `:X` to the same index digest, so a `v0.3.z` release moves `:0.3` (and `:0`) on its own — a match here means the whole fleet lands on the new image at its next ingestion-Job spawn. (Air-gapped installs that mirror `:0.3` into a private registry roll forward when *their* mirror syncs the tag; the check above confirms the upstream ghcr source that the release controls.)

## 8. (Optional) Bump the greenfield baseline in `tracebloc/client`

The pinned `images.ingestor.digest` in the chart only sets the **greenfield baseline** — the digest a brand-new install lands on for its first ingestion run, before it would otherwise resolve the floating `:0.3` tag. It does **not** drive rollout to existing installs (step 7 covers those). Bumping it is a deliberate, optional chart release, not a release step. Do it when you want fresh installs to start on the version you just shipped:

- Bump `images.ingestor.digest` to the new digest **and** `client/Chart.yaml`'s `version` + `appVersion` in lockstep (the chart's own SemVer, independent of the ingestor's).
- Open the PR against **`develop`**, like any other client change — this is not a `master`-targeting release PR.
- Precedent: [`client#161`](https://github.com/tracebloc/client/pull/161) (baseline v0.3.0 → v0.3.1) and [`client#185`](https://github.com/tracebloc/client/pull/185) (v0.3.1 → v0.3.2, tracking [#184](https://github.com/tracebloc/client/issues/184)).

(When you do pin, pull by digest, not tag — that's the whole point of signing — and the digest must be the multi-arch index, not a per-arch one, or arm64 nodes `ImagePullBackOff`.)

## Manual fallback (workflow_dispatch)

If the tag push didn't trigger the image workflow, or the workflow failed for an infrastructure reason and you want to retry against the same code:

```bash
gh workflow run release-image.yml --repo tracebloc/data-ingestors \
  -f ref=v${VERSION}
```

`inputs.ref` must point at an existing tag — the workflow reads it through `docker/metadata-action` to produce the `X.Y.Z`, `X.Y`, `X` tag set. Don't pass a branch.

## Conventions and gotchas

- **No `:latest`** is published (deliberate, see #45). Consumers pin to a major, minor, or specific patch.
- **No CHANGELOG.md** in the repo — release notes are auto-generated from the digest. If you want a human-written summary, edit it in afterwards: `gh release edit v${VERSION} --notes-file <(...)`.
- **PR base is always `develop`** for normal work. The `sync/develop-to-master-vX.Y.Z` PR is the only one targeting `master`.
- **Version is single-sourced** in `tracebloc_ingestor/__init__.py`; `setup.py` parses that literal, so bump the one file and never hardcode a version back into `setup.py` (that re-introduces the 0.3.4/0.3.5 drift, #175 — guarded by `tests/test_version_single_source.py`). The bump lives in the sync flow, not as a tag-time afterthought, so the PyPI version and image-baked version stay aligned.
- **The image entrypoint requires `MYSQL_HOST`** at runtime (see `docker-entrypoint.sh`). Smoke probes bypass it with `--entrypoint`; if you're sanity-checking by hand outside CI, you'll need the same flag.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `publish-master.yml` smoke step fails with `FileNotFoundError: schema/ingest.v1.json` | A packaging regression dropped `schema/` from the sdist. | Verify `tracebloc_ingestor/schema/__init__.py` exists and `MANIFEST.in` has the `recursive-include tracebloc_ingestor/schema *.json` line. |
| `release-image.yml` smoke step fails with `INGEST_CONFIG` not in output | `tracebloc-ingest` console script not installed (broken `entry_points` in setup.py) or `main()` raises at import time. | Run the second smoke probe locally against the digest with `--entrypoint tracebloc-ingest`. |
| Image workflow ran but no image was pushed | `docker/metadata-action` produced zero tags (the `inputs.ref` / `github.ref` mismatch class of bug). | Re-run via `gh workflow run release-image.yml -f ref=v${VERSION}`. The `Verify tags were produced` step exists exactly to catch this and fail loudly. |
| Cosign verify fails after a successful release | OIDC certificate identity changed (someone moved the workflow file). | Update the `--certificate-identity-regexp` to match the new path. |
| `release-image.yml` sits in `verify-published` for minutes, then fails with `never appeared on PyPI` | The version in the tag was never uploaded — usually `publish-master.yml` failed (check its run for this commit), or the tag was cut on a commit whose version differs from what published. | Fix the publish, then re-drive with `gh workflow run release-image.yml -f ref=v${VERSION}`. Do **not** bypass the gate: an image for an unpublished version is exactly what it exists to prevent. |
| A tag exists but no image or Release was ever produced | The `release-image.yml` run for that tag failed or was cancelled. Nothing re-drives it automatically — `push: tags` cannot re-fire for an existing ref, and the ruleset blocks delete-and-re-push. | Re-drive it by hand: `gh workflow run release-image.yml -f ref=v${VERSION}`. It's safe to repeat — the run edits or creates the Release and re-signs the same digest. |
