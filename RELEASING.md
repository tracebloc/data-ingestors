# Releasing data-ingestors

How to cut a new release of `tracebloc-ingestor` (PyPI package) and `ghcr.io/tracebloc/ingestor` (signed image). Aimed at maintainers; assumes you have push access and a `gh` token with `repo` + `workflow` + `packages` scopes.

## What publishes from what

| Trigger | Workflow | Artifact |
|---|---|---|
| Push to `develop` | `.github/workflows/publish-dev.yml` | sdist + wheel → GitHub Packages (dev pre-release) |
| Push to `master` | `.github/workflows/publish-master.yml` | sdist + wheel → PyPI / GitHub Packages |
| Push of tag `v*.*.*` (any branch) | `.github/workflows/release-image.yml` | Signed multi-tag image at `ghcr.io/tracebloc/ingestor`, plus a GitHub Release |

Each publisher runs the schema-load smoke probe (`tracebloc_ingestor.cli.run._load_schema()`) before publishing. If the bundled `schema/ingest.v1.json` ever goes missing from the artifact (the v0.3.0-rc1 bug), the workflow aborts and nothing ships.

The image and the PyPI package release **independently**. A tag without a `setup.py` bump produces an image whose pip-reported version lags. A `master` merge without a tag produces a PyPI release with no image. Doing the bump *inside* the sync PR (step 2) keeps them aligned.

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

## 2. Bump the version in `setup.py`

Pick the next SemVer per [semver.org](https://semver.org/) — patch for fixes, minor for backwards-compatible features, major for breaking changes. Export it once so the rest of this doc copy-pastes cleanly:

```bash
export VERSION=X.Y.Z   # e.g. 0.3.1
git checkout -b release/v${VERSION} origin/develop
# Edit setup.py: version="X.Y.Z"
git diff setup.py     # confirm only the version string changed
git add setup.py
git commit -m "chore(release): bump version to ${VERSION}"
git push -u origin release/v${VERSION}

gh pr create --base develop \
  --title "chore(release): bump version to ${VERSION}" \
  --body "Version bump ahead of v${VERSION} release. Companion sync PR will follow."
```

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

## 5. Tag the release

After the master publish is green:

```bash
git checkout master && git pull --ff-only
git tag -a v${VERSION} -m "v${VERSION}"
git push origin v${VERSION}
```

That tag push triggers `release-image.yml`, which:

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

## 7. Pin the new digest downstream

The Helm subchart in [`tracebloc/client`](https://github.com/tracebloc/client) reads the digest out of these release notes and bakes it into its values. Open a follow-up PR there pinning to the new digest. (Pull by digest, not tag — that's the whole point of signing.)

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
- **Version bump lives in the sync flow**, not as a tag-time afterthought, so the PyPI version and image-baked version don't drift.
- **The image entrypoint requires `MYSQL_HOST`** at runtime (see `docker-entrypoint.sh`). Smoke probes bypass it with `--entrypoint`; if you're sanity-checking by hand outside CI, you'll need the same flag.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `publish-master.yml` smoke step fails with `FileNotFoundError: schema/ingest.v1.json` | A packaging regression dropped `schema/` from the sdist. | Verify `tracebloc_ingestor/schema/__init__.py` exists and `MANIFEST.in` has the `recursive-include tracebloc_ingestor/schema *.json` line. |
| `release-image.yml` smoke step fails with `INGEST_CONFIG` not in output | `tracebloc-ingest` console script not installed (broken `entry_points` in setup.py) or `main()` raises at import time. | Run the second smoke probe locally against the digest with `--entrypoint tracebloc-ingest`. |
| Image workflow ran but no image was pushed | `docker/metadata-action` produced zero tags (the `inputs.ref` / `github.ref` mismatch class of bug). | Re-run via `gh workflow run release-image.yml -f ref=v${VERSION}`. The `Verify tags were produced` step exists exactly to catch this and fail loudly. |
| Cosign verify fails after a successful release | OIDC certificate identity changed (someone moved the workflow file). | Update the `--certificate-identity-regexp` to match the new path. |
