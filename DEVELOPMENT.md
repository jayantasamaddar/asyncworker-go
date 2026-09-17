## Development

This module has zero dependencies beyond the Go standard library, and is intentionally **not** a member a `go.work` workspace — it is meant to be usable, unmodified, by any Go application, in or out of this repository.

Do not add imports from elsewhere in this repo, and do not add it to `go.work`.

## Releasing

Versioning and the CHANGELOG are managed by [Changesets](https://github.com/changesets/changesets),
tracked against the `version` field in the root `package.json` — that file is a
shim (never published to npm) that exists only so Changesets has something to
bump; the release workflow reads its version to cut the git tag.

1. In your PR, run `npx changeset` and follow the prompts (bump level + a
   one-line summary). Commit the generated `.changeset/*.md` file with the PR.
   A PR that doesn't touch release-worthy code can skip this — the "Require
   changeset" check only fires on `.go` changes.
2. On merge to `main`, `.github/workflows/release.yml` consumes every pending
   changeset via `changeset version` (bumping `package.json` and writing
   `CHANGELOG.md`), commits that bump back to `main`, tags `v<version>`, then
   builds/vets/tests the tagged commit and publishes a GitHub Release with
   notes pulled from the new `CHANGELOG.md` section. No npm/Go registry
   publish step is needed — pushing the tag is already enough for
   pkg.go.dev to pick up the new module version.
3. Every step is idempotent, so the same workflow can also be run by hand
   (Actions → Release → Run workflow) with no input — useful when there's
   nothing new to push and retrigger it, e.g. recovering from a partial
   failure. It resolves to whatever's already true in git/GitHub: a
   version bump only happens if changesets are still pending, a tag only
   gets (re-)created if it doesn't already exist, and a release only gets
   (re-)published if one doesn't already exist for that tag.
