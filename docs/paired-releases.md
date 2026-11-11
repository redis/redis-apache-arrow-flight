# Paired public and Enterprise versions

Run **Coordinated release** in
`Redislabs-Solution-Architects/redis-apache-arrow-flight-enterprise` to publish one
version in both repositories. The private coordinator qualifies the exact public
commit from its composition manifest and its own main-branch commit, builds and
checks both editions, verifies both draft releases, and then publishes both.

Each release has the same `release-pair.json`: version, public/Enterprise commit
SHAs, per-edition artifact hashes and successful qualification binding. The
Enterprise SHA/run URL are intentionally recorded, while source and paid binaries
remain private. Existing matching public drafts (including `v0.2.0`) can be reused.
Tags never move and different artifact bytes are never overwritten. Publication
across repositories is sequential; a rerun repairs partial completion.

No PAT is required. A GitHub App installed only on this public repository grants
**Contents: read and write** to the private publication job. App credentials live
only in the private repository's `paired-release` environment. This public
repository needs only the Actions variable `RELEASE_COORDINATOR_APP_SLUG`, set
to that App's slug without `[bot]`. The private repository documents the exact
App setup and release/recovery procedure in `docs/paired-releases.md`.

After both GitHub releases publish, the App sends `qualified-release-pair` here.
**Qualified release image** runs on the public default branch, verifies the
published pair asset was uploaded by the configured App, checks the immutable tag
and qualification binding, runs the public tests, and publishes the public GHCR
image using this repository's own `GITHUB_TOKEN`. The image workflow must succeed
before announcing the release as fully shipped. To repair only an image failure,
run **Qualified release image** manually with the published version.

The legacy **Release artifact checks** workflow still validates PR binaries and
images, but rejects manual publication. Creating tags here is no longer the
release procedure. Merge the public image workflow and private coordinator
before first use. Public changes to Go sources require updating and qualifying
the private manifest's public commit before releasing the new version.
