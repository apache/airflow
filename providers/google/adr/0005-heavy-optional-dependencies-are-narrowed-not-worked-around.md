<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 5. Heavy transitive dependencies are narrowed or dropped, not worked around

Date: 2026-07-20

## Status

Accepted

## Context

This provider has the largest dependency surface in the repo: ~50 `google-cloud-*`
distributions plus `google-api-python-client`, `pandas-gbq`, `pyarrow`,
`looker-sdk`, `apache-beam` and — via `google-cloud-aiplatform[evaluation]` — Ray.
Those last are the problem children: large, deep trees, and routinely the last in
the monorepo to support a new Python version or CPU architecture.

Because Airflow resolves a single shared lock across the workspace, a dependency
that cannot resolve here blocks the repo's Python-version rollout, unrelated
providers' bumps, and everyone's CI image builds. Hence `pyproject.toml` already
shards pins by `python_version`, several annotated with the upstream issue URL that
retires the shard. The tempting fix each time is local — vendor a wheel, add a
pre-release index, patch the import, or pin around it — but each makes the build
diverge from PyPI and none retires when upstream fixes the problem. The project
repeatedly reaches the opposite: narrow the declared range so the unsupported
combination is not claimed. When Ray had no wheel for a new Python on ARM, review
disabled Ray for that Python version until a stable release existed rather than ship
a build differing from the published one.

## Decision

A heavy or transitive dependency that cannot support a platform is excluded for
that platform. It is not vendored, patched, or resolved around.

- **Narrow the declared range rather than working around the gap.** Express the
  limit as a `python_version` (or platform) marker in
  `providers/google/pyproject.toml`, so the unsupported combination is never
  claimed in the first place.
- **Never vendor or side-load a wheel, index, or pre-release** to make a
  dependency resolve. The provider must install from PyPI the same way a user's
  environment does.
- **Every marker-sharded or exclusion pin carries a comment** naming the
  upstream issue — by full URL — and the condition under which the shard is
  removed. The Python-3.14 `pydantic` shard in
  `providers/google/pyproject.toml` is the model: it names the Ray
  incompatibility, links `ray-project/ray#62664`, and thereby says what has to
  happen for the shard to go. Not every pin in that file meets the bar yet — the
  `looker-sdk!=24.18.0` exclusion carries the upstream URL but no removal
  condition — so treat the pydantic shard as the pattern, not the file as a
  whole.
- **Do not upper-bound to make resolution succeed.** A cap is a deliberate
  decision with a tracking issue and the full issue URL at the cap site, per the
  parent `providers/AGENTS.md` rule.
- **Wait for the upstream release when one is coming.** If the library's own fix
  is merged and pending release, the correct change is a floor bump when it
  ships — not a workaround now.
- **Guard the import as well as the pin.** A dependency that may be absent on
  some interpreters is imported under `TYPE_CHECKING` or lazily inside the code
  path that needs it, so Dag parsing does not fail for users who never touch
  that service.
- **A dependency change states its blast radius.** Because the lock is shared,
  the PR says what else in the workspace the change moves.

## Consequences

- The published distribution and the CI build install the same thing, so a failure
  a user hits is reproducible here.
- Some capabilities (Ray-backed Vertex AI evaluation is the standing example) are
  unavailable on the newest Python for a while — visible in the metadata, not
  hidden behind a patched build.
- Removing a shard requires revisiting the upstream issue; the comment-with-URL
  convention is what keeps these pins from being permanent.
- Contributors adding a Google service must weigh its transitive weight before
  adding the dependency, not after CI fails.

A change **violates** this decision when it:

- vendors, side-loads, or adds an index or pre-release to make a dependency
  resolve, instead of narrowing the declared range;
- adds or edits a marker-sharded pin without a comment carrying the upstream
  issue URL and the removal condition;
- introduces an upper bound to make resolution or CI succeed, with no tracking
  issue and no comment at the cap site;
- re-implements or patches around a defect the upstream project has already
  fixed in a pending or released version, rather than moving the floor;
- imports at module top level, on a path Dag parsing reaches, a dependency
  declared under `[project.optional-dependencies]` in
  `providers/google/pyproject.toml`. ("Heavy" is not the test — over a hundred
  modules here import `google.cloud.*` at top level and that is correct, because
  those are required dependencies. The defect is a missing *extra*
  turning into a parse failure for users who never touch that service.);
- changes a shared-lock dependency without stating what else in the workspace it
  affects.

## Evidence

- #64017 — adding an aarch64 Ray wheel for Python 3.14, closed on review's
  direction to disable Ray on that Python version instead (the build was a
  nightly); superseded by #64028.
- #61794 — an AIP-99 operator PR whose real blocker was workspace-wide resolution
  (`s3fs`/`fsspec`, `grpcio-status` pre-releases): how far one unresolvable pin
  propagates.
- #67481 — a `google-cloud-aiplatform` bump entangled with a downstream version,
  opened as version-chasing rather than a narrowed range.
- #61664 — an unrelated provider's bump held up by the Beam resolution thread, and
  closed: cross-provider blast radius.
- #67341 — the preferred shape from a sibling provider, closed as unnecessary
  because upstream's own fix would resolve CI once released.
- `providers/google/pyproject.toml` — the Ray shards, `google-cloud-bigquery-storage`
  split, `looker-sdk!=24.18.0` exclusion, and the `pydantic` pin avoiding a Ray
  incompatibility, each annotated with its upstream issue.
