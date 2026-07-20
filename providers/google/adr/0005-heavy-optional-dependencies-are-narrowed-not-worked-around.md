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

This provider has the largest dependency surface in the repository: roughly
fifty `google-cloud-*` distributions plus `google-api-python-client`,
`pandas-gbq`, `pyarrow`, `looker-sdk`, `apache-beam` and — pulled in through
`google-cloud-aiplatform[evaluation]` — Ray. Those last ones are the problem
children. They are large, they carry their own deep dependency trees, and
several of them are routinely the last thing in the entire monorepo to support a
new Python version or a new CPU architecture.

Because Airflow resolves a single shared lock across the whole workspace, a
dependency that cannot resolve here does not merely break this provider. It
blocks the repository's Python-version rollout, holds up unrelated providers'
own dependency bumps, and stalls CI image builds for everyone. That is why
`providers/google/pyproject.toml` already reads the way it does: version pins
sharded by `python_version`, several of them annotated with the upstream issue
URL that will let the shard be removed, and at least one pin that exists purely
to route around a downstream incompatibility in Ray.

The tempting fix, each time, is local: vendor a wheel for the missing
architecture, add a pre-release index, patch around the broken import, or pin
something else in the tree until resolution succeeds. Each of those makes this
provider's build diverge from what users get from PyPI, and none of them retires
when upstream fixes the problem — they have to be found and removed by someone
who remembers why they exist.

The decision the project reaches, repeatedly, is the opposite: narrow the
dependency's declared range so the unsupported combination is simply not
claimed. When Ray had no wheel for a new Python version on ARM, review's answer
to a PR adding one was to disable Ray for that Python version until a stable
release exists — accepting the reduced capability on that interpreter rather
than shipping a build that differs from the published one.

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

- The published distribution and the CI build install the same thing, so a
  failure a user hits is reproducible here.
- Some capabilities — Ray-backed Vertex AI evaluation is the standing example —
  are unavailable on the newest Python for a while. That gap is visible in the
  metadata rather than hidden behind a patched build.
- Removing a shard requires someone to revisit the upstream issue. The
  comment-with-URL convention is what makes that possible; without it these pins
  are permanent.
- Contributors adding a Google service must consider its transitive weight
  before adding the dependency, not after CI fails.

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

- #64017 — "Fix CI image build failure for Python 3.14 on ARM64 by adding
  aarch64 ray wheel": closed on review's direction to disable Ray on that Python
  version instead, since the available build was a nightly and no stable release
  existed; superseded by #64028.
- #61794 — an AIP-99 operator PR whose real blocker was workspace-wide
  dependency resolution (`s3fs`/`fsspec` conflicts, `grpcio-status`
  pre-releases), demonstrating how far a single unresolvable pin propagates
  beyond the provider that introduced it.
- #67481 — a `google-cloud-aiplatform` bump entangled with a downstream
  library's version, opened as a version-chasing fix rather than a narrowed
  range.
- #61664 — an unrelated provider's dependency bump held up by the Beam
  resolution thread, and closed: cross-provider blast radius from one heavy
  dependency.
- #67341 — the preferred shape, from a sibling provider: closed as unnecessary
  because the upstream project's own fix would resolve CI once released.
- `providers/google/pyproject.toml` itself — the Ray pins sharded across three
  Python versions, the `google-cloud-bigquery-storage` split, the
  `looker-sdk!=24.18.0` exclusion and the `pydantic` pin that exists only to
  avoid a Ray incompatibility, each annotated with its upstream issue.
