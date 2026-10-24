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

# Releasing the language SDKs

This is the shared release-management guide for the language SDKs that live in this repository:
the TypeScript SDK (`ts-sdk/`), the Java SDK (`java-sdk/`) and the Go SDK (`go-sdk/`). It covers
what every language SDK release has in common: the release cycle, the branch to release from, how
versions relate to Airflow core, and what a release candidate has to satisfy before it is published.

The per-language steps (build, sign, stage, publish) stay with each SDK, because the toolchains
have nothing in common. Follow this document first, then the SDK's own publishing guide:

| SDK | Published as | Publishing guide |
|---|---|---|
| TypeScript SDK | `apache-airflow-ts-sdk` on npm | [`ts-sdk/DEVELOPMENT.md`](../ts-sdk/DEVELOPMENT.md) |
| Java SDK | `org.apache.airflow:airflow-sdk` and friends on Maven Central | [`java-sdk/DEVELOPMENT.md`](../java-sdk/DEVELOPMENT.md) |
| Go SDK | `github.com/apache/airflow/go-sdk` on the Go module proxy | [`go-sdk/DEVELOPMENT.md`](../go-sdk/DEVELOPMENT.md) |

The Python Task SDK is not covered here. It is released together with Airflow core, as part of
[`README_RELEASE_AIRFLOW.md`](README_RELEASE_AIRFLOW.md).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Release cycle](#release-cycle)
- [Release branch](#release-branch)
- [Versioning](#versioning)
- [What a release requires](#what-a-release-requires)
- [Release checklist](#release-checklist)
  - [Schema and spec drift](#schema-and-spec-drift)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Release cycle

Each language SDK has its own release cycle. What ties a release to Airflow core is the branch it
is cut from, not its timing: an SDK release is always cut from a core release branch, but an SDK
decides for itself when to release.

Core feature releases (`X.Y.0`, roughly every two or three months, see
[Airflow's release process and version policy](../airflow-core/docs/release-process.rst)) are the
default moment to cut one, for an SDK with changes worth shipping. An SDK with nothing
user-visible to ship can skip a cycle entirely, and a patch release can happen at any time
against the branch of the core minor it targets.

# Release branch

Language SDKs are released from the core release branches, not from `main`.

Core creates a `vX-Y-test` branch early in the cycle, releases betas and release candidates from
it, and syncs it into `vX-Y-stable` for the final release. See
["Beta releases: fast-forwarding `vX-Y-test` to `main`"](README_RELEASE_AIRFLOW.md#beta-releases-fast-forwarding-vx-y-test-to-main)
for how that branch moves.

The rules for SDK changes are the same as for the rest of the repository:

- Land the change on `main` first.
- Cherry-pick it to the active `vX-Y-test` branch, the way core and provider fixes are
  cherry-picked. Do not commit SDK changes directly to a release branch.
- Cut the SDK release candidate from `vX-Y-test`, so the SDK sources match the core version the
  release targets. Cherry-picks always land on `vX-Y-test`, including after the core release is
  final; `vX-Y-stable` receives them once a release is cut from it.

A patch release is the same thing again: cherry-pick the fix onto the active `vX-Y-test` and tag
from there. Language SDKs do not get release branches of their own.

This matters more for a language SDK than for a Python distribution, because the SDK and the
supervisor it talks to are versioned together. An SDK built from `main` can declare a newer
supervisor schema than a released core accepts, and a runtime built that way is rejected before
the first task starts. Releasing from the release branch is what keeps the published SDK and the
published core in agreement.

# Versioning

Each language SDK carries its own version and its own SemVer promise. SDK versions are not
pinned to the Airflow version: the Java SDK being `1.0.0` says nothing about which core minor it
was released with, and a core release does not require a matching SDK bump.

The compatibility record is `<sdk>/capabilities.yaml`, which declares the minimum Airflow version
and the supervisor schema the SDK supports, and from which each SDK's published compatibility
matrix is generated. See the
[Language SDK conformance spec](../contributing-docs/30_new_language_sdk.rst) for the dimensions
and the prek hook that regenerates the tables.

So a release must state, in its announcement and in its compatibility matrix, which Airflow
versions it supports. A release that raises the minimum Airflow version or bumps the supervisor
schema is a breaking change for users on older core versions and is versioned accordingly.

Version strings differ per ecosystem, and each SDK's publishing guide gives the exact form:

| SDK | Version example | Release tag |
|---|---|---|
| TypeScript SDK | `1.0.0-beta1` (npm semver) | `ts-sdk/<version>` |
| Java SDK | `1.0.0-beta1` ([Maven version order]) | `java-sdk/<version>-rc<N>`, then `java-sdk/<version>` |
| Go SDK | `v1.0.0-beta1` (Go semver, with the `v`) | `go-sdk/v<version>` |

[Maven version order]: https://maven.apache.org/pom.html#Version_Order_Specification

# What a release requires

Anything published to Maven Central, npm or the Go module proxy that is not a snapshot is a
release under the [ASF Release Policy](https://www.apache.org/legal/release-policy.html#release-types),
alpha and beta included. The artifact the project formally votes on is a signed **source
package** in `https://dist.apache.org/repos/dist/dev/airflow/`; the npm tarball, the Maven
artifacts and the Go module are convenience binaries built from it.

A general-availability release of any language SDK therefore requires all of:

1. A signed release candidate tag, pushed to `apache/airflow` before the vote, so reviewers can
   check out the exact source being voted on.
2. A signed source package (`.tar.gz`, `.asc`, `.sha512`) uploaded to `dist/dev/airflow/<sdk>/`.
3. A `[VOTE]` thread on `dev@airflow.apache.org`, open for at least 72 hours, linking the tag and
   commit, the source package, the convenience binaries, and the `KEYS` file.
4. Three binding `+1` votes from PMC members and a `[RESULT][VOTE]` summary.
5. Promotion of the source package from `dist/dev` to `dist/release`, and only then publication
   of the convenience binaries.

The Java SDK does this for every release today, including its betas, and
[`java-sdk/DEVELOPMENT.md`](../java-sdk/DEVELOPMENT.md) is the worked example to follow: it has the vote
template, the reviewer checklist and the post-vote steps.

The TypeScript and Go SDKs have so far published pre-GA betas straight from a tag, without a
source package or a vote. That is a gap to close, not a precedent: the source release and the
vote above are required from the first GA of each SDK, and each SDK needs source-packaging and
signing tooling in place before that release is cut.

# Release checklist

Common to every language SDK release. The per-SDK guides expand steps 5 to 9.

1. Confirm which core minor you are releasing against, and work from that minor's `vX-Y-test`
   branch.
2. Confirm every change you intend to ship has landed on `main` and been cherry-picked to that
   branch.
3. Re-sync the vendored schemas and the generated code against the release branch, and commit any
   diff before tagging. See [Schema and spec drift](#schema-and-spec-drift) below.
4. Check `<sdk>/capabilities.yaml` and the generated compatibility matrix are current, and decide
   the version from what changed, including any minimum Airflow version or supervisor schema bump.
5. Bump the SDK version on the release branch through a normal PR.
6. Sign and push the release candidate tag.
7. Build the source package, sign it, and upload it to `dist/dev/airflow/<sdk>/<version>-rc<N>/`.
8. Stage the convenience binaries and verify them.
9. Call the vote, collect the result, promote the source package, and publish the convenience
   binaries.
10. Announce the release on `dev@` and `announce@airflow.apache.org`, naming the Airflow versions
    it supports.
11. Update the version on the release branch to the next development version, where the SDK's
    toolchain uses one.

## Schema and spec drift

Every SDK vendors two schemas that airflow-core and the Task SDK own, and generates code from
them: the supervisor schema it speaks to the coordinator, and the Dag serialization schema its
Dag, task and task group specs are built from.

Prek keeps the vendored copies and the generated code in step on `main`, and the hooks fire on
the files a commit touches. A release branch breaks that: it is assembled from partial
cherry-picks, so an SDK fix can be picked without the core schema change it was written against,
or a core schema change can be picked without the SDK copy that follows it. Neither shows up as
a failing hook, because no commit on the branch touched both sides.

So run the hooks over the whole tree on the release branch, before tagging:

```shell
prek run --all-files \
  sync-ts-sdk-schemas \
  sync-java-sdk-supervisor-schema \
  sync-java-sdk-dag-schema \
  sync-go-sdk-schemas \
  check-go-sdk-generated-drift \
  ts-sdk:check-ts-sdk-supervisor-schema \
  ts-sdk:check-ts-sdk-dag-schema \
  check-ts-sdk-serialization-conformance-shared \
  check-go-sdk-serialization-conformance \
  check-java-sdk-serialization-conformance
```

The `sync-*` hooks rewrite a stale vendored copy and fail, so review the diff and commit it. What
each SDK vendors and generates from it:

| SDK | Vendored copies | Generated from them |
|---|---|---|
| TypeScript SDK | `ts-sdk/schema/` | `ts-sdk/src/generated/`, committed, refreshed by `sync-ts-sdk-schemas` |
| Java SDK | `java-sdk/sdk/schema/` | nothing committed: `generateDagDsl` and `generateJsonSchema2Pojo` run as part of the build |
| Go SDK | `go-sdk/schema/` | the specs and coordinator-protocol models, committed, regenerated by `just generate-specs` and `just generate-models` |

A diff here is a decision, not just a fix to commit. If a vendored schema moves, work out whether
the release branch is missing the core commit the SDK change was written against, and pick it if
so. If the schema the SDK ends up on is older or newer than the one on `main`, record that in
`<sdk>/capabilities.yaml` so the published compatibility matrix matches what the release actually
speaks.

The serialization conformance hooks are the end-to-end check: they serialize the shared test Dags
with the SDK and with Airflow on that branch and compare the two. Passing them is what says the
generated specs still match the core the release targets.
