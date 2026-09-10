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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Reusing main images in PR tests](#reusing-main-images-in-pr-tests)
  - [Compatibility](#compatibility)
  - [Artifact selection and failures](#artifact-selection-and-failures)
  - [Retention and refresh](#retention-and-refresh)
  - [Verifying source freshness locally](#verifying-source-freshness-locally)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Reusing main images in PR tests

Selective checks decide which tests need an image. Image selection separately decides whether
that environment must be built. A cache hit keeps the prerequisite job successful and preserves
the selected tests and Python versions.

The main publisher builds images on a daily schedule and on changes to its build inputs. Its
concurrency group lets an active publication finish when another main push arrives. The main
image selection policy accepts only artifacts from a successful run of that publisher in
`apache/airflow`, on `main`. Canary and dependency-upgrade jobs continue to build images.

## Compatibility

An image fingerprint includes the actual checkout's build inputs, their executable modes and
symlink targets, the Python version and architecture, build options, base-image digest, and
applicable constraints. Unknown inputs are included conservatively. Comparing these fingerprints
accounts for dependency changes on main since the cached build, even when the PR itself only
changes application code.

CI environments can reuse editable installations with the PR sources mounted. The consumer
overrides `MOUNT_SOURCES=skip` to `selected` for a main CI image; otherwise the unit-test jobs
would execute main's baked-in code. Package manifests, generated metadata, shared-library inputs,
and installation scripts remain part of the fingerprint. The publisher requires frozen
dependency installation: a failed frozen install must not silently resolve a different environment
under the same fingerprint.

Complete production images additionally fingerprint application and UI contents. An API or UI
change therefore needs fresh production packages and a fresh final image. The
[production dependency layer](02_images.md#reusing-production-dependency-layers) allows that build
to reuse dependency installation while installing the current PR's wheels. Wheel metadata,
constraints, and build options invalidate that layer independently of application source changes.
The actual package installation still resolves dependencies and runs `pip check`.

| Change | CI environment | Complete production image |
| --- | --- | --- |
| Editable Python implementation | Reuse compatible main environment with PR mounts | Build current application layer |
| UI implementation | Reuse compatible environment; build/test current UI | Build current UI and application layer |
| Tests or chart, with identical image inputs | Reuse | Reuse when an image is needed |
| Dependency or installation inputs | Build | Build affected layers |
| Disable image cache / dependency upgrade / canary | Build | Build |

## Artifact selection and failures

Consumers download the selected image directly using an immutable artifact reference. The
preparation job publishes a small selection artifact instead of downloading and uploading a
multi-gigabyte main image again. A missed lookup builds an image for the current workflow run.
That fallback image is also identified by a current-run artifact, rather than searching previous
builds of the PR branch.

Downloads verify the GitHub-provided archive digest and producer provenance. Failed downloads
are retried a bounded number of times; a failed verification fails the job instead of silently
substituting another environment. Main selections pin the successful publisher attempt, so a later
running or failed publisher retry cannot invalidate a retained image. Publisher retries may replace
their run-local package and constraints intermediates, while shared images remain immutable.
Re-running failed jobs can read the preparation selection from
an earlier attempt of the same workflow run. Existing workflows outside this reuse path retain
their existing stash behavior.

The consumer needs `actions: read` to download artifacts. PR jobs cannot publish trusted main
entries: repository, branch, event, workflow identity, run outcome, and source commit are checked
against the GitHub API rather than trusting fields supplied in a selection file.

## Retention and refresh

Shared artifacts use seven-day retention; candidates older than 48 hours are not selected. PR
artifacts retain their shorter lifetime. Expiration bounds storage without deleting an artifact
as soon as a newer publication appears, which would disrupt consumers already using it.

Artifacts have explicit retention and do not compete with the Actions dependency-cache quota.
The dependency-layer export uses BuildKit `mode=max`, which retains intermediate build layers;
its storage must be measured separately from the final image tar archives. Compression is useful
for those uncompressed image archives, while already-compressed BuildKit blobs gain less from it.

Record archive sizes, publication frequency, hit/miss reasons, download/load time, and complete
job duration when evaluating the policy. A faster build step alone does not demonstrate a faster
workflow. Main input changes can create extra generations between daily publications, so budget
storage from the observed publication rate rather than assuming exactly one generation per day.

## Verifying source freshness locally

With an existing CI image built from compatible dependencies, run:

```bash
AIRFLOW_REUSE_TEST_IMAGE=ghcr.io/apache/airflow/main/ci/python3.10:latest \
  uv run --project dev/breeze pytest \
  dev/breeze/tests/integration_tests/test_reused_ci_image.py -m integration_tests
```

The test mounts a temporary modified checkout and verifies that core, Task SDK, provider, and
shared-library imports see those modifications inside the existing image. It does not pull or
rebuild an image. Fingerprint, producer validation, download failure, and selective-check tests
run separately in the Breeze unit suite.
