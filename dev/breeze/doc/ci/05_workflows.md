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
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CI run types](#ci-run-types)
  - [Pull request run](#pull-request-run)
  - [Fork push run](#fork-push-run)
  - [Canary run](#canary-run)
- [Workflows](#workflows)
  - [Workflow Architecture Overview](#workflow-architecture-overview)
  - [Branch-Specific Behavior](#branch-specific-behavior)
  - [Tests Workflow Structure](#tests-workflow-structure)
  - [Runners](#runners)
  - [Implementation Details](#implementation-details)
  - [CodeQL scan](#codeql-scan)
  - [Publishing documentation](#publishing-documentation)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CI run types

The Apache Airflow project utilizes several types of Continuous
Integration (CI) jobs, each with a distinct purpose and context. These
jobs are executed by the `ci-amd.yml` and `ci-arm.yml` workflows.

In addition to the standard "PR" runs, we also execute "Canary" runs.
These runs are designed to detect potential issues that could affect
regular PRs early on, without causing all PRs to fail when such problems
arise. This strategy ensures a more stable environment for contributors
submitting their PRs. At the same time, it allows maintainers to
proactively address issues highlighted by the "Canary" builds.

## Pull request run

These runs are triggered by pull requests from contributors' forks. The
majority of Apache Airflow builds fall into this category. They are
executed in the context of the contributor's "Fork", not the main
Airflow Code Repository, meaning they only have "read" access to all
GitHub resources, such as the container registry and code repository.
This is necessary because the code in these PRs, including the CI job
definition, might be modified by individuals who are not committers to
the Apache Airflow Code Repository.

The primary purpose of these jobs is to verify if the PR builds cleanly,
if the tests run correctly, and if the PR is ready for review and merge.
These runs utilize cached images from the Private GitHub registry,
including CI, Production Images, and base Python images. Furthermore,
for these builds, we only execute Python tests if significant files have
changed. For instance, if the PR involves a "no-code" change, no tests
will be executed.

Regular PR builds run in a "stable" environment:

- fixed set of constraints (constraints that passed the tests) - except
  the PRs that change dependencies
- limited matrix and set of tests (determined by selective checks based
  on what changed in the PR)
- no ARM image builds are build in the regular PRs
- lower probability of flaky tests for non-committer PRs (public runners
  and less parallelism)

Maintainers can also run the "Pull Request run" from the
"apache/airflow" repository by pushing to a branch in the
"apache/airflow" repository. This is useful when you want to test a PR
that changes the CI/CD infrastructure itself (for example changes to the
CI/CD scripts or changes to the CI/CD workflows). In this case the PR is
run in the context of the "apache/airflow" repository and has WRITE
access to the GitHub Container Registry.

When the PR changes important files (for example `generated/provider_dependencies.json` or
`pyproject.toml` or `hatch_build.py`), the PR is run in "upgrade to newer dependencies" mode -
where instead  of using constraints to build images, attempt is made to upgrade
all dependencies to latest versions and build images with them. This way we check how Airflow behaves when the
dependencies are upgraded. This can also be forced by setting the `upgrade to newer dependencies`
label in the PR if you are a committer and want to force dependency upgrade.

Every pull request run of `ci-amd.yml` starts with the `Fork CI gate` job. For pull requests
whose CI runs in the contributor's fork (AIP-120), the gate skips `build-info` and therefore
everything after it, so the run finishes in seconds; see
[Fork CI for external contributors](08_fork_ci_for_external_contributors.md).

### Approval of pull request runs

GitHub holds the runs of a fork PR in `action_required` until someone with write access approves
them, for the contributors the repository's approval setting covers. The
`auto-approve-workflow-runs.yml` workflow runs every 15 minutes on `main` and calls
`breeze ci approve-workflow-runs`, which approves the waiting runs of a commit when all of these hold:

- the PR targets `main`; PRs to release branches (`v3-4-test`, `airflow-ctl/v0-1-test`, ...) run those
  branches' own workflows and always wait for a maintainer
- the PR changes no file the fork CI reconciler counts as CI-defining (`.github/`, `dev/breeze/`,
  `scripts/ci/`, `scripts/docker/`, `scripts/in_container/`, a top-level `Dockerfile*`, a
  `.pre-commit-config.yaml` in any directory),
  which define the workflows, what their jobs run and the runners they use
- the PR changes no dependency manifest or lock file (`pyproject.toml`, `uv.lock`, `package.json`,
  `pnpm-lock.yaml`, `go.mod`, Gradle build files, ...) and adds no new provider (selective checks
  run from `main`'s tree and do not know it)
- selective checks for the PR's merge commit succeed, and do not need the full set of tests, all Python
  and database versions, a canary build or upgraded dependencies (the estimate does not model them)
- the jobs selective checks select are estimated at no more than 310 job-minutes on public runners
  (about 23% of a full-tests run, just above the cluster of single core areas and the Helm chart)
- the PR changes at most 100 lines of source code; tests (including e2e tests), docs, newsfragments,
  translations, lock files and images are not counted

A PR to `main` whose CI runs in the contributor's fork (AIP-120: the `Fork CI gate` job would skip
project CI for it) is decided by one rule instead, because its `Tests (AMD)` run only executes the
gate: its runs are approved whatever its size, cost or dependency changes, unless it changes one of
those CI-defining files. Such a PR gets the `need workflow approval` label and one comment telling
the author that the CI change needs a maintainer; see
[Approving gated runs](08_fork_ci_for_external_contributors.md#approving-gated-runs).

The runs of PRs whose head repository belongs to a trusted stakeholder (`astronomer/airflow` for
Astronomer, `aws-mwaa/upstream-to-airflow` for Amazon, `VladaZakharova/airflow` for Google; listed in
`TRUSTED_STAKEHOLDER_REPOSITORIES`) are approved whatever the PR changes or targets: only people with
write access to those repositories can push the PR's commits. The repositories are matched by their
numeric id, so a repository that later takes over one of these names is not trusted. None of the
rules above apply to them, and of the rules below only the 24-hour window does: their runs are
approved even when several open PRs share the head commit, or when the runs started before the PR
was opened, retargeted or relabelled.

Only runs created in the last 24 hours (`--max-run-age-hours`) are considered, so the workflow never
approves a backlog of older waiting runs at once; those stay with maintainers. A commit that is the
head of several open PRs is never approved automatically, because its runs cannot be told apart by
PR, and only runs started after the PR was opened and after its base branch last changed are
approved (retargeting a PR does not start new runs). A run selects its jobs from the labels the PR
had when it started, while the decision uses the current labels, so for PRs that run project CI a
run started before a label that changes which tests run was added or removed
(`full tests needed`, `all versions`, `default versions only`, `latest versions only`, `canary`,
`upgrade to newer dependencies`, `area:e2e-tests`, `area:kubernetes-tests`) is left for a
maintainer, since it still runs the selection it started with; the next push is decided afresh.
For a PR whose CI runs in the fork neither the selection nor the labels a run started with matter:
the gate reads the current labels when the run starts and fails when it cannot, so a run started
before `use fork ci` or `use project ci` was added or removed is approved too. Other PRs get the
`need workflow approval` label and wait for a maintainer. Their head commit also gets a pending
`Workflow approval` commit status whose description gives the reason (cut to GitHub's 140
characters), so the author sees why in the PR's checks without a notifying comment. The status is
not a required check and does not block merging. When processing a PR fails (for example on a
GitHub API error), the workflow tries to add the label and a pending status with a generic reason,
and the workflow run fails, so the failure stays visible even if the label could not be added.

The label is removed when a later commit of the PR is approved automatically. GitHub has no event
for approving runs, but a run waiting for approval never starts, so the `mark-workflow-runs-approved.yml`
workflow follows the `Tests (AMD)` runs of fork PRs instead: when one reaches `in_progress`, a
`ubuntu-slim` job makes a few `gh api` calls that turn the commit's pending status green and remove
the label of the open PRs whose head is that commit. This covers runs a maintainer approves by hand
as well as automatic approvals, without listing the waiting runs. A PR pushed again after the run
started keeps its label, because its newer runs may wait. The job checks out nothing, and the
contributor-chosen head branch reaches its script only through the environment. A maintainer can
approve the runs after the approval workflow listed them and before it writes the label and status,
so after writing either it checks the runs again and clears both if none still waits. Only a
started `Tests (AMD)` run clears them, so a PR that removes or renames that workflow keeps its
label and pending status after its other runs are approved; such a PR changes CI definition files
and is reviewed by a maintainer anyway, and the status does not block merging.

The workflow uses only `main`'s code. It reads the PR's metadata and fetches its merge commit as git
objects to list the changed files; it never checks out or runs the PR's code. The merge commit must
have been built from the head commit whose runs get approved, so a push after the check cannot slip
into an approved run. Approving with the workflow's `GITHUB_TOKEN` starts the runs right away, and
`workflow_run` workflows that follow them still trigger.

The estimate comes from `dev/breeze/src/airflow_breeze/utils/ci_minutes_estimate.py`, which turns the
selective-check outputs into job groups with job-minutes and a wall-clock estimate, weighting
providers by the size of their test suites. The job summary lists every waiting PR with its decision,
the expected job groups and the estimated time. The numbers are medians of recent `Tests (AMD)` PR
runs on public runners; refresh them with `dev/calibrate_ci_minutes_estimates.py`, as
`.github/AGENTS.md` describes. Run `breeze ci approve-workflow-runs --report-only` (or dispatch the
workflow with `report-only`) to see the decisions without approving or labelling anything.

## Fork push run

These runs are triggered by a push to any branch of a contributor's fork in which the
`Tests (AMD)` workflow is enabled. They run in the fork, on GitHub's public runners and with the
fork's token, and are how pull requests labelled `fork ci required` get their CI result. They are
selected like a pull request run: the changed files come from comparing the pushed commit with
`apache/airflow` (`main`, or the release branch the pushed code was cut from, as breeze's default
branch at that commit records it), so a fork whose `main` is behind does not make the run bigger,
and they never use self-hosted runners. The `schedule` trigger is skipped in forks, so enabling
the workflow does not start the canary matrix there. The fork CI reconciler reads the result of these runs and reports it on the pull
request as the `Fork CI / Tests (AMD)` status.

In `apache/airflow` itself, pushes still only run CI for the release-prep and providers branches
(`CANONICAL_PUSH_BRANCH_FILTERS` in `fork_ci_gate.py`, the branches `release-branch-ci.yml` starts
full runs for), and they test only the changes since the last successful run of the branch. That
incremental base is looked up only in `apache/airflow`: a fork's push is always compared with
`apache/airflow` as described above. A push to any other branch produces a run in which only the
gate executes.

## Canary run

This workflow runs on a schedule on the `main` branch, and a few times a day
on the release branches that have new commits (see
[Release branch runs](#release-branch-runs)). The `canary` run
aims to upgrade dependencies to their latest versions and promptly
pushes a preview of the CI/PROD image cache to the GitHub Registry. This
allows pull requests to quickly utilize the new cache, which is
particularly beneficial when the Dockerfile or installation scripts have
been modified. Even if some tests fail, this cache will already include
the latest Dockerfile and scripts. Upon successful execution, the run
updates the constraint files in the "constraints-main" branch with the
latest constraints and pushes both the cache and the latest CI/PROD
images to the GitHub Registry.

If the `canary` build fails, it often indicates that a new version of
our dependencies is incompatible with the current tests or Airflow code.
Alternatively, it could mean that a breaking change has been merged into
`main`. Both scenarios require prompt attention from the maintainers.
While a "broken main" due to our code should be fixed quickly, "broken
dependencies" may take longer to resolve. Until the tests pass, the
constraints will not be updated, meaning that regular PRs will continue
using the older version of dependencies that passed one of the previous
`canary` runs.

The `canary` runs are scheduled 4 times a day on `main`, alternating between
`ci-amd.yml` (01:58, 13:58 UTC) and `ci-arm.yml` (07:28, 19:28 UTC). You can also
trigger the `canary` run manually via `workflow-dispatch` mechanism.

A scheduled canary that has nothing new to test finishes at once: its first job,
`Check if the run is needed`, looks up the last completed full run (`schedule` or
`workflow_dispatch`) of the same workflow on the branch, and when that run tested the
same commit and succeeded, every other job is skipped and the run succeeds. When the
last run failed or was cancelled, or new commits landed since, the canary runs in full.
A skipped canary does not post to Slack. A canary on an unchanged commit therefore does
not pick up dependencies released since the last run; the next commit's canary does.

# Workflows

Apache Airflow's CI system has evolved into what we call a composite workflow architecture. This approach breaks down our testing process into manageable, reusable pieces that work together to validate code changes.

## Workflow Architecture Overview

Our CI system runs on two main workflows that handle different processor architectures:

- **`ci-amd.yml`** - Handles testing on AMD64 systems (the most common architecture)
- **`ci-arm.yml`** - Runs the same tests on ARM64 systems for compatibility

These workflows don't contain all the testing logic themselves. Instead, they call smaller, specialized workflows called composite workflows. Each composite workflow focuses on one specific area of testing.

### Understanding Composite Workflows

A composite workflow is essentially a reusable piece of our CI pipeline. Rather than having one massive workflow file with hundreds of lines, we split the work into focused components:

- Basic validation happens in one workflow
- Image building gets its own workflow
- Database testing has dedicated workflows
- Kubernetes testing runs separately

Each composite workflow is stored in `.github/workflows/` and gets called using GitHub's `uses: ./.github/workflows/...` syntax. When we need to modify how database tests work, we only touch that specific workflow file.

### Workflow Cancellation

GitHub Actions includes a concurrency feature that we use to prevent wasted resources. If you push new commits while tests are already running, the system cancels the old test run and starts over with your latest code. This saves time and compute resources.

### Cancelling runs of merged pull requests

Merging a pull request does not stop its `pull_request` runs, and the concurrency group does not
help, because the merge starts no new run in the pull request's group. Maintainers often merge
once the checks they need are green, while slower jobs are still queued or running for a result
nobody reads.

Merging pushes to the target branch, which starts `cancel-merged-pr-runs.yml`. It looks up the
merged pull request of the pushed commit and cancels its `pull_request` runs that are `queued`,
`in_progress`, `waiting`, `pending` or `requested`. A push run has a token that can write to
`apache/airflow`, so it cancels the runs of fork pull requests too. A run is cancelled when:

- its head repository and commit are the merged pull request's (runs of pull requests from forks
  do not list their pull request, so they are matched this way);
- no open pull request has the same head commit; such runs are left to finish, since no later push
  revisits them;
- it was created before the merge. A workflow triggered by the merge itself, such as one on the
  `closed` event, is meant to run.

A run still not finished five minutes after the cancel, for example because a step ignores the
cancellation, is force-cancelled. A run of an earlier commit of the pull request is normally
cancelled by its concurrency group when the next commit is pushed. A push runs the workflow file
of the pushed branch, so the cancellation works on a release branch only once the workflow is on
that branch.

## Branch-Specific Behavior

Different branches in our repository serve different purposes, so they get different levels of testing.

### Main Branch Testing

The `main` branch undergoes the most extensive testing since it serves as the primary development branch. All available test suites execute on this branch:

- Core Airflow functionality tests
- Provider package tests
- Integration tests with external systems
- Helm chart validation
- Kubernetes deployment tests
- Documentation building and validation

We run everything on `main` because we want to catch problems as early as possible in the development cycle.

### Release Branch Testing

Release branches (named `v*-*-test`) get a more focused set of tests. These branches are used only for releasing Airflow core and Docker images, not for provider packages or Helm charts. So we skip tests that aren't relevant:

- Core Airflow tests still run
- Image building and validation continues
- Provider-specific tests are skipped
- Helm and Kubernetes tests are skipped

This approach saves time and resources while still ensuring release quality.

### Release branch runs

Release-prep and providers branches (`vX-Y-test`, `providers-<name>/vX-Y`, `chart/vX-Yx-test`,
`airflow-ctl/vX-Y-test`) do **not** run the full CI on push. Backports land in bursts while a release
is prepared, and a full AMD + ARM run per merged commit was mostly cancelled by the next merge after
it had already used hundreds of runner-minutes. Every backport PR has run CI against the branch
already, so the branch needs a full run of its current HEAD a few times a day, not one per commit.

**Full runs.** [`release-branch-ci.yml`](../../../../.github/workflows/release-branch-ci.yml) runs from
`main` on a schedule and starts `ci-amd.yml` or `ci-arm.yml` with `workflow_dispatch` on each release
branch, unless the last `workflow_dispatch` run of that workflow on the branch tested its current HEAD
and succeeded, or is still running. A run of an unchanged HEAD is repeated only when the previous one
failed or was cancelled:

| Slot (UTC)   | Workflow started |
|--------------|------------------|
| 04:43, 16:43 | `ci-amd.yml`     |
| 10:43, 22:43 | `ci-arm.yml`     |

A `workflow_dispatch` run on a release branch is a canary run: it runs the full matrix and refreshes
the branch's constraints and image cache. Branches without a commit in the last 7 days are skipped.
`release-branch-ci.yml` can also be started by hand, with optional `branch`, `platform` and `force`
inputs.

**Incremental tests on push.** `ci-amd.yml` still has a `push` trigger for these branches (AMD only),
but a push run only tests what changed since the last successful run of `ci-amd.yml` on the branch,
whether that was a push run or a full run. The first job, `Check if the run is needed`, finds that
run's commit through the Actions API on `ubuntu-slim`. `build-info` fetches that one commit (a
two-commit diff needs no history in between) and passes it to selective checks as
`--push-base-commit`, together with `--selective-tests-on-push`, so the run selects tests from the
files changed since that commit as for a pull request, and is not a canary run. If there is no
successful run to build on, or the base commit cannot be fetched or diffed, all tests run. A
successful run of the same commit is passed over, so re-running a push run tests it again.

Push runs have a concurrency group of their own per branch: a newer push cancels the push run still
in progress, and the newer run then covers the cancelled run's changes too, because a cancelled run
is not a successful one. A failed run's changes are tested again the same way. A push never cancels a
full run of the branch.

Nothing has to be switched on or off for a release.

The workflow files of the release branch itself decide what a run does, so an older release branch
only gets this behaviour once the change that introduced it has been backported to it. Until then it
keeps running the full CI on every push.

The CI image cache of `vX-Y-test` and providers branches is still refreshed on every push, by
[`refresh-image-registry-cache.yml`](../../../../.github/workflows/refresh-image-registry-cache.yml).

### Branch Detection Logic

The system determines which tests to run through the `build-info` job. This job checks the `AIRFLOW_BRANCH` setting in `airflow_breeze/branch_defaults.py` to understand what branch type it's working with, then enables or disables test groups accordingly.

## Tests Workflow Structure

The Tests workflow handles all code validation for Apache Airflow. Its scheduled runs on `main` and its full runs on release branches (see [Release branch runs](#release-branch-runs)) are "canary" builds that also update shared infrastructure like dependency constraints and Docker images.

### Workflow Execution Flow

Tests run in a specific sequence, with each stage building on the results of previous stages:

#### 1. Build Info

The build-info job examines what files changed in your pull request and decides which tests need to run. If you only changed documentation, it might skip Python tests entirely. If you modified provider code, it focuses on provider-related testing. This selective approach saves time and resources.

#### 2. Basic Tests

Before running expensive tests, we validate basic functionality:

- Breeze development environment tests
- React UI component tests
- Code formatting and style checks
- Dependency upgrade monitoring

These quick checks catch obvious problems early.

#### 3. Image Building

We build the Docker environments needed for testing:

- CI images optimized for running tests
- Production images that match what users download
- Dependency constraint files that lock versions for consistent testing

Having standardized environments ensures tests behave predictably.

#### 4. Core Testing

The main testing phase runs multiple test suites in parallel:

- Database tests against PostgreSQL, MySQL, and SQLite
- Non-database tests for core logic
- Provider package tests for external integrations
- Integration tests that verify component interactions

Parallel execution speeds up the overall process.

#### 5. Specialized Testing

Additional tests verify specific deployment scenarios:

- Kubernetes integration tests
- Helm chart deployment tests
- Task SDK and CLI tool validation

These ensure Airflow works correctly in various real-world environments.

#### 6. Finalization

Successful test runs trigger several cleanup and publishing tasks:

- Updated Docker images get pushed to the registry
- Constraint files get updated with tested dependency versions
- Test warnings and issues get summarized
- Artifacts get prepared for future builds

### Composite Workflow Groups

Here's what each workflow group does and when it runs:

| Workflow Group                     | Purpose                                                     | PR      | main    | v*-*-test |
|------------------------------------|-------------------------------------------------------------|---------|---------|-----------|
| **Build Info**                     | Analyzes changes and determines which tests to run          | Yes     | Yes     | Yes       |
| **Basic Tests**                    | Runs quick validation: Breeze, UI, static checks            | Yes     | Yes     | Yes       |
| **CI Image Build**                 | Builds Docker images for testing                            | Yes     | Yes     | Yes       |
| **Additional CI Image Checks**     | Validates CI images meet quality standards                  | Yes     | Yes     | Yes       |
| **Generate Constraints**           | Creates dependency version lock files                       | Yes     | Yes     | Yes       |
| **CI Image Checks**                | Runs static analysis and builds docs (AMD only)             | Yes     | Yes     | Yes       |
| **Provider Tests**                 | Tests integration with external services                    | Yes     | Yes     | Yes (1)   |
| **Unit Tests (Multiple Groups)**   | Runs core functionality tests with different databases      | Yes     | Yes     | Yes (1)   |
| **Special Tests**                  | Handles integration and system tests (AMD only)             | Yes (3) | Yes     | Yes (1)   |
| **Helm Tests**                     | Tests Kubernetes deployment via Helm                        | Yes     | Yes     | Yes (1)   |
| **PROD Image Build**               | Builds production-ready Docker images                       | Yes     | Yes     | Yes       |
| **Additional PROD Image Tests**    | Final validation of production images (AMD only)            | Yes     | Yes     | Yes       |
| **Kubernetes Tests**               | Tests deployment in Kubernetes environments                 | Yes     | Yes     | Yes (1)   |
| **Distribution Tests**             | Tests Task SDK and CLI tools (AMD only)                     | Yes     | Yes     | Yes       |
| **Agent Framework Tests**          | Tests Common AI's adapters for other agent frameworks (4)   | Yes     | Yes     | No        |
| **Finalize Tests**                 | Publishes results and updates shared resources              | Yes     | Yes (2) | Yes (2)   |

#### AMD-Only Workflows

Some workflows run only on AMD architecture because they produce architecture-independent results. Running documentation builds or static analysis on both AMD and ARM would waste resources without providing additional value.

### Individual Jobs (Non-Composite)

A few jobs run as individual tasks rather than composite workflows:

| Individual Job                     | Purpose                                                     | PR      | main    | v*-*-test |
|------------------------------------|-------------------------------------------------------------|---------|---------|-----------|
| **Pin Versions Hook** (AMD only)   | Ensures dependency versions are properly locked             | Yes     | Yes     | Yes       |
| **Go SDK Tests**                   | Tests Go language components                                | Yes     | Yes     | Yes       |
| **Summarize Warnings** (AMD only)  | Collects and organizes warnings from all test runs          | Yes     | Yes     | Yes       |

#### Why These Remain Separate

- **Pin Versions Hook**: Simple validation that doesn't require complex orchestration
- **Go SDK Tests**: Uses Go's native testing tools, separate from Python ecosystem
- **Summarize Warnings**: Runs after other tests complete, just aggregates data

### Footnotes

The numbered references in the tables above have specific meanings:

**`(1)` Release Branch Optimization**

Release branches skip certain test categories:

- Provider tests are skipped since providers aren't released from these branches
- Helm and Kubernetes tests are skipped for the same reason
- This saves time and compute resources while maintaining release quality

**`(2)` Image and Constraint Publishing**

Successful test runs trigger publishing activities:

- Cache images for both AMD and ARM get pushed to speed up future builds
- Only AMD images get the "latest" tag since it's our primary architecture
- Dependency constraint files only get updated during canary runs

**`(3)` Conditional Special Tests**

Special tests (integration and system tests) run selectively:

- When complete test coverage is required for thorough validation
- In canary runs for scheduled quality checks
- When dependency upgrades require thorough testing

**`(4)` Agent Framework Tests**

The [Common AI provider](../../../../providers/common/ai/docs/index.rst) lets an agent built with
another framework use Airflow's toolsets, through small adapters in
[`airflow.providers.common.ai.tools`](../../../../providers/common/ai/src/airflow/providers/common/ai/tools).
The adapter tests import the framework they adapt, so they only run where that framework is
installed. Today that is two frameworks, [Strands Agents](https://strandsagents.com/), an open-source
agent SDK from AWS, and [Google ADK](https://google.github.io/adk-docs/), and neither can be installed
in the workspace: Strands caps `mcp`, and ADK caps `opentelemetry` and `websockets`, below the versions
`uv.lock` resolves. The regular test jobs therefore skip their adapter tests.

This job runs once per framework, each installing its framework into the CI image and running the
Common AI tool tests (`providers/common/ai/tests/unit/common/ai/tools`), so a breaking release of one
framework does not hide the other's result. It runs on `main` only, on amd64, when Common AI or
common.sql code, the Common AI dependencies or `uv.lock` change, or when the run tests everything.

- Outside a canary run, every package in the image keeps its version and the frameworks' caps on them
  are overridden, so the adapters are tested against the same dependencies as the rest of Airflow.
- On a canary run, the frameworks' own dependency pins win, which is the environment a user who
  installs them gets.

Both install the newest framework releases older than the repository's uv `exclude-newer` window, so a
release that breaks an adapter fails this job about that long after it ships. The job is not a
dependency of **Finalize Tests**, so such a release does not stop the image cache from being pushed.

## Runners

Two kinds of GitHub-hosted runner are used, and which one a job gets depends on what
the job actually needs.

**`ubuntu-22.04` / `ubuntu-22.04-arm`** (2 cores, 7-16 GB RAM, privileged) is the
default for anything that builds or runs a container, installs Airflow, or runs a real
test suite. Jobs that get their runner from selective checks (`amd-runners` /
`arm-runners`, see [04_selective_checks.md](04_selective_checks.md)) always land here.

**`ubuntu-slim`** (1 core, 5 GB RAM, 14 GB disk) is used for the short bookkeeping jobs
around the edges of CI — computing a matrix, posting a Slack notification, closing stale
issues, checking a newsfragment name. It is a container rather than a VM and runs
unprivileged, which constrains what can go on it:

- **No Docker daemon.** The Docker *client* is on the image, but nothing can build an
  image, start a container, or use a Docker-container action. Anything touching Breeze
  is out.
- **`python3` is Ubuntu's system interpreter**, so it is [PEP 668](https://peps.python.org/pep-0668/) externally managed and
  a bare `pip install` fails. Stdlib-only scripts are fine; a job that needs
  dependencies must bring its own interpreter. For anything driven by `uv run`, use
  `astral-sh/setup-uv` with `version-file: uv.lock` — that installs the uv version the
  workspace is locked to, and `uv run` then provisions its own Python.
- **Minimal toolset** — `git`, `gh`, `jq`, `node`, `curl` are present; Java and Go are
  not. `actions/setup-*` still works, since those actions download into the tool cache.
- **One core**, so give a job that does real work a timeout with some slack in it.

When adding a job, reach for `ubuntu-slim` if it only shuffles metadata around, and
`ubuntu-22.04` otherwise.

### Spot interruptions of self-hosted runners

Runs of `apache/airflow` committers and the canary run on self-hosted
[RunsOn](https://runs-on.com) runners, which are EC2 Spot instances. When AWS reclaims
one, RunsOn fails the job with an error annotation titled "EC2 Spot interruption", and
once the run finishes it re-runs the failed jobs, after attempts 1 and 2; attempt 3 is
its last. An interrupted job ends with exit code 137 and no test failure in its log.

- **Pull requests.** `spot-interruption-notice.yml` checks the newest "Tests (AMD)" run of
  each pull request every 10 minutes, including runs still in progress, and keeps one
  comment on the pull request: which jobs were interrupted and that RunsOn re-runs them,
  and, once the re-run finished, whether it passed. It runs on a schedule, not on the
  runs' events, so no pull request run gets write access. GitHub server errors (HTTP 5xx)
  are retried, and when they persist the affected run is skipped with a warning and left
  to the next check rather than failing the workflow; other errors still fail it.
- **Slack.** The `notify-slack` job of a canary does not post when every failure is a Spot
  interruption that RunsOn re-runs, and leaves the notification state alone; it posts for
  RunsOn's re-run instead, including when the re-run passed. It still posts for attempt
  3, and for any other failure. A re-run started by a maintainer does not post.

Both use `scripts/ci/spot_interruptions.py`, which recognises an interruption only by the
annotation on a job that ran on a RunsOn runner: a pull request's run executes the pull
request's workflow file, which could add such an annotation, but external contributors'
runs use GitHub-hosted runners. A notice names the run it is about, and only that run's
re-run reports into it. Completed runs are looked at for 7 days after they were created,
so the outcome of a re-run of an older run is not reported.

**API budget of the notice.** Each poll uses the workflow's `GITHUB_TOKEN`, whose limit for
`apache/airflow`, a repository of an Enterprise Cloud account, is 15,000 requests an hour,
shared by every workflow of the repository (it would be 1,000 outside an enterprise). Measured
in October 2026, with 120-340 pull request runs of "Tests (AMD)" completing a day, a poll makes
about 45-55 requests:

| Requests | Per poll |
|---|---|
| Runs in progress | 1 |
| Runs completed in the last 7 days, listed a day at a time (100 per page) | ~22 |
| Jobs of each run in progress or changed in the last 2 hours (50 per page; ~140 jobs in a full run) | ~15-27 |
| Annotations of failed jobs on RunsOn runners, pull request lookups, comments | ~5, more while runs are interrupted |

That is about 270-330 requests an hour at one poll every 10 minutes, about 2% of the limit.
Most of it is the listing of completed runs, which grows with the 7-day window: shortening
it to 3 days saves about 12 requests a poll. A day with more than 1,000 completed runs would
exceed what GitHub lists for one query, and the script then warns that older runs are missed.

## Implementation Details

Here's how the composite workflow system is organized in practice.

### Composite Workflow Files

Each composite workflow is stored in a separate file under `.github/workflows/`. This organization makes the system easier to maintain and understand:

#### Basic Validation

- `basic-tests.yml` - Handles Breeze tests, UI validation, static analysis, and upgrade monitoring

#### Image Management

- `ci-image-build.yml` - Builds Docker images for testing
- `additional-ci-image-checks.yml` - Validates CI images meet quality standards
- `prod-image-build.yml` - Creates production-ready images
- `additional-prod-image-tests.yml` - Final validation of production images

#### Standards and Documentation

- `generate-constraints.yml` - Creates dependency version lock files
- `ci-image-checks.yml` - Runs static analysis and builds documentation

#### Testing

- `run-unit-tests.yml` - Flexible testing framework that works with different databases
- `test-providers.yml` - Tests integration with external services
- `special-tests.yml` - Handles complex integration and system tests

#### Deployment Testing

- `helm-tests.yml` - Tests Kubernetes deployment via Helm charts
- `k8s-tests.yml` - Tests direct Kubernetes integration
- `airflow-distributions-tests.yml` - Validates package distributions

#### Finalization

- `finalize-tests.yml` - Publishes results and updates shared resources

### Benefits of Composite Workflows

This architecture provides several practical advantages:

#### Modularity

Each workflow handles one specific area of testing. When you need to modify provider testing, you only touch `test-providers.yml`. This isolation prevents changes from having unexpected side effects.

#### Maintainability

Problems are easier to diagnose and fix. Database test failures point you directly to `run-unit-tests.yml`. Image building problems are isolated in the image-related workflows.

#### Reusability

Both AMD and ARM workflows use the same composite workflows. We write the testing logic once and reuse it across architectures.

#### Clarity

The main workflow files focus on orchestration rather than implementation details:

```yaml
- name: "Basic tests"
  uses: ./.github/workflows/basic-tests.yml
- name: "Build CI images"
  uses: ./.github/workflows/ci-image-build.yml
```

#### Parallel Execution

Independent workflows can run simultaneously. While images are building, static checks can run in parallel, reducing overall execution time.

#### Expertise Focus

Different team members can specialize in different workflow files. Kubernetes experts maintain `k8s-tests.yml`, while Docker specialists handle the image building workflows.

This approach has transformed our CI system from a single large workflow into a collection of focused, maintainable components.

## CodeQL scan

The [CodeQL](https://securitylab.github.com/tools/codeql) security scan
uses GitHub security scan framework to scan our code for security
violations. It is run for JavaScript and Python code.

## Publishing documentation

Documentation from the `main` branch is automatically published on Amazon S3.

To make this possible, GitHub Action has secrets set up with credentials
for an Amazon Web Service account - `DOCS_AWS_ACCESS_KEY_ID` and
`DOCS_AWS_SECRET_ACCESS_KEY`.

This account has permission to write/list/put objects to bucket
`apache-airflow-docs`. This bucket has public access configured, which
means it is accessible through the website endpoint. For more
information, see: [Hosting a static website on Amazon
S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/WebsiteHosting.html)

Website endpoint:
<http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/>

-----

Read next about [Debugging CI builds](06_debugging.md)
