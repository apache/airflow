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
  - [Canary run](#canary-run)
- [Workflows](#workflows)
  - [Differences for `main` and `v*-*-test` branches](#differences-for-main-and-v--test-branches)
  - [Tests Workflow](#tests-workflow)
  - [CodeQL scan](#codeql-scan)
  - [Publishing documentation](#publishing-documentation)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CI run types

The Apache Airflow project utilizes several types of Continuous
Integration (CI) jobs, each with a distinct purpose and context. These
jobs are executed by the `ci.yaml` workflow.

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

When the PR changes important files (for example `generated/provider_depdencies.json` or
`pyproject.toml` or `hatch_build.py`), the PR is run in "upgrade to newer dependencies" mode -
where instead  of using constraints to build images, attempt is made to upgrade
all dependencies to latest versions and build images with them. This way we check how Airflow behaves when the
dependencies are upgraded. This can also be forced by setting the `upgrade to newer dependencies`
label in the PR if you are a committer and want to force dependency upgrade.

## Canary run

This workflow is triggered when a pull request is merged into the `main`
branch or pushed to any of the `v*-*-test` branches. The `canary` run
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

The `canary` runs are executed 6 times a day on schedule, you can also
trigger the `canary` run manually via `workflow-dispatch` mechanism.

# Workflows

A general note about cancelling duplicated workflows: for `Tests` and `CodeQL` workflows,
we use the `concurrency` feature of GitHub actions to automatically cancel "old" workflow runs of
each type. This means that if you push a new commit to a branch or to a pull
request while a workflow is already running, GitHub Actions will automatically cancel the
old workflow run.

## Differences for `main` and `v*-*-test` branches

The type of tests executed varies depending on the version or branch
being tested. For the "main" development branch, we run all tests to
maintain the quality of Airflow. However, when releasing patch-level
updates on older branches, we only run a subset of tests. This is
because older branches are used exclusively for releasing Airflow and
its corresponding image, not for releasing providers or Helm charts,
so all those tests are skipped there by default.

This behaviour is controlled by `default-branch` output of the
build-info job. Whenever we create a branch for an older version, we update
the `AIRFLOW_BRANCH` in `airflow_breeze/branch_defaults.py` to point to
the new branch. In several places, the selection of tests is
based on whether this output is `main`. They are marked in the "Release branches" column of
the table below.

## Tests Workflow

This workflow is a regular workflow that performs all checks of Airflow code. The `main` and `v*-*-test`
pushes are `canary` runs.

| Job                                | Description                                                | PR      | main    | v*-*-test |
|------------------------------------|------------------------------------------------------------|---------|---------|-----------|
| Build info                         | Prints detailed information about the build                | Yes     | Yes     | Yes       |
| Push early cache & images          | Pushes early cache/images to GitHub Registry               |         | Yes (2) | Yes (2)   |
| Check that image builds quickly    | Checks that image builds quickly                           |         | Yes     | Yes       |
| Build CI images                    | Builds images                                              | Yes     | Yes     | Yes       |
| Generate constraints/CI verify     | Generate constraints for the build and verify CI image     | Yes     | Yes     | Yes       |
| Build PROD images                  | Builds images                                              | Yes     | Yes     | Yes (3)   |
| Run breeze tests                   | Run unit tests for Breeze                                  | Yes     | Yes     | Yes       |
| Test OpenAPI client gen            | Tests if OpenAPIClient continues to generate               | Yes     | Yes     | Yes       |
| React WWW tests                    | React UI tests for new Airflow UI                          | Yes     | Yes     | Yes       |
| Test examples image building       | Tests if PROD image build examples work                    | Yes     | Yes     | Yes       |
| Test git clone on Windows          | Tests if Git clone for for Windows                         | Yes (4) | Yes (4) | Yes (4)   |
| Upgrade checks                     | Performs checks if there are some pending upgrades         |         | Yes     | Yes       |
| Static checks                      | Performs full static checks                                | Yes (5) | Yes     | Yes (6)   |
| Basic static checks                | Performs basic static checks (no image)                    | Yes (5) |         |           |
| Build and publish docs             | Builds and tests publishing of the documentation           | Yes (8) | Yes (8) | Yes (8)   |
| Spellcheck docs                    | Spellcheck docs                                            | Yes     | Yes     | Yes (7)   |
| Tests wheel provider distributions | Tests if provider distributions can be built and released  | Yes     | Yes     |           |
| Tests Airflow compatibility        | Compatibility of provider distributions with older Airflow | Yes     | Yes     |           |
| Tests dist provider distributions  | Tests if dist provider distributions can be built          |         | Yes     |           |
| Tests airflow release commands     | Tests if airflow release command works                     |         | Yes     | Yes       |
| DB tests matrix                    | Run the Pytest unit DB tests                               | Yes     | Yes     | Yes (7)   |
| No DB tests                        | Run the Pytest unit Non-DB tests (with pytest-xdist)       | Yes     | Yes     | Yes (7)   |
| Integration tests                  | Runs integration tests (Postgres/Mysql)                    | Yes     | Yes     | Yes (7)   |
| Quarantined tests                  | Runs quarantined tests (with flakiness and side-effects)   | Yes     | Yes     | Yes (7)   |
| Test airflow packages              | Tests that Airflow package can be built and released       | Yes     | Yes     | Yes       |
| Helm tests                         | Run the Helm integration tests                             | Yes     | Yes     |           |
| Helm release tests                 | Run the tests for Helm releasing                           | Yes     | Yes     |           |
| Summarize warnings                 | Summarizes warnings from all other tests                   | Yes     | Yes     | Yes       |
| Docker Compose test/PROD verify    | Tests quick-start Docker Compose and verify PROD image     | Yes     | Yes     | Yes       |
| Tests Kubernetes                   | Run Kubernetes test                                        | Yes     | Yes     |           |
| Update constraints                 | Upgrade constraints to latest ones                         | Yes     | Yes (2) | Yes (2)   |
| Push cache & images                | Pushes cache/images to GitHub Registry (3)                 |         | Yes (3) |           |
| Build CI ARM images                | Builds CI images for ARM                                   | Yes (9) |         |           |

`(1)` Scheduled jobs builds images from scratch - to test if everything
works properly for clean builds

`(2)` PROD and CI cache & images are pushed as "cache" (both AMD and
ARM) and "latest" (only AMD) to GitHub Container Registry and
constraints are upgraded only if all tests are successful. The images
are rebuilt in this step using constraints pushed in the previous step.
Constraints are only actually pushed in the `canary` runs.

`(3)` In main, PROD image uses locally build providers using "latest"
version of the provider code. In the non-main version of the build, the
latest released providers from PyPI are used.

`(4)` Always run with public runners to test if Git clone works on
Windows.

`(5)` Run full set of static checks when selective-checks determine that
they are needed (basically, when Python code has been modified).

`(6)` On non-main builds some of the static checks that are related to
Providers are skipped via selective checks (`skip-pre-commits` check).

`(7)` On non-main builds the unit tests, docs and integration tests
for providers are skipped via selective checks.

`(8)` Docs publishing is only done in Canary run.

`(9)` ARM images are not currently built - until we have ARM runners available.

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
