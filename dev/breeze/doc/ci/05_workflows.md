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
  - [Workflow Architecture Overview](#workflow-architecture-overview)
  - [Branch-Specific Behavior](#branch-specific-behavior)
  - [Tests Workflow Structure](#tests-workflow-structure)
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

### Branch Detection Logic

The system determines which tests to run through the `build-info` job. This job checks the `AIRFLOW_BRANCH` setting in `airflow_breeze/branch_defaults.py` to understand what branch type it's working with, then enables or disables test groups accordingly.

## Tests Workflow Structure

The Tests workflow handles all code validation for Apache Airflow. When changes get pushed to `main` or release branches, these runs become "canary" builds that also update shared infrastructure like dependency constraints and Docker images.

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
