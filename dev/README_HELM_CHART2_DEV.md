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
**Table of contents**

- [Main branch is Airflow Helm Chart 2.x](#main-branch-is-airflow-helm-chart-2x)
- [Contributors](#contributors)
  - [Why separate branches?](#why-separate-branches)
  - [Developing for Airflow Helm Chart 2.x](#developing-for-airflow-helm-chart-2x)
  - [Developing for Airflow Helm Chart 1.2x.x](#developing-for-airflow-helm-chart-12xx)
- [Committers / PMCs](#committers--pmcs)
  - [Merging PRs targeted for Airflow Helm Chart 2.X](#merging-prs-targeted-for-airflow-helm-chart-2x)
  - [What do we backport to `chart/v1-2x-test` branch?](#what-do-we-backport-to-chartv1-2x-test-branch)
  - [How to backport PR with `cherry-picker` CLI](#how-to-backport-pr-with-cherry-picker-cli)
  - [Merging PRs for Airflow Chart 1.2x.x](#merging-prs-for-airflow-chart-12xx)
- [Milestones for PR](#milestones-for-pr)
  - [Set Airflow Helm Chart 2.0.0](#set-airflow-helm-chart-200)
  - [Set Airflow Helm Chart 1.2x.x](#set-airflow-helm-chart-12xx)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Main branch is Airflow Helm Chart 2.x

The `main` branch is for **cleanup, deprecations, and preparation** toward Airflow Helm Chart `2.x`.
Airflow Helm Chart `2.x` releases will be cut from `main` branch.
Airflow Helm Chart `1.2x.x` releases will be cut from [chart/v1-2x-test](https://github.com/apache/airflow/tree/chart/v1-2x-test)

> [!NOTE]
> We follow a staged approach: cleanup and refurbishment happens on `main` first.
> Once validated, relevant changes are cherry-picked to `chart/v1-2x-test`.
> This separates stability (test branch) from preparation for the next major release (main).
> Each cleanup/deprecation task is tracked via individual tickets linked to the umbrella issue.

# Contributors

The following section explains which branches you should target with your PR.

## Why separate branches?

After `1.20.0` release, we will maintain two branches for Airflow Helm Chart.
Airflow Helm Chart `1.2x.x` will be the staircase versions for Airflow Helm Chart `2.x` versions.

There is ongoing Airflow Helm Refurbish work, which includes cleanup, deprecations, breaking changes, and restructuring to reduce technical debt before cutting the next major release.
We want to be able to merge bug-fixes and documentation changes that are relevant to the latest release of Airflow Helm Chart 1.2x series without being blocked by the ongoing work on Airflow Helm Chart 2.x.
At the same time, we want to be able to merge cleanup, deprecations, and preparation work for Airflow Helm Chart 2.x on `main` without being blocked by the need to backport them to Airflow Helm Chart 1.2x series.

For the refurbish work, we have a separate project in Apache Confluence:

[Airflow Helm Refurbish Project in Apache Confluence](https://cwiki.apache.org/confluence/display/AIRFLOW/Helm+Refurbish)

## Developing for Airflow Helm Chart 2.x

PRs should target `main` branch.

## Developing for Airflow Helm Chart 1.2x.x

PR should target `chart/v1-2x-test` branch.

> [!IMPORTANT]
> The `chart/v1-2x-test` branch is **strictly for maintenance, stability, and compatibility**.
> No new features will be added here.
>
> We do not accept new features or refactorings.
> We only accept bug-fixes and documentation changes that are relevant to the latest release.
> If you want to contribute new features or refactorings, please target `main` branch.
> We will cherry-pick it to `chart/v1-2x-test` branch if we decide that it is relevant to the latest release.
> `1.2x.x` will be the latest release of Airflow Helm Chart 1.2x series.
> We will not cut any new major release from `chart/v1-2x-test` branch.

# Committers / PMCs

The following sections explains the protocol for merging PRs.

## Merging PRs targeted for Airflow Helm Chart 2.X

PRs should target `main` branch.
We will cherry-pick relevant changes to `chart/v1-2x-test` branch if we decide that they are relevant to the latest release.


## What do we backport to `chart/v1-2x-test` branch?

The `chart/v1-2x-test` branch is for development of Airflow Helm Chart `1.2x.x`.
We will backport bug-fixes and documentation changes that are relevant to the latest release.
We will not backport new features or refactorings.

* **Cleanup/Deprecations** cherry-pick according to version deprecation policy.
  * Each minor version of Airflow Helm Chart will include some level of deprecation warnings.
  * These warnings will mainly aim to be dropped at 2.0.0.
  * If agreed cleanup/deprecation is relevant to the latest release, it should be cherry-picked to `chart/v1-2x-test` branch.
* **Bug-fixes** cherry-pick only those relevant to the latest chart release and not difficult to apply.
* **CI changes** cherry-pick most CI changes to keep the bugfix branch up-to-date and CI green.
* **Documentation changes** cherry-pick only if relevant to the latest chart release and not about features only in `main`.
* **Refactorings in active areas** do not cherry-pick.
* **New features** do not cherry-pick.

## [How to backport PR with `cherry-picker` CLI](README.md#how-to-backport-pr-with-cherry-picker-cli)

## Merging PRs for Airflow Chart 1.2x.x

PRs should target `chart/v1-2x-test` branch.
We will not merge new features or refactorings.
We will only merge bug-fixes and documentation changes that are relevant to the latest release.


# Milestones for PR

## Set Airflow Helm Chart 2.0.0

Milestone will be added only to the original PR.

* PR targeting `main` branch for cleanup, deprecations, preparation work, or refactoring should be added to `Airflow Helm Chart 2.0.0` milestone.

## Set Airflow Helm Chart 1.2x.x

Milestone will be added only to the original PR.

* PR targeting `v1-2x-test` branch should be added to relevant release accordingly `Airflow Helm Chart 1.2x.x` milestone.
  * The version depends on the release cycle and the decision of the maintainers such as `1.20.0`, `1.21.0`, etc.
