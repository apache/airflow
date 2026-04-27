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
  - [What changes between 1.2x.x and 2.x](#what-changes-between-12xx-and-2x)
  - [Scope of Helm Chart 2.0](#scope-of-helm-chart-20)
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

## What changes between 1.2x.x and 2.x

The 2.x line is not just a version bump. The refurbish work converging on `main` includes:

* **Dropped deprecations.** Long-deprecated values, templates, and behaviours that have
  carried warnings through the 1.2x.x line are removed in 2.x. Anything you remove on
  `main` should land with a deprecation warning on `chart/v1-2x-test` first (when
  feasible) so 1.2x.x users get one release of notice before the breaking change in 2.x.
* **Slimmer core chart, kustomizable overlays for optional features.** A number of
  features that today live inside the chart as feature-flagged templates and `values.yaml`
  knobs are being moved out of the core chart and into **separate kustomizable overlays**.
  The core chart on `main` is being trimmed to the components every Airflow deployment
  needs; opt-in features (extra integrations, optional sidecars, niche deployment shapes,
  etc.) ship as overlays that users layer on with `kustomize` on top of the rendered
  chart. This keeps the core chart smaller and easier to reason about, while preserving
  the functionality through a composable mechanism instead of an ever-growing
  `values.yaml`.
* **Restructuring and renames** that would be too disruptive to ship to 1.2x.x users
  (template layout, value-key reorganisations, defaults that change behaviour) — these
  belong on `main` only and are explicitly **not** backported.

> [!IMPORTANT]
> When you propose a change on `main`, decide up-front which of the three buckets it
> falls into and call it out in the PR description: (1) bug-fix or doc-fix that should
> also be cherry-picked to `chart/v1-2x-test`, (2) deprecation that lands on
> `chart/v1-2x-test` as a warning and on `main` as a removal, or (3) `main`-only
> restructuring / overlay extraction that 1.2x.x users will not see.

## Scope of Helm Chart 2.0

Concrete in-scope items for the first 2.x release, captured from the
[Release Plan](https://cwiki.apache.org/confluence/display/AIRFLOW/Release+Plan)
on Apache Confluence:

* **Drop support for Airflow `< 3.1`.** The 2.x chart will only target Airflow 3.1+,
  which lets us remove the compatibility branches that today bridge 2.x and 3.x
  Airflow on the same chart.
* **Cut out complex features and document Kustomize.** This is the overlay extraction
  work referenced above: features that don't belong in every deployment move out of
  the core chart, and the chart docs gain a Kustomize section explaining how users
  compose them back in.
* **Drop in-chart DB support, document a "simple" Postgres container setup, and use
  it in our own CI.** The bundled Postgres/MySQL subcharts go away; users running a
  trivial dev/test setup get a documented standalone Postgres container recipe, and
  CI switches to that recipe so the chart itself stays focused on Airflow.
* **Review bumping the default Helm tooling to 4.0.**
* **Consider supporting multiple Airflow instances in a single Kubernetes namespace.**

> [!NOTE]
> The release plan is the source of truth and continues to evolve ("More to come..." per
> the wiki). Treat the list above as a snapshot — when in doubt about whether something
> is in scope for 2.0, check the wiki page or ask on the dev list rather than relying
> on this file.

The 1.2x.x release line on `chart/v1-2x-test` continues in parallel on its own
cadence (the next 1.2x.x release at the time of writing is 1.22.0). The exact dates
move; the [Release Plan](https://cwiki.apache.org/confluence/display/AIRFLOW/Release+Plan)
holds the current schedule.

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

### Contributing a bug-fix that should land in 1.2x.x

Where you open the PR depends on **where the bug exists**. Pick one of the three:

1. **The bug exists in both `main` (2.x) and `chart/v1-2x-test` (1.2x.x).**
   This is the common case for any bug that has been around for a while.
   * Open the PR against **`main`**, with the fix and tests for 2.x.
   * Add (or ask a maintainer to add) the **`backport-to-chart/v1-2x-test`** label to
     the PR. After your PR is merged to `main`, the
     [Automatic Backport](../.github/workflows/automatic-backport.yml) workflow uses
     [`cherry-picker`](https://github.com/python/cherry-picker) to open a follow-up
     backport PR against `chart/v1-2x-test`. You'll see a comment with the link.
   * **If the cherry-pick conflicts**, the bot leaves the failed cherry-pick on a branch
     and posts a comment. The committer (or you, if you're comfortable) needs to open
     a manual PR against `chart/v1-2x-test` with the equivalent fix. Mention the
     original `main` PR number in the description so the two are linked.

2. **The bug exists only in `chart/v1-2x-test` (1.2x.x), not on `main`.**
   This happens when the buggy code has already been removed, rewritten, or extracted
   to an overlay on `main` as part of the 2.x refurbish.
   * Open the PR directly against **`chart/v1-2x-test`**.
   * In the PR description, briefly explain why the fix doesn't apply to `main` (e.g.
     "the affected template was removed in 2.x as part of the Kustomize overlay
     extraction in #NNNNN"). This saves the reviewer the cross-check.
   * **No** `backport-to-...` label — there is nothing to forward-port.

3. **The bug exists only on `main` (2.x), not in any released 1.2x.x.**
   This is the case for regressions introduced by the refurbish work itself.
   * Open the PR against **`main`** as a normal bug-fix.
   * **No** backport label.

> [!TIP]
> If you're not sure which bucket your bug falls into, default to scenario 1 (target
> `main` and add `backport-to-chart/v1-2x-test`). The committer reviewing the PR will
> drop the label if the backport doesn't apply, which is cheaper than figuring it out
> up-front.

The `backport-to-chart/v1-2x-test` label is the chart equivalent of
`backport-to-v3-2-test` for Airflow core (see
[README_AIRFLOW3_DEV.md](README_AIRFLOW3_DEV.md)). The `automatic-backport.yml`
workflow is generic — it strips the `backport-to-` prefix from any label and
cherry-picks the merge commit to a branch with that name — so the label is already
wired end-to-end with no chart-specific code.

# Committers / PMCs

The following sections explains the protocol for merging PRs.

## Merging PRs targeted for Airflow Helm Chart 2.X

PRs should target `main` branch.
We will cherry-pick relevant changes to `chart/v1-2x-test` branch if we decide that they are relevant to the latest release.

Before merging, decide whether the change should be backported to `chart/v1-2x-test`
according to the policy in the next section. If yes, **add the
`backport-to-chart/v1-2x-test` label** before merging. The
[Automatic Backport](../.github/workflows/automatic-backport.yml) workflow runs on
push to `main`, reads the label off the merged PR, and opens a backport PR against
`chart/v1-2x-test` automatically. If the cherry-pick conflicts, the workflow comments
on the original PR with the failure — the committer then either resolves the conflict
themselves and opens a manual backport PR, or asks the original author to do so.

## What do we backport to `chart/v1-2x-test` branch?

The `chart/v1-2x-test` branch is for development of Airflow Helm Chart `1.2x.x`.
We will backport bug-fixes and documentation changes that are relevant to the latest release.
We will not backport new features or refactorings.

* **Cleanup/Deprecations** cherry-pick according to version deprecation policy.
  * Each minor version of Airflow Helm Chart will include some level of deprecation warnings.
  * These warnings will mainly aim to be dropped at 2.0.0.
  * If agreed cleanup/deprecation is relevant to the latest release, the *warning* (not
    the removal) should be cherry-picked to `chart/v1-2x-test` so 1.2x.x users see one
    release of notice before the feature disappears in 2.x.
* **Bug-fixes** cherry-pick only those relevant to the latest chart release and not difficult to apply.
* **CI changes** cherry-pick most CI changes to keep the bugfix branch up-to-date and CI green.
* **Documentation changes** cherry-pick only if relevant to the latest chart release and not about features only in `main`.
* **Refactorings in active areas** do not cherry-pick.
* **New features** do not cherry-pick.
* **Extracting features into kustomizable overlays** do not cherry-pick. Removing a
  feature from the core chart on `main` because it has been re-shipped as an overlay
  is a breaking change for 1.2x.x users — those features stay in the core chart on
  `chart/v1-2x-test` until 2.x.

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
