<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [The `.github` folder — Agent Instructions](#the-github-folder--agent-instructions)
  - [Keep the CI-minutes estimates in sync with the workflows](#keep-the-ci-minutes-estimates-in-sync-with-the-workflows)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# The `.github` folder — Agent Instructions

## Keep the CI-minutes estimates in sync with the workflows

`breeze ci approve-workflow-runs` (run by `workflows/auto-approve-workflow-runs.yml`) decides whether
to approve an external contributor's CI runs from an estimate of the job-minutes they take. The
numbers live in `dev/breeze/src/airflow_breeze/utils/ci_minutes_estimate.py` and are medians
measured on recent `Tests (AMD)` PR runs on public runners.

After you change the CI workflows in a way that adds, removes, splits or resizes jobs (new test
groups, a changed matrix, new composite workflows, a move between runner types), refresh the
estimates once the change has run on enough PRs, and in any case about once a month:

1. Run `uv run dev/calibrate_ci_minutes_estimates.py --since <date>`
   from the repository root with an authenticated `gh` CLI. Pick a date that covers at least a
   week of runs after the workflow change.
2. Copy the medians and fits it prints into the constants of `ci_minutes_estimate.py`, and update
   the "Last calibrated" line next to them.
3. When you add a new job group to the workflows, add a matching group to both
   `ci_minutes_estimate.py` (with the selective-check output that turns it on) and the
   `JOB_GROUPS` patterns of the calibration script. Unmatched jobs fall into the base group.
4. Run `uv run --project dev/breeze pytest dev/breeze/tests/test_ci_minutes_estimate.py
   dev/breeze/tests/test_workflow_run_approval.py -xvs`.

The estimates cover public runners only. Self-hosted (RunsOn) runners need their own set once there
is enough data from them.
