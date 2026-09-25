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

- [18. Raise the floors of fast-moving dependencies automatically](#18-raise-the-floors-of-fast-moving-dependencies-automatically)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 18. Raise the floors of fast-moving dependencies automatically

Date: 2026-09-24

## Status

Proposed

## Context

Lower bounds (`>=`) of external dependencies are only ever raised by hand. Nothing in the
automated `breeze ci upgrade` flow touches them, and `check-dependency-lower-bounds` only
requires that a floor exists, not that it is recent. Fast-moving SDKs drift far behind: the
amazon provider still declares `boto3>=1.41.0` despite a comment asking to raise it
"regularly". Ancient floors mean:

- the resolver has a large candidate space to backtrack through (boto3 publishes daily);
- the lowest-direct dependency tests exercise versions nobody runs any more, and break when
  those old versions stop installing or stop matching newer transitive deps.

## Decision

`breeze ci upgrade` (the "Upgrade important CI environment" run) gets a step that raises the
lower bounds of a curated set of dependencies. It follows these rules:

- Raise the floors of a curated set of dependencies automatically, as part of the regular
  "Upgrade important CI environment" run.
- Never raise a floor to a version released less than 6 months ago.
- Never propose a floor that makes the workspace unresolvable (highest or lowest-direct).
- Never touch a package we are deliberately holding back.

### Configuration

New section in the root `pyproject.toml`, next to `[tool.uv.exclude-newer-package]`, so the
dependency policy lives in one place:

```toml
[tool.airflow.dependency-floors]
# A floor is never raised to a release uploaded more recently than this.
min-age = "180 days"
# Curated packages whose floors are kept fresh. fnmatch-style globs on canonical names.
packages = [
    "boto3",
    "botocore",
    "google-cloud-*",
    "google-api-python-client",
    "azure-*",
]
# Packages that must always share the same floor.
groups = [
    ["boto3", "botocore"],
]

[tool.airflow.dependency-floors.exclude]
# package = "reason (link)"
sagemaker-studio = "Do not change without approval from AWS (providers/amazon/pyproject.toml)"
pandas = "DataFrame XComs need pandas<3 (#70791)"
pymysql = "Capped below 1.2 in PyPI constraints generation (#67491)"
fastapi = "Repeated breakage on new releases (#59681, #59710, #59856, #61579, #68578)"
deltalake = "Missing ARM wheels on new releases (#59977, #60098, #60376)"
gunicorn = "API server startup deadlocks above 25.1 (#62524)"
qdrant-client = "Capped (#62193)"
airbyte-api = "1.x breaks provider tests (#69081)"
mysql-connector-python = "Releases without wheels for >=3.12 (#60889, #66026)"
anthropic = "Provider SDK migration is version-sensitive (#72072, #72094)"
azure-ai-projects = "2.5+ needs openai>=3, broke main (#73621, #73627)"
```

`min-age` accepts the same duration syntax as uv's `exclude-newer-package` (`N days`,
`N hours`, `N minutes`).

The explicit `exclude` list is seeded from the caps/exclusions and the single reverted
dependency upgrade in the history since 2025-03 (`git log --grep '^(Cap|Limit|Pin|Exclude)'`).
Entries that are also caught by automatic exclusion are kept anyway: they document *why*, and
survive the cap being removed.

### Component: `scripts/ci/prek/upgrade_dependency_floors.py`

A uv inline-script (same header style as `check_dependency_lower_bounds.py`), exposed as a
manual-stage prek hook `upgrade-dependency-floors` (`pass_filenames: false`,
`require_serial: true`). Units, each independently testable:

1. **`load_config(root_pyproject) -> FloorConfig`** — parses the section above; validates
   that every group member is covered by `packages`.
2. **`find_requirements(workspace) -> list[RequirementSite]`** — for each workspace member's
   `pyproject.toml` (members from `[tool.uv.workspace]`, reusing the logic in
   `check_dependency_lower_bounds.get_workspace_distribution_names`), every requirement in
   `project.dependencies`, `project.optional-dependencies` and `dependency-groups`, with file,
   section and the raw string. Workspace distributions and URL requirements are ignored. Members with a
   `uv.lock` of their own (`dev/breeze`) are not scanned: the resolve check covers only the
   root lock, so raising their floors would leave their lock stale.
3. **`get_exclusion_reason(package, sites, config) -> str | None`** — returns a reason when the
   package is in `exclude`, or when any site constrains it with `<`, `<=`, `!=`, `==`, `~=`
   or `===` ("held back by `<spec>` in `<file>`"). Caps in the root `[tool.uv]`
   `constraint-dependencies` and `override-dependencies` count as well. Otherwise `None`.
4. **`find_target_version(releases, min_age, now) -> Version | None`** — newest final
   (non-pre, non-dev), non-yanked release whose earliest upload time is `<= now - min_age`.
   `now` is a parameter, so tests need no clock mocking. PyPI JSON
   (`https://pypi.org/pypi/<name>/json`) is fetched once per package; a fetch failure or
   malformed release data skips the package with a reason instead of failing the run. After
   three consecutive connection failures PyPI is treated as unreachable and the remaining
   packages are skipped without further requests.
5. **`find_group_target(...)`** — for a group, the newest version that satisfies (4) for
   *every* member; if none, the group is skipped.
6. **`rewrite_requirement(raw, target) -> str | None`** — replaces the `>=`/`>` value only
   when `target` is higher, preserving extras, markers, other specifiers and the original
   quoting/formatting of the line. Returns `None` when there is nothing to raise. Entries
   split by marker (e.g. a higher floor for `python_version >= '3.14'`) are handled per
   entry: each is raised only if the target exceeds its current floor.
7. **`resolve_check(workspace) -> ResolveResult`** — runs `uv lock --dry-run` and
   `uv lock --dry-run --resolution lowest-direct`; returns success or the resolver stderr.
   Nothing is written.
8. **`apply_with_rollback(bumps)`** — applies all bumps (per package/group), runs (7); on
   failure bisects over the package/group units to find the failing ones, rolls those back,
   and re-checks until green. Each rolled-back unit carries the resolver error. A bump is
   applied to all of its files or to none, and a rollback restores only the occurrences the
   bump changed, so an identical requirement already at the target is never lowered.

Edits are text-level on the `pyproject.toml` files (replace the exact requirement string) so
comments and layout are preserved; provider `pyproject.toml` regeneration already preserves
dependency lists.

#### Output

- Console summary via `rich`.
- A Markdown report written to the path in `DEPENDENCY_FLOORS_REPORT` (if set) with three
  sections: **Raised** (`package: old → new`, with files), **Skipped** (reason),
  **Rolled back** (resolver error, first lines).
- Exit code 0 whenever the run completes (a rolled-back bump is a reported outcome, not an
  error), non-zero only for configuration errors or a lock that is broken before any bump. In
  that case the report says "Not updated: <error>", so the failure shows in the PR.

### Integration: `breeze ci upgrade`

- New flag `--upgrade-dependency-floors/--no-upgrade-dependency-floors`, default on, added to
  the command's option groups in the config file.
- New step `upgrade-dependency-floors` in `UPGRADE_COMMANDS`, **after**
  `upgrade-important-versions` and **before** `update-uv-lock`, so `uv lock --upgrade`
  incorporates the new floors.
- breeze sets `DEPENDENCY_FLOORS_REPORT` to a file in a temporary directory, reads it (and
  removes the directory) after the steps, and appends it to the PR body. The upgrade branch
  name is stable, so most runs update an already-open PR: its body is replaced as well, so
  the report always matches the pushed diff.

### Error handling

| Situation | Behaviour |
|---|---|
| PyPI fetch fails for a package, or its data is malformed | Skip it, reason "PyPI metadata unavailable" |
| Three consecutive connection failures | Skip the remaining packages, reason "PyPI unreachable" |
| No release old enough | Skip, reason "no release older than min-age" |
| Floor already at/above target | Nothing to do; not listed |
| Resolve check fails | Bisect, roll back offending units, report error |
| Resolve check fails with *no* bumps applied | Abort the step with the error (the lock was already broken) and leave files untouched |
| Invalid config | Non-zero exit with a clear message, also written to the report |

### Testing

`scripts/tests/ci/prek/test_upgrade_dependency_floors.py` (pytest, parametrized):

- target selection: min-age boundary, pre-releases/dev skipped, yanked skipped, earliest
  upload time used, none eligible;
- group target: intersection across members, no common version;
- rewrite: plain `>=`, `>` , with extras, with markers, with an extra `<` spec present on
  another entry, per-marker split floors, target not higher → `None`;
- exclusion: explicit list, each capping operator, capped in one file only;
- rollback: resolver mocked (`spec`/`autospec`) to fail for specific units; verifies the
  bisection rolls back exactly those and reports their errors; failure with no bumps aborts.

`dev/breeze/tests`: the new flag toggles the step, and the step runs between
`upgrade-important-versions` and `update-uv-lock`.

### Documentation

- `dev/breeze/doc/08_ci_tasks.rst` ("Running ci upgrade"): describe floor bumping, the
  config section and the exclusion rules; regenerate the command images.
- `providers/amazon/pyproject.toml`: replace "We should update minimum version of boto3 …
  regularly" with a pointer to the automation.
- The config section itself carries short comments on `min-age`, `groups` and `exclude`.

No newsfragment: build/CI tooling. Individual floor bumps land in provider changelogs through
the normal `git log`-based release notes.


## Consequences

- Floors of the curated SDKs trail PyPI by roughly six months instead of drifting for years,
  so the resolver has fewer candidates to backtrack through and the lowest-direct dependency
  tests exercise versions people still run.
- Raised floors reach users when the affected provider is next released; they show up in the
  provider changelog through the normal `git log`-based release notes.
- A bump that breaks resolution never reaches the upgrade PR: it is rolled back and reported
  in the PR body, so it cannot block the lock refresh the way #73308 did.
- Packages we hold back need no extra bookkeeping when they are capped; uncapped ones that
  must not move are listed in `[tool.airflow.dependency-floors.exclude]` with a reason.
- Out of scope:

- Fixing the `constraints-version-check` slowdown: that job measures the lock against the
  newest PyPI release, which a 6-month-old floor never reaches. The lock refresh
  (`uv lock --upgrade`) remains the fix for that.
- Bumping every external dependency. Coverage grows by extending the curated list.
- Lowering floors, adding caps, or editing `build-system.requires`.
