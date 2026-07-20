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

# 3. The chart releases independently and supports a range of Airflow versions

Date: 2026-07-20

## Status

Accepted

## Context

The Helm chart is its own distribution. `chart/Chart.yaml` carries a chart
`version` that moves on its own cadence and an `appVersion` that merely names
the Airflow release the chart *defaults* to. The two are not coupled: a chart
release can ship without an Airflow release, and an Airflow release ships
without necessarily moving the chart.

More importantly, `appVersion` is not what users run. `values.yaml` exposes
`airflowVersion`, `defaultAirflowTag`, and `defaultAirflowRepository`
separately, and users routinely set them — to an older supported Airflow, to a
patch release ahead of the chart's default, or to a custom-built image derived
from Airflow. The chart's only hard statement about the range it supports is the
floor enforced in `templates/check-values.yaml`:

```yaml
{{- if semverCompare "<3.1.0" .Values.airflowVersion }}
  {{ required "This chart only supports Apache Airflow version 3.1.0 and above." nil }}
{{- end }}
```

Everything between that floor and the newest Airflow is in scope. So a template
that hardcodes an assumption matching `appVersion` — a CLI flag that only exists
in the newest Airflow, a config key that was renamed, a component that only some
versions have — breaks for the users who are legitimately a version or two
behind, and it breaks at render time in their cluster.

Independent release also changes how user-facing changes are communicated. The
chart is one of the **few** distributions in this repository that genuinely
consumes newsfragments: `chart/newsfragments/` is a towncrier directory
(`config.toml` → `RELEASE_NOTES.rst`) with its own categories, `significant`
among them. That is the opposite of `providers/` and `airflow-ctl/`, whose
release managers regenerate changelogs from `git log` and where newsfragments
are a defect. A chart change that alters what users configure or what gets
rendered, and that lands without a newsfragment, is simply absent from the
release notes the user reads when deciding whether to upgrade.

## Decision

The chart is versioned, released, and documented as an independent artifact that
supports a *range* of Airflow versions. Concretely:

- **Version-dependent behaviour is gated on `.Values.airflowVersion`** via
  `semverCompare`, never on `appVersion`, the chart version, or an implicit
  assumption that the running Airflow matches the chart default.
- **The supported floor lives in `templates/check-values.yaml`** and is raised
  deliberately, as its own change, with release notes — not as a side effect of
  a feature that happens to need a newer Airflow.
- **User-facing changes carry a `chart/newsfragments/{PR}.{type}.rst`** using the
  towncrier categories in `chart/newsfragments/config.toml` (`significant`,
  `feature`, `improvement`, `bugfix`, `doc`, `misc`). Removals, renames, and
  default changes are `significant` and describe the migration.
- **Chart version bumps, `Chart.yaml` `artifacthub.io/changes`, and
  `RELEASE_NOTES.rst` are release-manager work.** A feature PR does not bump the
  chart version or hand-edit the generated release notes as a side effect.

## Consequences

- Users can adopt chart fixes without being forced onto the newest Airflow, and
  can adopt a new Airflow without waiting for a chart release.
- Templates accumulate `semverCompare` gates, which is more conditional logic
  than a single-version chart would need, and each gate is another combination
  the tests should render.
- Dropping support for an Airflow major is a real, scheduled event rather than
  an incidental cleanup — as the Airflow 2 drop and the move to chart 2.0 show.
- The newsfragment requirement is the *inverse* of the repository-wide default
  for `providers/`, so it is a standing source of contributor confusion and has
  to be checked explicitly in review.

A change **violates** this decision when, in `chart/`, it:

- makes a template assume a single Airflow version — using `appVersion`, the
  chart version, or a hardcoded assumption where `semverCompare` against
  `.Values.airflowVersion` is required;
- introduces behaviour requiring a newer Airflow than the
  `check-values.yaml` floor without either gating it or raising the floor
  deliberately;
- lands a user-facing values, rendering, or default change with no
  `chart/newsfragments/` entry, or files one under the wrong towncrier category;
- bumps the chart `version`, edits `artifacthub.io/changes`, or hand-writes
  `RELEASE_NOTES.rst` as part of an unrelated feature PR.

A reviewer should reject any chart change that silently narrows the range of
Airflow versions the chart works with, or that ships a user-visible change with
no release-note entry.

## Evidence

- #65866 — "Drop Airflow 2 Support in Chart" and #66199 — "Additional Chart
  Modifications After Airflow 2 Drop": dropping a supported Airflow major
  handled as a deliberate, multi-PR event with `significant` newsfragments, not
  as incidental cleanup.
- #67131 — "Upgrade main chart to 2.0.0": the chart's own major version moving
  independently of any Airflow release.
- #69481 — "Chart: Default airflow version to 3.3.0", following #67681 and
  #65666 for earlier defaults: the chart's *default* Airflow version is a
  routine, isolated change — precisely because templates must not depend on it.
- #64081 — "Post Helm release upgrade chart version" and #65569 — "Add
  RELEASE_NOTES.rst and Chart.yaml changes for 1.21.0": version bumps and
  release notes as release-manager mechanics, separate from feature work.
- #70043 — "Add missing newsfragment to Helm Chart": a user-facing chart change
  merged without its newsfragment and corrected afterwards — the exact gap this
  decision exists to close.
- #66118 — "Docs: Expand Helm Chart upgrade tasks in Airflow 3 migration guide":
  the cross-version upgrade path documented for chart users independently of the
  core release notes.
