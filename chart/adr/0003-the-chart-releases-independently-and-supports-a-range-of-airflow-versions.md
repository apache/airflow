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

The Helm chart is its own distribution. `chart/Chart.yaml` carries a `version` that
moves on its own cadence and an `appVersion` that merely names the Airflow release the
chart *defaults* to — the two are not coupled. And `appVersion` is not what users run:
`values.yaml` exposes `airflowVersion`, `defaultAirflowTag`, and
`defaultAirflowRepository` separately, and users routinely set them to an older
supported Airflow, a patch release, or a custom image. The chart's only hard statement
about its supported range is the floor in `templates/check-values.yaml`:

```yaml
{{- if semverCompare "<3.1.0" .Values.airflowVersion }}
  {{ required "This chart only supports Apache Airflow version 3.1.0 and above." nil }}
{{- end }}
```

Everything between that floor and the newest Airflow is in scope, so a template that
hardcodes an assumption matching `appVersion` breaks users a version or two behind, at
render time in their cluster. Independent release also changes communication: the chart
is one of the **few** distributions that genuinely consumes newsfragments —
`chart/newsfragments/` is a towncrier directory (`config.toml` → `RELEASE_NOTES.rst`)
with a `significant` category — the opposite of `providers/` and `airflow-ctl/`, where
newsfragments are a defect. A user-facing chart change landing without one is absent
from the release notes users read when deciding whether to upgrade.

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

- Users can adopt chart fixes without moving to the newest Airflow, and adopt a new
  Airflow without waiting for a chart release.
- Templates accumulate `semverCompare` gates, each another combination the tests should
  render.
- Dropping support for an Airflow major is a scheduled event, not incidental cleanup.
- The newsfragment requirement is the *inverse* of the `providers/` default, so it is a
  standing source of contributor confusion checked explicitly in review.

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

- #65866 / #66199 — dropping Airflow 2 support handled as a deliberate, multi-PR event
  with `significant` newsfragments, not incidental cleanup.
- #67131 — the chart's own major version moving independently of any Airflow release.
- #69481, following #67681 and #65666 — the chart's *default* Airflow version is a
  routine, isolated change, precisely because templates must not depend on it.
- #64081 / #65569 — version bumps and release notes as release-manager mechanics,
  separate from feature work.
- #70043 — a user-facing chart change merged without its newsfragment and corrected
  afterwards; the exact gap this decision closes.
- #66118 — the cross-version upgrade path documented for chart users independently of
  the core release notes.
