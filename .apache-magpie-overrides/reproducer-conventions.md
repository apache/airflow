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

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow — reproducer evidence-package layout](#apache-airflow--reproducer-evidence-package-layout)
  - [Campaign directory layout](#campaign-directory-layout)
  - [Optional probe files](#optional-probe-files)
  - [Why frozen copies](#why-frozen-copies)
  - [Cross-references](#cross-references)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — reproducer evidence-package layout

Directory layout used by [`issue-reproducer`](../../skills/issue-reproducer/SKILL.md)
when writing per-issue evidence packages, and consumed by
[`issue-reassess-stats`](../../skills/issue-reassess-stats/SKILL.md)
for campaign-level aggregation.

## Campaign directory layout

Evidence packages live under the repository's gitignored `files/` folder
(`AGENTS.md` puts all generated output there, and Breeze mounts it at
`/files`, so the reproducer is runnable in the container without copying):

```text
files/airflow-reassess/<campaign-id>/<ISSUE-KEY>/
├── description.md      (frozen copy of the issue body at extraction time)
├── issue.json          (frozen JSON snapshot from the tracker)
├── original.py         (verbatim code from the issue, untouched)
├── reproducer.py       (the adapted runnable form)
├── run.log             (captured stdout + stderr from execution)
└── verdict.json        (the structured verdict)
```

Substitute:

- `<project>` — `airflow` (lower-cased `short_name` from
  [`project.md`](project.md)); already expanded in the path above.
- `<campaign-id>` — the campaign identifier
  (e.g., `pilot-2026-09-26`).
- `<ISSUE-KEY>` — the GitHub issue number in `apache/airflow`, without
  `#` (e.g., `45123`).
- `<ext>` — `.py`. Every Airflow reproducer is Python: a Dag file, a
  plain script, or a pytest module (Helm-chart issues use the chart's
  pytest-based tests under `chart/tests/`, run with `breeze testing helm-tests`, still `.py`).

Airflow-specific conventions for `reproducer.py`:

- **Shape.** Prefer, in order: (1) a single Dag file run with
  `airflow dags test <dag_id>` for scheduling / task-execution bugs;
  (2) a plain script for library-level bugs (hooks, serialization,
  utilities); (3) a pytest module for bugs only observable through the
  test fixtures. When the reproducer later becomes a regression test in a
  fix PR, it moves to the mirrored tests path
  (`airflow/cli/cli_parser.py` → `tests/cli/test_cli_parser.py`) and must
  follow the Testing Standards in `AGENTS.md`.
- **Dag authoring.** Use the Task SDK public API — `from airflow.sdk
  import DAG, task` (and other `airflow.sdk` imports) — not the legacy
  `airflow.models` / `airflow.decorators` paths, unless the issue is
  explicitly about an Airflow 2 or deprecated import path. Give the Dag a
  unique `dag_id` such as `repro_<ISSUE-KEY>`, a fixed past `start_date`,
  `schedule=None` and `catchup=False`, so `airflow dags test` runs exactly
  one Dag run.
- **Minimal.** Keep only what triggers the reported behaviour; replace
  external systems with in-process equivalents where that does not change
  the bug. Put the expected vs. actual behaviour in a top-of-file comment
  that names the issue number.
- **Version.** Run first against `main` (the default Breeze image). When
  `main` passes, re-run against the version in the issue's
  `affected_version:<X.Y>` label or its "Apache Airflow version" field
  with `breeze shell --use-airflow-version <X.Y.Z>` — that is what
  distinguishes *fixed since* from *never reproduced*. Record both runs
  and versions in `verdict.json`. Airflow 2.x reached end-of-life on
  2026-04-22 (its version labels are renamed `_eol_affected_version:2.*`);
  a bug that reproduces only on 2.x is flagged as EOL-only in the verdict
  for a maintainer to decide on.
- **No secrets.** This is a public repository and the evidence may be
  quoted on public issues: never put real connection URIs, passwords,
  tokens, cloud credentials or hostnames from the reporter's environment
  into `reproducer.py` or `run.log`. Use placeholder connections
  (`AIRFLOW_CONN_<ID>` env vars with placeholder values) and redact any
  credential that appears verbatim in `original.py` or `description.md`.
- **Security reports.** If an issue turns out to describe a
  vulnerability, stop — do not write or run a reproducer on the public
  tracker; the report goes to `security@airflow.apache.org` per the
  [security policy](https://github.com/apache/airflow/security/policy).
- **Prose.** Write "Dag" (title case) in `verdict.json` text and any
  comment drafted from it; keep `DAG` only as the literal code token.
- **Execution.** Run only through Breeze as described in
  [`runtime-invocation.md`](runtime-invocation.md), never with `python` /
  `pytest` / `airflow` directly on the host.

## Optional probe files

When a [cross-family probe](../../skills/issue-reproducer/probe-templates.md)
was run alongside the reproducer, also persist:

```text
├── cross-type-probe.py              (the probe script across type variants)
├── cross-type-probe.log             (captured output)
├── operator-variants-probe.py       (across operator variants)
└── operator-variants-probe.log
```

A separate `cross-type-probe-findings.md` is added when the probe
surfaces project-wide signal worth recording outside `verdict.json`.

For Airflow the useful variant axes are: metadata-DB backend
(`--backend sqlite|postgres|mysql`), Airflow version
(`main` vs. `--use-airflow-version <X.Y.Z>`), executor (e.g.
`LocalExecutor` vs. `CeleryExecutor`), and — for operator bugs — the
sibling operators in the same provider. Name the probe file after the
axis (e.g. `backend-probe.py`, `version-probe.log`) when it is not a
type or operator probe.

## Why frozen copies

`description.md` and `issue.json` are deliberately **frozen** at
extraction time. The tracker may change (comments added, fields
edited, status changed) between extraction and re-verification.
Frozen copies make the verdict auditable against the same input
state the agent reviewed.

The same logic applies when re-running a campaign against a newer
codebase — comparing fresh runs against frozen description gives a
clean before/after, where comparing against live tracker state
introduces moving targets.

For `apache/airflow`, take the snapshot with
`gh issue view <ISSUE-KEY> -R apache/airflow --json number,title,body,labels,state,createdAt,updatedAt,author,comments,url`
and write `body` to `description.md`.

## Cross-references

- [`runtime-invocation.md`](runtime-invocation.md) — how the
  reproducer is executed.
- [`reassess-pool-defaults.md`](reassess-pool-defaults.md) — named
  pools that surface candidates for evidence packages.
- [`issue-reproducer/verdict-composition.md`](../../skills/issue-reproducer/verdict-composition.md) —
  the `verdict.json` schema.
