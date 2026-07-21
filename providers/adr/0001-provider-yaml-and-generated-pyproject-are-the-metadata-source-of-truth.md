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

# 1. Provider metadata is declared once, and everything downstream is generated

Date: 2026-07-20

## Status

Accepted

## Context

`provider.yaml` is the declarative registry of what a provider *is*: package
name, released `versions`, `source-date-epoch`, and the capabilities it
contributes — `integrations`, `operators`, `sensors`, `hooks`,
`connection-types`, `transfers`, `extra-links`, `config`, `executors`,
`auth-managers`, `secrets-backends`, `filesystems`, `excluded-platforms`. It
generates `get_provider_info.py`, feeds the docs build, and is how Airflow
discovers the provider's contributions. Several prek hooks fail the build when
code and `provider.yaml` disagree — `check_provider_yaml_files.py`,
`check_provider_conn_fields.py`, `check_connection_doc_labels.py`,
`check_excluded_provider_markers.py`.

`pyproject.toml` is rendered from
`dev/breeze/src/airflow_breeze/templates/pyproject_TEMPLATE.toml.jinja2` and
carries a banner saying so. The one deliberate exception is the dependency
lists: `dependencies` and `[project.optional-dependencies]` are edited *in
place* and preserved across regeneration, with `prek
update-providers-dependencies --all-files` re-deriving the cross-provider graph.
From those lists flow the README requirements table and generated docs.
Hand-editing a generated artefact appears to work and then silently reverts, or
ships inconsistent metadata to PyPI. Generated files are outputs; treating them
as inputs is the mistake this decision prevents.

## Decision

Declare provider metadata once, in the file that owns it, and let generation
produce everything else.

- **`provider.yaml` owns provider metadata and capability registration.**
  `operators`, `sensors`, `hooks` and `transfers` list *python modules*, not
  classes, so a new class in an already-declared module needs no yaml edit — but
  a new **module**, connection type, config option, executor, auth manager,
  secrets backend or filesystem does, in the same PR that adds the Python code.
  Code and `provider.yaml` disagreeing on that set is a defect, not a follow-up.
- **`pyproject.toml` is generated except for its dependency lists.** Dependencies
  and optional-dependency extras are edited in place there; every other part is
  changed by editing the Jinja template, never the rendered output.
- **Regenerate rather than hand-patch.** After a dependency change, run
  `prek update-providers-dependencies --all-files`; do not hand-edit the README
  requirements table, the generated docs, or `get_provider_info.py` to match.
- **Do not upper-bound a dependency by default.** A cap needs a justification, a
  tracking issue, and a comment carrying the full issue URL at the cap site.
- **Cross-provider symbol changes bump the consumer's floor** with a
  `# use next version` marker, because the two packages are released separately.

## Consequences

- The generated docs, README requirements tables, `get_provider_info.py`, and the
  published wheel metadata all agree, because each has one upstream source.
- Metadata drift becomes a static-check failure rather than a post-release bug.
- Adding a capability costs a `provider.yaml` edit alongside the code — that
  declaration is what makes the capability discoverable at all.
- A diff that touches a generated region is a signal the generation hook was not
  run.

A change **violates** this decision when it:

- hand-edits a generated region of `pyproject.toml` (anything outside the
  dependency lists), the README requirements table, the generated provider docs,
  or `get_provider_info.py`, instead of editing the source and regenerating;
- adds a new operator, sensor, hook, transfer or trigger **module**, or a new
  connection type, config option, executor, auth manager, secrets backend or
  filesystem, without the matching `provider.yaml` entry. Adding a class to a
  module already listed there, or fixing behaviour inside one, is not a
  `provider.yaml` change;
- changes dependencies without running `prek update-providers-dependencies
  --all-files`, leaving the README, docs, and cross-provider graph stale;
- adds an upper bound to a dependency with no justification, no tracking issue,
  and no issue URL in a comment at the cap site;
- changes a symbol another provider imports without bumping that provider's
  dependency floor with a `# use next version` marker.

## Evidence

- #69655 — adds the static check treating code-vs-`provider.yaml` disagreement as
  a failure.
- #68991 — generated provider docs drifting from `pyproject.toml`.
- #67669 — makes the README requirements table a generated artefact.
- #69218 — the `# use next version` marker rule, made discoverable to contributors.
- #68740 — the marker resolved to a concrete floor at release time.
- #69478 — extras declared in `pyproject.toml` flowing into generated docs.
