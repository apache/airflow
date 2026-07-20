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

A provider distribution is described by two files at its root, and neither is a
free-form place to put things.

`provider.yaml` is the declarative registry of what the provider *is*: its
package name, its released `versions` list, its `source-date-epoch`, and the
capabilities it contributes to Airflow — `integrations`, `operators`, `sensors`,
`hooks`, `connection-types`, `transfers`, `extra-links`, `config`, `executors`,
`auth-managers`, `secrets-backends`, `filesystems`, `excluded-platforms`. That
declaration is what generates `get_provider_info.py`, what the docs build reads,
and what Airflow itself uses to discover the provider's contributions. Several
prek hooks exist purely to catch code and `provider.yaml` disagreeing —
`check_provider_yaml_files.py`, `check_provider_conn_fields.py` (connection form
widgets present in the hook but missing from `conn-fields`),
`check_connection_doc_labels.py` (`howto/connection:` labels that match no
declared `connection-type`), and `check_excluded_provider_markers.py`.

`pyproject.toml` carries a blunt banner: *"NOTE! THIS FILE IS AUTOMATICALLY
GENERATED AND WILL BE OVERWRITTEN!"*. It is rendered from
`dev/breeze/src/airflow_breeze/templates/pyproject_TEMPLATE.toml.jinja2`, so the
project table, classifiers, build backend, and packaging config are all template
output. The one deliberate exception is the dependency lists: `dependencies` and
`[project.optional-dependencies]` are edited *in place* in the generated file and
preserved across regeneration, with `prek update-providers-dependencies
--all-files` re-deriving the cross-provider graph afterwards. From those
dependency lists flow the provider's README requirements table and its generated
docs — both auto-synced, both a recurring source of drift when someone edits one
side by hand.

The consequence is that hand-editing a downstream artefact appears to work and
then silently reverts, or — worse — passes review and ships inconsistent metadata
to PyPI. The generated files are outputs; treating them as inputs is the mistake
this decision exists to prevent.

## Decision

Declare provider metadata once, in the file that owns it, and let generation
produce everything else.

- **`provider.yaml` owns provider metadata and capability registration.** Note
  what it actually registers: `operators`, `sensors`, `hooks` and `transfers`
  list *python modules*, not classes, so a new class added to an
  already-declared module needs no yaml edit — but a new **module**, connection
  type, config option, executor, auth manager, secrets backend or filesystem
  does, in the same PR that adds the Python code. Code and `provider.yaml`
  disagreeing on that set is a defect, not a follow-up.
- **`pyproject.toml` is generated except for its dependency lists.** Dependencies
  and optional-dependency extras are edited in place there; every other part of
  the file is changed by editing the Jinja template, never the rendered output.
- **Regenerate rather than hand-patch.** After a dependency change, run
  `prek update-providers-dependencies --all-files`; do not hand-edit the README
  requirements table, the generated docs, or `get_provider_info.py` to match.
- **Do not upper-bound a dependency by default.** A cap is a deliberate decision
  that needs a justification, a tracking issue, and a comment carrying the full
  issue URL at the cap site.
- **Cross-provider symbol changes bump the consumer's floor** with a
  `# use next version` marker, because the two packages are released separately.

## Consequences

- The generated docs, README requirements tables, `get_provider_info.py`, and the
  published wheel metadata all agree, because they have one upstream source each.
- Metadata drift becomes a static-check failure rather than a bug a user finds
  after release.
- Adding a capability costs a `provider.yaml` edit alongside the code — accepted,
  because that declaration is what makes the capability discoverable at all.
- Contributors must run the generation hooks rather than editing what they see;
  a diff that touches a generated region is a signal the hook was not run.

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

- #69655 — "Flag conn-fields in hook but absent from `provider.yaml` in static
  checks": adds the static check that treats code-vs-`provider.yaml` disagreement
  as a failure, which is exactly the invariant this ADR states.
- #68991 — "Fix inconsistency between generated provider docs and
  `pyproject.toml`": a concrete instance of generated output drifting from its
  declared source.
- #67669 — "Auto-sync provider README.rst Requirements with `pyproject.toml`":
  makes the README requirements table a generated artefact rather than something
  kept in step by hand.
- #69218 — "Point provider agents at the cross-provider dependency-bump rule":
  the `# use next version` marker rule, made discoverable to contributors.
- #68740 — "Resolve `common.ai` '# use next version' pin to common-compat
  1.15.0": the marker being resolved to a concrete floor at release time — the
  mechanism working as designed.
- #69478 — "Document each provider's optional extras in its docs index": extras
  declared in `pyproject.toml` flowing into generated documentation.
