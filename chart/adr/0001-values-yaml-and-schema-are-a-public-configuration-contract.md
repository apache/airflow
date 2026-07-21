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

# 1. `values.yaml` and `values.schema.json` are a public, versioned configuration contract

Date: 2026-07-20

## Status

Accepted

## Context

`chart/values.yaml` is the chart's **API**, not a defaults file. Every deployment
using the official chart has a values file (or `--set` list, or Argo/Flux/Terraform
manifest) written against its key names, nesting, and types — files that live outside
this repository, are never seen by a reviewer, and are applied unchanged at the next
`helm upgrade`.

`chart/values.schema.json` *enforces* that contract: Helm validates merged values
against it before rendering, and it is heavily locked down (`additionalProperties`
appears ~400 times), so an unrecognised key is a hard failure. A value present in
`values.yaml` but missing from the schema is therefore unusable. The schema also
feeds the generated `chart/docs/parameters-ref.rst`; the `values_schema.schema.json`
meta-schema requires every parameter to carry a `default` and `description` and every
top-level property an `x-docsSection`; the `chart-schema` prek hook wires this together.
The failure mode is asymmetric — *adding* a key with a behaviour-preserving default
costs nothing, while *renaming, removing, retyping, or re-nesting* one breaks every
user who set it, and the chart releases independently so the only fix is a new chart
version. The decision below forces tidiness-driven cleanups onto the deprecation path.

## Decision

`values.yaml` and `values.schema.json` are a single public, versioned contract,
always changed together. Concretely:

- **Every value added to or changed in `values.yaml` gets a matching
  `values.schema.json` entry** — correct `type` (including the nullable forms
  where `~` is meaningful), plus the `default` and `description` the meta-schema
  requires, and an `x-docsSection` for a new top-level property.
- **Changes are additive with behaviour-preserving defaults.** A new value's
  default must render exactly what the chart rendered before it existed.
- **Renames, removals, re-nestings, and type changes go through deprecation** —
  the old key keeps working with a warning for at least one minor chart release,
  and the removal lands in a **major** chart version with a `significant`
  newsfragment in `chart/newsfragments/` that spells out the old → new mapping.
- **New per-component knobs follow the established nesting** (`workers.celery.*`
  / `workers.kubernetes.*`, `scheduler.*`, `apiServer.*`, `triggerer.*`,
  `dagProcessor.*`) and the spelling their siblings already use, rather than
  inventing a parallel shape at the root.

## Consequences

- Users can upgrade the chart within a major version without editing their values
  file — the property that makes the chart safe to pin and automate.
- Cleanups are slower: a rename is a deprecation PR, a wait, and a removal PR in a
  major version. That cost is deliberate.
- The schema grows large and duplicative, and the prek hook only partially catches
  drift between the two files.

A change **violates** this decision when, in `chart/`, it:

- adds or modifies a value in `values.yaml` without the corresponding
  `values.schema.json` entry (or vice versa), or omits the `default` /
  `description` / `x-docsSection` the meta-schema requires;
- renames, removes, re-nests, or retypes an existing value outside a major chart
  version, or without a preceding deprecation period and a `significant`
  newsfragment describing the migration;
- places a per-component knob outside the component's existing section, or names
  it inconsistently with its siblings, creating a second way to express the same
  thing.

A reviewer should reject any change that alters the meaning of an existing key,
or that lands a value in only one of the two files.

A new value whose default changes what an existing release renders is covered by
[ADR 2](0002-chart-changes-must-be-upgrade-safe-for-running-deployments.md) and is
not repeated here — it is one rule, reported once.

## Evidence

- #64339 — "Add missing fields in schema file": values in `values.yaml` but not the
  schema back-filled; the canonical *two files must move together* instance.
- #65409 — a schema entry that did not match the value's real shape rejected valid
  user configuration.
- #61915 / #62030 / #64730 / #65027 / #65056 — the `workers.celery.*` /
  `workers.kubernetes.*` series: a large reorganisation delivered as small, additive,
  individually-reviewable value additions rather than one sweeping rename.
- #63659 then #66671 — the deprecate-then-remove cycle for the `workers` section, the
  removal landing with the chart 2.0 major.
- #68043 / #68036 — ingress and SecurityContext removals held to a major version, each
  with a `significant` newsfragment giving the old → new mapping.
- #64559 — consistency work done within the contract rather than by retyping keys.
