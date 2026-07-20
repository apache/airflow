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

`chart/values.yaml` is not a defaults file — it is the chart's **API**. Every
Airflow deployment on Kubernetes that uses the official chart has a values file
(or a `--set` list, or an Argo/Flux/Terraform manifest) written against the key
names, nesting, and types in it. Those files live outside this repository, are
never seen by a reviewer, and are applied unchanged at the user's next
`helm upgrade`.

`chart/values.schema.json` is what actually *enforces* that contract. Helm
validates the user's merged values against it before rendering anything, and the
schema is heavily locked down — `additionalProperties` appears roughly 400 times
— so an unrecognised key is a hard failure, not a warning. A value that exists
in `values.yaml` but is missing from the schema is therefore not "partially
supported": it is unusable, and the user gets a validation error rather than the
feature. The same schema is the source for the generated
`chart/docs/parameters-ref.rst`, and `chart/values_schema.schema.json` is a
meta-schema that requires every parameter to carry a `default` and a
`description`, and every top-level property an `x-docsSection`. The
`chart-schema` prek hook wires all of this together.

This makes the failure mode asymmetric. *Adding* a key with a
behaviour-preserving default costs an existing user nothing. *Renaming,
removing, retyping, or re-nesting* one costs every user who set it a broken
upgrade, with an error message that names the rejected key but not its
replacement. The chart is also released independently and users pin chart
versions, so the fix is not "wait for the next Airflow release" — it is "publish
a new chart version and hope the user reads the release notes".

The recurring pressure is tidiness: a value sits at the wrong level, or has an
awkward name, or duplicates a sibling, and the clean fix is obviously to move
it. The decision below is what forces that cleanup onto the deprecation path
instead of into a patch release.

## Decision

`values.yaml` and `values.schema.json` are treated as a single public,
versioned contract, and the two are always changed together. Concretely:

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

- Users can upgrade the chart within a major version without editing their
  values file, which is the property that makes the chart safe to pin and
  automate.
- Cleanups are slower and noisier: a rename is a deprecation PR, a wait, and a
  removal PR in a major version — as the `workers` section reorganisation and
  the ingress/security-context removals show. That cost is deliberate.
- The schema grows large and duplicative relative to `values.yaml`, and keeping
  them in sync is manual work the prek hook only partially catches.

A change **violates** this decision when, in `chart/`, it:

- adds or modifies a value in `values.yaml` without the corresponding
  `values.schema.json` entry (or vice versa), or omits the `default` /
  `description` / `x-docsSection` the meta-schema requires;
- renames, removes, re-nests, or retypes an existing value outside a major chart
  version, or without a preceding deprecation period and a `significant`
  newsfragment describing the migration;
- introduces a new value whose default changes what the chart renders for an
  existing release;
- places a per-component knob outside the component's existing section, or names
  it inconsistently with its siblings, creating a second way to express the same
  thing.

A reviewer should reject any change that alters the meaning of an existing key,
or that lands a value in only one of the two files.

## Evidence

- #64339 — "Add missing fields in schema file": values that existed in
  `values.yaml` but not in `values.schema.json` had to be back-filled; the
  canonical instance of the *two files must move together* rule.
- #65409 — "Fix Helm chart image volume schema validation": a schema entry that
  did not match the value's real shape rejected valid user configuration.
- #61915 / #62030 / #64730 / #65027 / #65056 — the `workers.celery.*` and
  `workers.kubernetes.*` series: a large reorganisation delivered as many small,
  additive, individually-reviewable value additions rather than one sweeping
  rename.
- #63659 — "Add missing deprecation warnings for workers section", then #66671 —
  "Remove `workers` section deprecation": the deprecate-then-remove cycle in
  full, with the removal landing alongside the chart 2.0 major.
- #68043 — "Remove deprecated ingress options from chart" and #68036 — "Remove
  deprecated SecurityContext from chart": removals held back to a major version,
  each carrying a `significant` newsfragment with the old → new key mapping.
- #64559 — "Improve consistency of values.yaml & misc": consistency work done
  within the contract rather than by retyping existing keys.
