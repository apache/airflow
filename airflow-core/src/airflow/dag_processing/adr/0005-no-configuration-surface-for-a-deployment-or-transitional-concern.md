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

# 5. No configuration surface for a deployment or transitional concern

Date: 2026-07-20

## Status

Accepted

## Context

The Dag File Processor reads bundle files, forks processes, and imports modules on
an interval, and every touchpoint attracts a locally-reasonable configuration
option. Two kinds are refused.

The first is a concern the **deployment already owns**. Bundle file/directory
permissions are set by the process umask and group membership: an operator running
impersonation sets a group-writable umask and puts impersonated users in the
processor's group, with no Airflow code. Two options in their place would codify a
workaround into the (compatibility-surface) configuration space, outlive it by
releases, and leave the concurrency cost unsolved — so the proposal was withdrawn
for documentation.

The second is a concern the architecture is **removing**. Pre-importing `airflow.*`
modules to cut parse cost is meaningless after Task-SDK isolation, when the only
importable `airflow.` surface is `airflow.sdk` and its shims — which the manager can
always pre-import. Building the tunable now ships, documents, and then deprecates an
option whose justification expires on a known schedule. The useful residue —
letting a user name their *own* preload modules — survives, and that is where the
reviewer redirected. Configuration is the most expensive thing this area can add:
code can be deleted, but an option set in an `airflow.cfg` must be deprecated,
warned about, and carried.

## Decision

**An option is added here only when the behaviour cannot be achieved by deployment
configuration and will still be needed after the transitions currently in flight.**

- **Check the deployment layer first** — umask, group membership, filesystem
  permissions, container settings, the bundle's own configuration. If an operator can
  get the behaviour today without Airflow code, the deliverable is documentation.
- **Do not encode a temporary workaround as configuration.** An option introduced to
  bridge to a permanent solution outlives the bridge; if it must exist, its removal
  needs a linked tracking issue at the definition site, per the repository's
  deferred-work rule.
- **Do not build a configuration surface around a parse cost Task-SDK isolation is
  removing.** The refused case was a *tunable* — an operator-visible list of
  `airflow.*` modules to pre-import — whose justification expires when the only
  importable surface is `airflow.sdk` and its shims, which the manager can then
  always pre-import unconditionally. Optimising the code path itself is a different
  matter: the deprecation shims stay on the import path of every pre-SDK Dag for
  several releases, so making shim resolution cheaper is legitimate work. The
  objection is to the knob, not to the speed-up.
- **Prefer a user-supplied specification to an Airflow-supplied default** where a
  need is real but its content is deployment-specific — a list the operator names
  survives an architectural change that a hard-coded heuristic does not.
- **A defaults change is a compatibility event.** Altering a default that governs
  permissions or isolation silently weakens or tightens existing installations on
  upgrade and needs to be argued as such, not slipped in with the feature.

## Consequences

- The configuration space stays proportionate to what genuinely varies between
  deployments, without options whose reason has expired.
- Operators get answers sooner: a documented umask recipe ships immediately; an
  option ships in a release.
- Some inefficiencies stay unaddressed and some contributors are redirected from a
  working patch to documentation — worse for them, better for every deployment that
  would otherwise inherit the option.

A change **violates** this decision when it:

- adds a configuration option for behaviour reachable through umask, group
  membership, filesystem permissions, or container configuration;
- describes its own option as temporary, transitional, or pending a permanent
  solution, without a linked tracking issue at the definition site;
- adds a **configuration option** whose stated justification is parse-time or
  import-time cost arising from Dags importing `airflow.*` modules outside
  `airflow.sdk` — a knob that expires with the transition. A code-level speed-up of
  that same path, with no option attached, is not a violation;
- hard-codes a deployment-specific list where the operator could supply it;
- changes a default governing file permissions, isolation, or resource limits
  without stating the effect on existing installations at upgrade.

## Evidence

- #60270 — closed by its own author: the umask+group recipe already achieves it, two temporary options would pollute config, they didn't address the concurrency cost, and the default-permission change would silently alter access control on upgrade.
- #58890 — pre-loading `airflow` modules declined as investment in a path Task-SDK isolation removes; redirected to a user-specified preload list, author closed agreeing.
