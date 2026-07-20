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

The Dag File Processor sits where Airflow meets the machine it runs on: it reads
files from bundles on a filesystem, forks processes, imports modules, and does it
all again on an interval. Every one of those touchpoints attracts a configuration
option, and each proposal is locally reasonable — a knob for the file and folder
permissions a bundle writes with, a list of modules to pre-import so parsing is
faster, a threshold to tune the loop.

Two kinds of these are refused, for different reasons that converge on the same
outcome.

The first is a concern the deployment already owns. File and directory permissions
under a bundle are set by the umask of the process and the group membership of the
users involved; an operator running impersonation sets a group-writable umask and
puts the impersonated users in the Dag processor's group, and gets the intended
result with no Airflow code at all. Two new options in their place would have
codified a workaround into the configuration space, left the concurrency cost they
were meant to address unsolved, and — because a configuration option is a
compatibility surface — outlived the workaround by several releases. The proposal
was withdrawn in favour of documenting the deployment practice.

The second is a concern the architecture is in the middle of removing. Pre-importing
`airflow.*` modules to cut per-file parse cost is a real optimisation today and
close to meaningless after Task-SDK isolation completes, when the only modules a Dag
can import from `airflow.` are `airflow.sdk` and its deprecation shims — which the
manager can simply always pre-import. Building the tunable now means shipping,
documenting, and then deprecating an option whose entire justification expires on a
known schedule. The useful residue of that proposal — letting a user name their *own*
modules to pre-load — survives the transition, and that is what the reviewer
redirected toward.

Configuration is the most expensive thing this area can add. Code can be deleted;
an option that a deployment has set in its `airflow.cfg` has to be deprecated,
warned about, and carried. The bar is correspondingly higher than for the code
behind it.

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
  deployments, and does not accumulate options whose reason for existing has expired.
- Operators get answers sooner: a documented umask recipe ships immediately, an
  option ships in a release.
- Some real inefficiencies stay unaddressed for a while, and some contributors are
  redirected from a working patch to a documentation change — a worse outcome for them
  and a better one for every deployment that would otherwise inherit the option.

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

- #60270 — "Add configs to set bundle file and folder permissions": closed by its
  own author after review pointed out that the same effect is available today by
  setting a group-writable umask and putting impersonated users in the Dag processor's
  group, that two options acknowledged as temporary would pollute the configuration
  space until a permanent solution arrived, and that they would not address the
  concurrency overhead they were motivated by. The PR also raised changing default
  permissions, which would have silently altered access control for every existing
  installation on upgrade.
- #58890 — pre-loading `airflow` modules in the Dag processor: declined as an
  investment in a path that Task-SDK isolation removes, since Dags will be able to
  import only `airflow.sdk` and its deprecation shims, which the manager can always
  pre-import; the reviewer redirected to a user-specified module preload list, and the
  author closed the PR agreeing the direction.
