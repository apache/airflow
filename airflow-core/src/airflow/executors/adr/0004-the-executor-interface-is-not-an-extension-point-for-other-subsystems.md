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

# 4. The executor interface is not an extension point for other subsystems

Date: 2026-07-20

## Status

Accepted

## Context

`BaseExecutor` and `ExecutorLoader` are attractive places to hang things: the
loader already discovers per-deployment, provider-supplied classes from
configuration and instantiates them at start-up — exactly the machinery any other
pluggable subsystem needs. So adding a method to `BaseExecutor` for DB migrations,
a config shim, or a start-up hook looks like reuse.

It is coupling, and expensive. `BaseExecutor` is implemented by every provider
executor (Celery, Kubernetes, ECS, Batch, Lambda, Edge, out-of-tree), versioned
independently of core; a method added for one concern becomes one all of them must
carry across the core/provider version matrix. It also fuses two lifetimes: DB
managers matter at migration time when no executor need exist, executors matter at
scheduling time — tying the two means a deployment cannot migrate without a
working executor config, and the only way to ship a DB manager is to also ship an
executor. This was worked through on a proposal to add `get_db_manager` with
auto-discovery through `ExecutorLoader` (#60752): the need was real, but the FAB
provider already exposes DB-manager functionality through its own CLI command
group, and that is the seam to follow — provide the manager directly. The same
instinct keeps the loader narrow: validation that crept into executor-adjacent
start-up paths (team existence during Dag validation and CLI parser loading) was
removed, because those paths run constantly and exist to resolve an executor.

## Decision

The executor interface stays about executing tasks:

- **Do not add a method to `BaseExecutor` for a concern that is not task
  execution.** Database managers, migrations, auth wiring, and similar
  subsystems get their own seam — a CLI command group or a provider entry point
  — not an executor hook.
- **Do not extend `ExecutorLoader` into general plugin discovery.** It resolves
  executors; a second discovery mechanism that happens to be convenient here
  belongs where its own subsystem lives.
- **Keep executor lookup and loader-adjacent start-up paths free of unrelated
  validation.** These run on every scheduler loop and every CLI invocation.
- **If a capability legitimately belongs to executors, it goes in
  `BaseExecutor`** with a default that every existing implementation can inherit
  unchanged — the generalise-don't-copy-paste rule still applies within the
  executor concern.

## Consequences

- Provider executors stay implementable against a small, stable surface, and
  subsystems with different lifetimes stay independently deployable — migrate
  without a configured executor, ship a DB manager without shipping one.
- The cost is duplicated plumbing: a subsystem needing per-deployment discovery
  builds its own mechanism, and a contributor arriving with a working
  executor-hook is asked to rewrite it against a seam that may need extending
  first.

A change **violates** this decision when it:

- uses `ExecutorLoader` to discover or instantiate something that is not an
  executor;
- makes a subsystem's availability conditional on an executor being configured
  or loadable;
- adds validation of teams, bundles, or other external state to executor lookup
  or to start-up paths that run on every CLI/scheduler invocation.

Reviewer prompt — the judgement this ADR exists to inform, which the diff frames
but does not answer:

- Is the new `BaseExecutor` method actually about queuing, running, monitoring, or
  adopting tasks? A method name rarely settles this; `get_db_manager` looked like
  executor configuration and was a migration-time concern with a different
  lifetime. Ask what has to be true for the method to be called, and whether an
  executor needs to exist at that moment.

## Evidence

- #60752 — `get_db_manager` on executors via `ExecutorLoader` auto-discovery; closed, reviewers pointed at the FAB provider's CLI seam and objected to more interface complexity.
- #61155 — the successor that landed via the separate route.
- #60596 — the earlier attempt at the same need, superseded.
- #62596 — team-existence validation removed from Dag validation.
- #58067 — team-existence verification removed from CLI parser loading.
