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

# 4. Vendor SDK initialisation is centralised in the shared library, not in consumers

Date: 2026-07-20

## Status

Accepted

## Context

A scheduler, Dag processor, triggerer, worker, and their subprocesses are
separate OS processes with separate memory. No central constructor builds a tracer
or metrics client once for everyone; every process configures the vendor SDK for
itself, at its own start-up. The wrapper classes in `shared/observability/` look,
in one file, like pointless indirection over `opentelemetry.*`; read across
processes, they are the only place the per-process SDK setup is written down once.
Import the vendor SDK directly and each process initialises it differently —
different resource attributes, sampling, exporter config, one silently
unconfigured — telemetry simply wrong in a way no diff or single-distribution test
catches.

This came to a head when a maintainer proposed removing the tracing abstraction as
obfuscatory (#62154). The metaclass indirection and most debug spans were agreed
worth deleting and landed separately, but reaching for `opentelemetry.*`
throughout was pushed back on: the file holding SDK initialisation is load-bearing
and cannot go without first saying what initialises the SDK in each process
instead. A second property falls out: consumers may link different versions, so the
library is one place to absorb a vendor API change (as the structlog and
Datadog-timer fixes did).

## Decision

Cross-process SDK setup lives in the shared library and consumers do not
reimplement it:

- **Consumers obtain configured objects from the shared library** — a tracer, a
  logger, a stats client — rather than standing up their own. What is centralised
  is **initialisation**, not the vendor API: `trace.get_tracer(...)`, propagator
  use, `StatusCode`, span attributes and the like are ordinary application code
  and appear at a couple of dozen sites across `airflow-core` and `task-sdk`
  today. It is `TracerProvider` / `MeterProvider` construction, exporter and
  sampler wiring, and resource attributes that belong to the library alone.
  Reading the already-configured provider (as
  `providers/common/ai/.../observability.py` does, to nest its spans under the
  task span without installing anything) is the correct pattern, not a breach.
- **The per-process initialisation path is written once**, in the library, and
  is reusable by every process type; a consumer that needs different behaviour
  gets it through configuration, not a second initialisation site.
- **Vendor API changes are absorbed in the library**, so consumers linking
  different versions of it are insulated from the vendor's release cadence.
- **Removing one of these wrappers is a design change, not a cleanup.** State
  what performs the per-process initialisation afterwards, and how each process
  type reaches it, before deleting the abstraction.
- **The rule is bounded by the multi-process argument that produced it.** What
  belongs in the library is what every process must do *identically* and would
  otherwise do differently — SDK setup. Behaviour used by a single consumer, or
  behaviour whose divergence between processes is harmless, stays with that
  consumer; moving it into `shared/` widens a cross-distribution surface that is
  hard to withdraw (see ADR 0005) and adds a version-skew problem that did not
  exist. "It could be shared" is not a reason; "these processes must agree and
  today they do not" is.

## Consequences

Telemetry is configured identically across every process, and a vendor SDK change
is absorbed in one library. The cost is regularly mistaken for a defect: the
wrappers add a layer hard to justify from a single file, obscure the vendor API
when debugging a span, and bottleneck any capability the wrapper does not yet
expose. Some indirection really was excessive — which is why the narrower deletions
proceeded while the wholesale removal did not.

A change **violates** this decision when it:

- installs or configures a `TracerProvider`, `MeterProvider`, exporter, sampler,
  or resource-attribute set from a consumer distribution rather than from the
  shared library's initialisation path (using the vendor *API* — obtaining a
  tracer, setting span status, propagating context — is normal and is not a
  violation);
- adds a second initialisation site for an SDK the library already initialises;
- deletes a shared wrapper over a vendor SDK without specifying the replacement
  per-process initialisation path.

## Evidence

- #62154 — proposal to remove the tracing abstraction and use `opentelemetry.*`
  throughout; closed unmerged after the multi-process initialisation argument,
  agreed deletions taken separately.
- #61897 — accompanying span-lifecycle discussion on cross-process span parenting
  and SDK config via environment variables; closed unmerged.
- #56187 — traces and metrics consolidated under the common observability package.
- #62127 — airflow-core imports refactored out of `shared/stats` so the library
  stands alone.
- #63932 — `DualStatsManager` and duplicate `Stats` interfaces removed, collapsing
  to a single path.
- #69202 — mypy fix for the Datadog timer wrapped by the shared `Timer`: the kind
  of vendor-shape change the library absorbs for every consumer.
