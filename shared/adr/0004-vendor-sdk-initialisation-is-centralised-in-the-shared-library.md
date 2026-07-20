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

Airflow is not one program. A scheduler, a Dag processor, a triggerer, a worker,
and the subprocesses each of them spawns are separate operating-system processes
with separate memory. There is no central constructor that builds a tracer or a
metrics client once and hands it to everyone; every process has to configure the
vendor SDK for itself, from configuration it reads itself, at its own start-up.

That is the whole reason the observability, logging, and stats libraries exist
in the shape they do. The wrapper classes in `shared/observability/` look, in a
single file, like pointless indirection over `opentelemetry.*` — a tracer that
wraps a tracer. Read across processes, they are the only place the per-process
SDK setup is written down once. Import the vendor SDK directly from consumer
code and each process ends up initialising it slightly differently: different
resource attributes, different sampling, different exporter configuration, one
process silently not configured at all. Nothing crashes. The telemetry is simply
wrong in a way that no diff shows and no test in one distribution catches.

This came to a head when a maintainer proposed removing the tracing abstraction
as obfuscatory. The specific complaints were fair — the metaclass indirection
and most of the debug spans were agreed to be worth deleting, and much of that
deletion did land separately. But the proposal to reach for `opentelemetry.*`
directly throughout the codebase was pushed back on precisely because of the
multi-process argument: the common file holding the SDK initialisation logic is
load-bearing, and the abstraction cannot be removed without first saying what
initialises the SDK in each process instead. The PR did not merge.

The same reasoning drove the moves *into* the shared libraries: the traces and
metrics code was consolidated under a common observability package, and
airflow-core imports were refactored out of `shared/stats` so the library could
stand alone. The direction is consistent — initialisation converges into the
library, and consumers get a configured object rather than a vendor API.

A second property falls out of it: consumers may link different versions of a
shared library. A consumer that imports the vendor SDK directly pins itself to
that SDK's API, and a vendor release then breaks that consumer alone, out of
step with the others. Going through the library gives one place to absorb the
change — which is what the compatibility fixes around structlog and the
Datadog timer wrapper have actually done.

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

Telemetry is configured identically across every Airflow process, and a vendor
SDK change is absorbed in one library rather than in every consumer that touches
it.

The cost is genuine and is regularly mistaken for a defect: the wrappers add a
layer that is hard to justify from any single file, they obscure the vendor API
from someone debugging a span, and they make the shared library a bottleneck for
anyone who wants a capability the wrapper does not yet expose. Some of that
indirection really was excessive, and the pushback that arrived on this point
was partly right — which is why the narrower deletions proceeded while the
wholesale removal did not.

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
  with the agreed subset of deletions taken separately.
- #61897 — the accompanying span-lifecycle discussion, which turned on cross-
  process span parenting and standard SDK configuration via environment
  variables; closed unmerged.
- #56187 — traces and metrics consolidated under the common observability
  package.
- #62127 — airflow-core imports refactored out of `shared/stats` so the library
  stands alone.
- #63932 — removal of the `DualStatsManager` and duplicate `Stats` interfaces,
  collapsing to a single path.
- #69202 — mypy fix for the Datadog timer wrapped by the shared observability
  `Timer`: the kind of vendor-shape change the library absorbs on behalf of
  every consumer.
