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

# ADR-0015: Per-Dag Source in the Lang-SDK Bundle Artifact

## Status

Proposed

> **Note:** The TypeScript SDK is the reference implementation, merged in
> [apache/airflow#73723](https://github.com/apache/airflow/pull/73723). Go and Java adoption, and
> the Airflow-core plumbing that surfaces this source in the UI Code tab, are follow-up work.

## Context

A native Lang-SDK Dag is authored entirely in the SDK's language, with no Python `@task.stub` file
behind it ([ADR-0010](0010-native-dag-processing.md)). Its source is therefore the only source there
is to show in the Code tab.

[ADR-0003](0003-pure-java-dags.md) packed one `.java` file into the JAR, named by the
`Airflow-Java-SDK-Dag-Code` manifest attribute — a single source per bundle. That is enough only
while a bundle defines one Dag in one file. A real native bundle defines several Dags across several
files: one entry module imports a module that constructs a Dag, and a single embedded source then
shows the entry file for a Dag whose `new Dag(...)` lives in an imported module. Every Dag would show
the same, mostly-unrelated file.

[ADR-0006](0006-no-lang-sdk-source-display.md) settled the neighbouring case — a **mixed-language**
Dag (Python owns the Dag, the Lang SDK only supplies task handlers) shows only its Python file — and
deferred the native case to "the normal single-file path". [ADR-0010](0010-native-dag-processing.md)
then flagged the gap directly: `get_source_code` is abstract, "a native Lang-SDK Dag has no Python
source to return. What it should return, and how that squares with ADR-0006, is not settled here."
This ADR settles it: what a native Lang-SDK bundle stores for display, and what a reader returns per
Dag.

## Decision

### 1. Store each Dag-defining file, attributed per Dag, best effort

The packer records, per native Dag, the source file the Dag was declared in, and embeds that file
verbatim. The mapping is metadata (`dag_source_paths: {dag_id: path}` in the TypeScript bundle); the
files are embedded regions alongside the compiled artifact.

Attribution is **best effort**, and it covers the shapes authors actually use. A `new Dag(...)` at a
module's top level — directly, in a `for` loop, or in a factory the module calls while it evaluates
(before or after an `await`) — is recorded against the file whose module body was running when it was
constructed. Dynamically generated Dags are therefore attributed like any other and are fully
supported. The one shape it cannot attribute is a Dag constructed *after* its module has finished
evaluating — from a detached callback such as `setTimeout` or a floating `.then` — which has no entry
and is handled by the fallback below rather than by guessing. That pattern is non-idiomatic for a Dag
file.

### 2. De-duplicate by path

Source content is stored **once per unique file path**. Several Dags declared in one file all map to
that one path, and the file's bytes appear once in the artifact, never once per Dag. The per-Dag map
is `dag_id → path`; the stored content is keyed by `path`. This is the property every SDK must
preserve: a bundle with twenty Dags in two files embeds two files, not twenty.

### 3. Always embed the entrypoint, as the fallback

The entry file passed to the packer is **always** embedded and named (`entrypoint_path` in the
TypeScript metadata), whether or not it declares a Dag. It is the fallback source: a reader asked for
a native Dag that has no attributed file of its own returns the entrypoint rather than nothing, so a
Dag the packer could not tie to a file still shows the file that assembles the bundle. This restores
the guarantee the single-source format ([ADR-0003](0003-pure-java-dags.md)) gave — there is always *a*
source — without giving up per-Dag attribution for the Dags that have it (which is the common case,
generated Dags included).

### 4. Mixed-language Dags carry no Lang-SDK source

A Dag the bundle only supplies task handlers for is owned by Python, which owns its Code tab
([ADR-0006](0006-no-lang-sdk-source-display.md)). The bundle stores no source for it, and a reader
returns nothing — never the entrypoint — so the caller shows the Python file. The reader distinguishes
the two by whether the caller asks for a specific Dag's source at all: a native Dag is looked up (and
falls back to the entrypoint), a Python-owned Dag is not looked up and yields nothing.

### 5. Every Lang SDK follows this

TypeScript is the reference ([#73723](https://github.com/apache/airflow/pull/73723)). The Go and Java
packers adopt the same shape — per-Dag source attribution, de-duplication by path, and an
always-embedded entrypoint fallback — so a bundle reader treats every language the same way and the
Code tab behaves identically regardless of which SDK produced the artifact.

## What a reader returns, per Dag

| the Dag is… | reader returns |
|---|---|
| native, with an attributed file (the common case — includes loop/factory-generated Dags) | that file's source |
| native, but the packer could not attribute a file (a detached-callback construction) | the entrypoint source (fallback) |
| mixed-language (Python-owned) | nothing — Python owns the Code tab |

## Consequences

- A native bundle's Code tab shows the file each Dag was actually declared in, not the entry file for
  all of them, and a bundle with many Dags in few files stays small (dedup).
- Every Lang-SDK packer grows a source-attribution and de-duplication step, and the coordinator /
  `DagImporter` reader grows a per-Dag lookup with an entrypoint fallback. The behaviour is uniform
  across languages, so the core side that eventually surfaces it (`get_source_code` →
  `DagCode` → the Code tab, deferred to [ADR-0010](0010-native-dag-processing.md)'s open question and
  future work) sees one contract.
- Generated Dags (built in a loop or a factory during module evaluation) are attributed to their
  generator file like any other Dag — they are fully supported, not a fallback case. Attribution
  being best effort only bites a Dag built in a detached callback after its module finished, which
  then shows the entrypoint — a real file in the bundle rather than a wrong one. This mirrors the
  source view already being best effort for Python factory-function Dags
  ([ADR-0006](0006-no-lang-sdk-source-display.md), "Why Not" #3).

## References

- [ADR-0003](0003-pure-java-dags.md) — the single-source packing precedent this generalises.
- [ADR-0006](0006-no-lang-sdk-source-display.md) — mixed-language Dags show only the Python file.
- [ADR-0010](0010-native-dag-processing.md) — native Dag processing; the `get_source_code` gap this settles.
- [apache/airflow#73723](https://github.com/apache/airflow/pull/73723) — the TypeScript reference implementation.
