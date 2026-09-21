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

# ADR-0011: Bundle Metadata — Retiring the Build-Time Inventory, Converging on a Cache Digest

## Status

Proposed

## Context

[ADR-0010](0010-persisted-task-handler-bindings.md) resolves a stub task to its artifact during Dag
processing and persists the result, so nothing searches for an artifact at execution time. Two
consequences land on the artifact format: the build-time Dag inventory loses its only purpose, and
the Dag processor gains a new need — a stable value it can compare cheaply to decide whether
re-validation is required.

Today that artifact carries a build-time inventory of the Dag and task ids it exposes. This is what
`airflow-go-pack` emits, and what the published schema requires
([`$id`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/airflow-metadata.schema.json#L3),
[`required`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/airflow-metadata.schema.json#L7)):

```yaml
airflow_bundle_metadata_version: "1.0"
sdk:
  language: "go"
  version: "0.1.0"
  supervisor_schema_version: "2026-06-16"
source: "main.go"
dags:                       # <-- frozen when the artifact was built
  etl:
    tasks:
      - "extract"
      - "transform"
  reporting:
    tasks:
      - "publish"
```

The `dags` mapping is required and must be non-empty. TypeScript emits the same shape under the key
`task_handlers`; Java emits no such document at all.

The three SDKs are not in the same place, and the differences matter more than the shared spec
suggests.

**Go** packs a binary trailer: `binary || source || metadata || trailer`, with a 64-byte
`AFBNDL01` trailer whose normative layout lives at
[`bundlefooter trailer layout`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/bundlefooter/footer.go#L24-L31). Its `binary_sha256` covers **`[0, source_start)` —
the binary region only** ([`Append`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/bundlefooter/footer.go#L98-L101), [`Read`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/bundlefooter/footer.go#L173), [`hashRegion`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/bundlefooter/footer.go#L269-L280)). The source and metadata regions carry
no digest at all. The manifest key is `dags` ([`Manifest`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/airflowmetadata/airflowmetadata.go#L35-L51)).

**TypeScript** has a wholly separate, text-comment format with its own spec
([`ts-bundle-spec.rst`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/ts-bundle-spec.rst)): a `//# airflowBundle=` layout header on line 1, then
`//# airflowMetadata=` raw UTF-8 JSON, then source, then code
([`EMBEDDED_METADATA_PREFIX`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/cli/bundle-encoder.ts#L48-L52), [`encodeBundle`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/cli/bundle-encoder.ts#L81-L88)). It shares no magic, no ordering, no serialization
and no code with Go. It already **hashes all three regions independently**
([`encodeBundle digests`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/cli/bundle-encoder.ts#L100-L104)) and verifies all three on read
([`_compute_stable_digests`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/node/_bundle_reader.py#L293-L318), [`_verify_integrity`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/node/_bundle_reader.py#L321-L361)). And it emits
**`task_handlers`**, not `dags`, with a rationale that anticipates ADR-0010 exactly
([`BundleManifest`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/coordinator/manifest.ts#L28-L31)): *"a TypeScript bundle provides handlers for Dags
declared elsewhere, not Dag definitions."*

**Java** has no packer and no artifact metadata beyond two manifest attributes — `Main-Class`
([`Main-Class`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/java-sdk/plugin/src/main/kotlin/org/apache/airflow/sdk/plugin/AirflowSdkPlugin.kt#L113)) and
`Airflow-Supervisor-Schema-Version` ([`Airflow-Supervisor-Schema-Version`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/java-sdk/plugin/src/main/kotlin/org/apache/airflow/sdk/plugin/AirflowSdkPlugin.kt#L194-L195)). No inventory, no digest, no source region.
`Airflow-Java-SDK-Metadata` and `Airflow-Java-SDK-Dag-Code` exist only in
[ADR-0003](0003-pure-java-dags.md) prose and were never built.

So the published schema — which requires `dags` and carries `1.0` in its `$id`
([`$id`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/airflow-metadata.schema.json#L3), [`required`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/airflow-metadata.schema.json#L7), [`dags`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/airflow-metadata.schema.json#L43-L50)) — currently describes Go and no one else.
TypeScript emits a document that cannot validate against it while still declaring
`airflow_bundle_metadata_version: "1.0"`; Java emits nothing schema-shaped. That split predates this
work and has to be reconciled regardless; ADR-0010 forces the question rather than creating it.

## Decision

### The artifact carries no Dag or task identifiers

The inventory is not renamed, softened, or made optional. It is **removed**, and no replacement is
added under any name. After this change an artifact contains exactly three things:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  compiled artifact     the executable, JAR, or bundled code             │
│  entrypoint source     the authored source, verbatim, for display       │
│  metadata              only what is needed to launch and to trust       │
└─────────────────────────────────────────────────────────────────────────┘
```

and the metadata region is reduced to this:

```yaml
airflow_bundle_metadata_version: "2.0"
sdk:
  language: "go"                          # this is an Airflow Lang-SDK artifact
  version: "0.1.0"
  supervisor_schema_version: "2026-06-16" # how to speak to it
source: "main.go"                         # display name only
digests:
  integrity: "<sha256 of the executable region>"
  cache: "<sha256 of all logical content>"
# no dags:
# no task_handlers:
```

No `dag_id` appears anywhere in it, and no `task_id`. That holds for **both** roles: a mixed-language
artifact contributing task handlers, and a native Dag artifact. The artifact declares what it *is*
and how to launch it; what it *contains* is discovered by asking it, and recorded by
[ADR-0010](0010-persisted-task-handler-bindings.md).

The reasoning is the same in both roles and is not specific to dynamic rendering. An identifier
recorded at build time is a claim about runtime behaviour made by a process that cannot observe it.
It is produced by executing the freshly built artifact against an in-memory registry recorder
([`collectManifest`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/pkg/execution/metadata.go#L105-L112); [`readBundleManifest`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/cli/pack.ts#L117-L124)), so it is accurate
only while registration depends on nothing the build environment lacks. Dynamic Dag rendering — Dag
ids generated from data the artifact reads at runtime — is the case that makes the claim plainly
false, and today it cannot be packed at all, because an empty inventory is fatal in both packers
([`empty-dags check`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/cmd/airflow-go-pack/pack.go#L166-L168), [`empty-handlers check`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/cli/pack.ts#L226-L230)). But a claim that
happens to be true today is still a second source of truth that can drift from the runtime tomorrow.
Keeping it as advisory metadata would preserve exactly that drift while removing the only thing that
would have caught it — a consumer that fails when it is wrong.

So both the `dags` mapping (Go, and the published schema) and the `task_handlers` mapping
(TypeScript) go, along with the two fatal-empty checks that guarded them.

**This supersedes recent work, not dead code.** The Node coordinator routes by `dag_id` through
`task_handlers` today ([`_build_execute_task_command`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/node/coordinator.py#L125-L127), reading
via [`_parse_bundle_metadata`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/node/_bundle_reader.py#L379-L383)), landed in #73126. The
capability survives — its input moves from a build-time guess to a parse-time observation — but the
mechanism is replaced. The Executable coordinator's `_dag_ids` / `_Bundle.find` pair
([`_dag_ids`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py#L249-L254), [`_Bundle.find`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py#L296-L322)) goes the same
way. Java loses nothing, because Java never had it: it matches on `Main-Class` and discards
`what.dag_id` entirely ([`_build_execute_task_command`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/java/coordinator.py#L207-L215)), with
its own docstring conceding that "it may be nondeterministic which one ends up being executed"
([`JavaCoordinator`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/java/coordinator.py#L180-L183)).

Candidate *detection* is unaffected and still requires executing nothing — it is exactly the "this is
an Airflow Lang-SDK artifact" marker doing its job: the `AFBNDL01` trailer magic for Go
([`Magic`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/bundlefooter/footer.go#L57)), the `.min.mjs` suffix plus a valid layout header for
TypeScript ([`BUNDLE_SUFFIX`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/node/coordinator.py#L43)), a `Main-Class` attribute
for Java.

The entrypoint source region stays, for display. That sits awkwardly against
[ADR-0006](0006-no-lang-sdk-source-display.md), which rules out Lang-SDK source display for
mixed-language Dags, while TypeScript landed source embedding in #73127 and Python-side
`read_bundle_source` exists with no production caller. Whether the region is displayed, and in which
role, is ADR-0006's question and is not reopened here; this ADR only records that the region is
retained.

### A cache digest, distinct from the integrity hash

The Dag processor must answer "has this artifact changed since I validated it" tens of times a
minute. That is a different question from "is this artifact intact", and the two must not be
conflated:

| | integrity hash | cache digest |
|---|---|---|
| answers | is this artifact intact? | has this artifact changed? |
| checked | at task execution, per launch | during Dag processing, per parse |
| how | **computed** and compared | **read** |
| must cover | the executable region, at minimum | all logical content |

Reading a stored value cannot substitute for computing one — a truncated or half-downloaded artifact
still reports a plausible stored digest. Every existing integrity check stays exactly where it is and
keeps computing.

**The digest is opaque and coordinator-defined.** It is not "SHA-256 of the file". Each runtime
supplies a value that is stable across rebuilds changing nothing and differs across rebuilds changing
something; consumers compare for equality and interpret nothing. This must be stated wherever the
column is documented, because the three definitions below genuinely differ and a later change
unifying them on file bytes would silently invalidate every cached binding on every rebuild.

### Per-SDK mechanics

No artifact can contain its own complete hash — writing the digest changes the bytes it covers — so
each scheme excludes the field holding it.

**TypeScript — already done.** The layout header carries a `sha256` for each of the three regions
([`VerifiedByteRange`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/cli/bundle-encoder.ts#L69-L79), [`encodeHeader`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/cli/bundle-encoder.ts#L214-L220)), all outside the header itself. The cache digest
is derived from those three values. No format change, no packer change.

**Go — one new trailer field.** Today nothing covers the source and metadata regions, so a metadata
change is invisible to `binary_sha256`. A whole-content digest over
`[0, metadata_start + metadata_len)` — everything except the trailer — is added. `binary_sha256`
stays unchanged, for integrity.

The trailer must grow: `TrailerSize` is 64 with only 12 reserved bytes
([`bundlefooter trailer layout`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/bundlefooter/footer.go#L24-L31), [`TrailerSize`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/bundlefooter/footer.go#L46-L53)), which cannot hold a 32-byte digest. Growing
it has a compatibility trap. The reader seeks to `file_size - FOOTER_SIZE` and checks the magic at
`[56:64]` of that window ([`_Footer.read`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py#L94-L98)),
so against a 96-byte trailer an old reader lands 32 bytes in, fails the magic check, and treats the
file as *not a bundle at all* — surfacing as "cannot find executable bundle containing dag_id=…"
([`_Bundle.find`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py#L320)) rather than the unsupported-version error the format already knows how to raise ([`_Footer.read`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py#L101-L105)).
Lay the new trailer out so an old reader still lands on a field it rejects loudly, or accept that a
new-format artifact on an old runtime looks like a missing one.

**Java — an entry-set manifest attribute.** `Airflow-Bundle-Digest`, computed as SHA-256 over a
canonical serialisation of `(entry name, entry digest)` for every entry except the manifest, sorted
by name.

Entry-set rather than file bytes, deliberately: a JAR is a zip, and its bytes vary with timestamps,
entry order and compression level even when nothing meaningful changed. A byte digest would
invalidate the cache on nearly every rebuild, which is the cost this digest exists to avoid.
`jarsigner` solves the same problem the same way — per-entry `SHA-256-Digest` plus
`SHA-256-Digest-Manifest` — but requires a keystore.

The Gradle plugin writes it alongside the attribute it already stamps
([`Airflow-Supervisor-Schema-Version`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/java-sdk/plugin/src/main/kotlin/org/apache/airflow/sdk/plugin/AirflowSdkPlugin.kt#L194-L195)). Maven
users reproduce it by hand, as they already must for `Airflow-Supervisor-Schema-Version`
([`Maven shade recipe`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/docs/authoring-and-scheduling/language-sdks/java.rst#L765)).

**Any SDK, as a fallback.** A coordinator with no stored digest computes one at read time. Correct
everywhere, at the cost of reading the artifact on every parse — which is why the stored field exists
for runtimes that can carry one, and why Java's absence of one is worth closing.

### What this retracts

Two accepted Go-SDK ADRs state the removed requirement outright and are superseded on this point:

- [`discovery without execution`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/adr/0004-self-contained-executable-bundle.md#L43-L50) — "**Discovery without execution** — the
  scanner must be able to read `dag_id`/`task_id` and the SDK language/version from a bundle on disk
  without running the binary."
- [`footer required`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/adr/0005-retire-go-edge-worker.md#L58-L62) — "The metadata footer on a packed bundle is
  required for coordinator discovery."

Discovery-without-execution is relocated rather than weakened: nothing needs to learn a `dag_id` from
an artifact any more, because ADR-0010 records the binding when the Dag is processed. The footer
stays required — for candidate detection, the supervisor schema version, integrity, and now the cache
digest.

## Consequences

- Dynamic Dag rendering becomes packable. The fatal empty-inventory checks disappear with the
  requirement they enforced.
- The canonical schema goes to `2.0` and drops the identifier mapping entirely, which also resolves a
  split in which TypeScript bundles have never validated against the schema they claim to conform to:
  there is no longer a key for them to disagree on. Readers accept `1.x` and ignore `dags` /
  `task_handlers` for one release.
- Two spec documents converge or stay explicitly divergent. [`executable-bundle-spec.rst`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/executable-bundle-spec.rst)
  and [`ts-bundle-spec.rst`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/ts-bundle-spec.rst) describe genuinely different container formats but should
  share one metadata schema; today only the former references it.
- The Go trailer format changes and its version is bumped. TypeScript needs no format change. Java
  gains its first content digest.
- Node's build-time `dag_id` routing (#73126) and the Executable coordinator's `_dag_ids` lookup are
  both replaced by the persisted binding. Java's `Main-Class` first-match-wins — the one genuinely
  nondeterministic selector left — is replaced too, which is a fix rather than a migration.
- The packer no longer needs to execute the artifact at all. `supervisor_schema_version` is a
  compile-time constant of the SDK, so with the inventory gone the introspection round trip goes with
  it — taking with it the cross-compile host-arch sidecar build
  ([`buildIntrospectionSidecar`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/cmd/airflow-go-pack/pack.go#L136-L151)), the rule that `--executable` with a
  non-host-runnable binary is a hard error ([`--executable guard`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/cmd/airflow-go-pack/pack.go#L384-L391)), and the scan-stdout-for-a-sentinel hack
  TypeScript needs because user import-time logging pollutes stdout ([`readBundleManifest`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/cli/pack.ts#L138-L152)).
  Whether to keep `--airflow-metadata` and `airflow-go-pack inspect` as debugging affordances is left
  open.
- A byte-identical rebuild produces no new record. A deployment wanting each deploy distinguishable
  regardless should use the bundle version, not the artifact.
- The digest's definition differs per SDK by design, and will be "fixed" unless that is written down
  next to the column.
- Three directory walkers still exist ([`_walk_files`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/_bundle_metadata.py#L67),
  [`_walk_executables`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py#L270), [`_walk_jars`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/java/coordinator.py#L59)), with a standing note that the other two
  should move onto the shared one ([`walk_files`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/_bundle_metadata.py#L61-L62)). ADR-0010 removes the need for all
  three on the execution path, which makes that consolidation cheap to finish.

## References

- [ADR-0010](0010-persisted-task-handler-bindings.md) — what consumes the digest, and why the inventory is unused
- [ADR-0003](0003-pure-java-dags.md) — the Java build-time inventory that was designed and never built
- [`go-sdk ADR-0001`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/adr/0001-bundle-packing-options.md) — why the inventory was runtime-introspected, not AST-scanned
- [`go-sdk ADR-0004`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/adr/0004-self-contained-executable-bundle.md) — the Go artifact format, partially superseded here
- [`go-sdk ADR-0005`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/adr/0005-retire-go-edge-worker.md) — the footer-required statement, clarified here
- [`footer.go`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/bundlefooter/footer.go) — the normative Go trailer layout
- [`bundle-encoder.ts`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/cli/bundle-encoder.ts) — the TypeScript container format and its per-region digests
- [`executable-bundle-spec.rst`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/executable-bundle-spec.rst) — the Go-shaped spec to amend
- [`ts-bundle-spec.rst`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/ts-bundle-spec.rst) — the TypeScript spec, which uses `task_handlers` already
- [`airflow-metadata.schema.json`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/airflow-metadata.schema.json) — the schema to republish as `2.0`
