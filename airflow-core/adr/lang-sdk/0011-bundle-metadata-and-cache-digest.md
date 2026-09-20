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

The three SDKs are not in the same place, and the differences matter more than the shared spec
suggests.

**Go** packs a binary trailer: `binary || source || metadata || trailer`, with a 64-byte
`AFBNDL01` trailer whose normative layout lives at
`go-sdk/internal/bundlefooter/footer.go:24-31`. Its `binary_sha256` covers **`[0, source_start)` —
the binary region only** (`go-sdk/internal/bundlefooter/footer.go:98-101, 173, 269-280`). The source and metadata regions carry
no digest at all. The manifest key is `dags` (`go-sdk/internal/airflowmetadata/airflowmetadata.go:35-51`).

**TypeScript** has a wholly separate, text-comment format with its own spec
(`task-sdk/docs/ts-bundle-spec.rst`): a `//# airflowBundle=` layout header on line 1, then
`//# airflowMetadata=` raw UTF-8 JSON, then source, then code
(`ts-sdk/src/cli/bundle-encoder.ts:48-52, 81-88`). It shares no magic, no ordering, no serialization
and no code with Go. It already **hashes all three regions independently**
(`ts-sdk/src/cli/bundle-encoder.ts:100-104`) and verifies all three on read
(`task-sdk/src/airflow/sdk/coordinators/node/_bundle_reader.py:293-318, 321-361`). And it emits
**`task_handlers`**, not `dags`, with a rationale that anticipates ADR-0010 exactly
(`ts-sdk/src/coordinator/manifest.ts:28-31`): *"a TypeScript bundle provides handlers for Dags
declared elsewhere, not Dag definitions."*

**Java** has no packer and no artifact metadata beyond two manifest attributes — `Main-Class`
(`java-sdk/plugin/src/main/kotlin/org/apache/airflow/sdk/plugin/AirflowSdkPlugin.kt:113`) and
`Airflow-Supervisor-Schema-Version` (`:194-195`). No inventory, no digest, no source region.
`Airflow-Java-SDK-Metadata` and `Airflow-Java-SDK-Dag-Code` exist only in
[ADR-0003](0003-pure-java-dags.md) prose and were never built.

So the published schema — which requires `dags` and carries `1.0` in its `$id`
(`task-sdk/docs/airflow-metadata.schema.json:3, 7, 43-50`) — currently describes Go and no one else.
TypeScript emits a document that cannot validate against it while still declaring
`airflow_bundle_metadata_version: "1.0"`; Java emits nothing schema-shaped. That split predates this
work and has to be reconciled regardless; ADR-0010 forces the question rather than creating it.

## Decision

### `task_handlers` becomes the canonical name, and the inventory becomes advisory

TypeScript's naming is correct and is adopted: an artifact in the mixed-language role contributes
task bodies for Dags declared elsewhere, so a mapping called `dags` misdescribes it. The canonical
schema is republished as `2.0` with `task_handlers` in place of `dags`, and Go's manifest follows.

More importantly, the inventory stops being **required** and stops being **load-bearing**. It is no
longer the routing key for any coordinator; ADR-0010's persisted binding is. A packer that cannot
enumerate handlers at build time emits the key empty, or omits it, and this is not an error.

That last clause is the point of the change. The inventory is produced by executing the freshly
built artifact against an in-memory registry recorder (`go-sdk/pkg/execution/metadata.go:105-112`;
`ts-sdk/src/cli/pack.ts:117-124`), so it is correct only when registration depends on nothing the
build environment lacks. Dynamic Dag rendering — Dag ids generated from data the artifact reads at
runtime — violates that by construction, and today it cannot even be packed: an empty inventory is
fatal in both packers (`go-sdk/cmd/airflow-go-pack/pack.go:166-168`,
`ts-sdk/src/cli/pack.ts:226-230`). Those two checks are removed.

Everything else in the metadata stays: `airflow_bundle_metadata_version`, `sdk.language`,
`sdk.version`, `sdk.supervisor_schema_version`, `source`.

**This supersedes recent work, not dead code.** The Node coordinator routes by `dag_id` through
`task_handlers` today (`task-sdk/src/airflow/sdk/coordinators/node/coordinator.py:125-127`, reading
via `task-sdk/src/airflow/sdk/coordinators/node/_bundle_reader.py:379-383`), landed in #73126. That routing is replaced by the persisted
binding, not merely deleted: the capability survives, its input moves from a build-time guess to a
parse-time observation. The Executable coordinator's `_dag_ids` / `_Bundle.find` pair
(`task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py:249-254, 296-322`) goes the same
way. Java loses nothing, because Java never had it — it matches on `Main-Class` and discards
`what.dag_id` entirely (`task-sdk/src/airflow/sdk/coordinators/java/coordinator.py:207-215`), with its own docstring conceding that
"it may be nondeterministic which one ends up being executed" (`:180-183`).

Candidate *detection* is unaffected and still requires executing nothing: the `AFBNDL01` trailer
magic for Go (`go-sdk/internal/bundlefooter/footer.go:57`), the `.min.mjs` suffix plus a valid layout
header for TypeScript (`task-sdk/src/airflow/sdk/coordinators/node/coordinator.py:43`), a `Main-Class` attribute for Java.

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
(`ts-sdk/src/cli/bundle-encoder.ts:69-79, 214-220`), all outside the header itself. The cache digest
is derived from those three values. No format change, no packer change.

**Go — one new trailer field.** Today nothing covers the source and metadata regions, so a metadata
change is invisible to `binary_sha256`. A whole-content digest over
`[0, metadata_start + metadata_len)` — everything except the trailer — is added. `binary_sha256`
stays unchanged, for integrity.

The trailer must grow: `TrailerSize` is 64 with only 12 reserved bytes
(`go-sdk/internal/bundlefooter/footer.go:24-31, 46-53`), which cannot hold a 32-byte digest. Growing
it has a compatibility trap. The reader seeks to `file_size - FOOTER_SIZE` and checks the magic at
`[56:64]` of that window (`task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py:94-98`),
so against a 96-byte trailer an old reader lands 32 bytes in, fails the magic check, and treats the
file as *not a bundle at all* — surfacing as "cannot find executable bundle containing dag_id=…"
(`:320`) rather than the unsupported-version error the format already knows how to raise (`:101-105`).
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
(`java-sdk/plugin/src/main/kotlin/org/apache/airflow/sdk/plugin/AirflowSdkPlugin.kt:194-195`). Maven
users reproduce it by hand, as they already must for `Airflow-Supervisor-Schema-Version`
(`airflow-core/docs/authoring-and-scheduling/language-sdks/java.rst:765`).

**Any SDK, as a fallback.** A coordinator with no stored digest computes one at read time. Correct
everywhere, at the cost of reading the artifact on every parse — which is why the stored field exists
for runtimes that can carry one, and why Java's absence of one is worth closing.

### The digest lives in the artifact, not beside it

A sidecar hash file would reverse `go-sdk/adr/0004-self-contained-executable-bundle.md`, whose
central decision was replacing a multi-part ZIP with a single self-contained file, and which the
end-to-end tests deliberately uphold — "Deliberately no metadata sidecar: the coordinator must
resolve the schema version from the metadata airflow-ts-pack embedded in the bundle"
(`airflow-e2e-tests/tests/airflow_e2e_tests/conftest.py:721-722`).

### What this retracts

Two accepted Go-SDK ADRs state the removed requirement outright and are superseded on this point:

- `go-sdk/adr/0004-self-contained-executable-bundle.md:43-50` — "**Discovery without execution** — the
  scanner must be able to read `dag_id`/`task_id` and the SDK language/version from a bundle on disk
  without running the binary."
- `go-sdk/adr/0005-retire-go-edge-worker.md:58-62` — "The metadata footer on a packed bundle is
  required for coordinator discovery."

Discovery-without-execution is relocated rather than weakened: nothing needs to learn a `dag_id` from
an artifact any more, because ADR-0010 records the binding when the Dag is processed. The footer
stays required — for candidate detection, the supervisor schema version, integrity, and now the cache
digest.

## Consequences

- Dynamic Dag rendering becomes packable. The fatal empty-inventory checks disappear with the
  requirement they enforced.
- The canonical schema goes to `2.0` and renames `dags` to `task_handlers`, resolving a split in which
  TypeScript bundles have never validated against the schema they claim to conform to. Readers accept
  `1.x` and treat `dags` as a synonym for one release.
- Two spec documents converge or stay explicitly divergent. `task-sdk/docs/executable-bundle-spec.rst`
  and `task-sdk/docs/ts-bundle-spec.rst` describe genuinely different container formats but should
  share one metadata schema; today only the former references it.
- The Go trailer format changes and its version is bumped. TypeScript needs no format change. Java
  gains its first content digest.
- Node's build-time `dag_id` routing (#73126) and the Executable coordinator's `_dag_ids` lookup are
  both replaced by the persisted binding. Java's `Main-Class` first-match-wins — the one genuinely
  nondeterministic selector left — is replaced too, which is a fix rather than a migration.
- The packer may no longer need to execute the artifact at all. `supervisor_schema_version` is a
  compile-time constant of the SDK, so with the inventory optional the introspection round trip could
  go — taking with it the cross-compile host-arch sidecar build
  (`go-sdk/cmd/airflow-go-pack/pack.go:136-151`), the rule that `--executable` with a
  non-host-runnable binary is a hard error (`:384-391`), and the scan-stdout-for-a-sentinel hack
  TypeScript needs because user import-time logging pollutes stdout (`ts-sdk/src/cli/pack.ts:138-152`).
  Whether to keep `--airflow-metadata` and `airflow-go-pack inspect` as debugging affordances is left
  open.
- A byte-identical rebuild produces no new record. A deployment wanting each deploy distinguishable
  regardless should use the bundle version, not the artifact.
- The digest's definition differs per SDK by design, and will be "fixed" unless that is written down
  next to the column.
- Three directory walkers still exist (`task-sdk/src/airflow/sdk/coordinators/_bundle_metadata.py:67`,
  `task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py:270`, `task-sdk/src/airflow/sdk/coordinators/java/coordinator.py:59`), with a standing note that the other two
  should move onto the shared one (`task-sdk/src/airflow/sdk/coordinators/_bundle_metadata.py:61-62`). ADR-0010 removes the need for all
  three on the execution path, which makes that consolidation cheap to finish.

## References

- [ADR-0010](0010-persisted-task-handler-bindings.md) — what consumes the digest, and why the inventory is unused
- [ADR-0003](0003-pure-java-dags.md) — the Java build-time inventory that was designed and never built
- `go-sdk/adr/0001-bundle-packing-options.md` — why the inventory was runtime-introspected, not AST-scanned
- `go-sdk/adr/0004-self-contained-executable-bundle.md` — the Go artifact format, partially superseded here
- `go-sdk/adr/0005-retire-go-edge-worker.md` — the footer-required statement, clarified here
- `go-sdk/internal/bundlefooter/footer.go` — the normative Go trailer layout
- `ts-sdk/src/cli/bundle-encoder.ts` — the TypeScript container format and its per-region digests
- `task-sdk/docs/executable-bundle-spec.rst` — the Go-shaped spec to amend
- `task-sdk/docs/ts-bundle-spec.rst` — the TypeScript spec, which uses `task_handlers` already
- `task-sdk/docs/airflow-metadata.schema.json` — the schema to republish as `2.0`
