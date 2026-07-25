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

# ADR-0006: No Lang-SDK Source Code Display for Mixed-Language (`@task.stub`) Dags

## Status

Proposed

> **Note:** This ADR records a *negative* decision. It exists so that future feature requests
> or PRs proposing multi-file / multi-language source display for stub-backed Dags can be
> pointed here instead of re-litigating the design. Tracked operationally in
> [apache/airflow#67260](https://github.com/apache/airflow/issues/67260), which is closed as
> won't-fix referencing this document.

## Context

Per [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ), Java tasks are declared as
`@task.stub` operators in ordinary Python Dag files: the Dag is defined in Python, while the
task implementation lives in a Java-SDK build artifact (a JAR) shipped in the same bundle. We
call this a **mixed-language Dag**. This is distinct from pure Java Dags
([ADR-0003](0003-pure-java-dags.md)), where the Dag itself is defined in Java and the
coordinator extracts the packed `.java` source via the `Airflow-Java-SDK-Dag-Code` manifest
attribute for display — a mechanism that was removed from AIP-108 scope along with pure Java
Dag authoring.

During AIP-108 testing and demos, users asked whether the UI Code view for a
mixed-language Dag could show the Java-SDK-side source *in addition to* the Python file
containing the stub declarations
([apache/airflow#67260](https://github.com/apache/airflow/issues/67260)) — for example, as a
second tab next to the Python source.

A design sketch was drafted to make this work by extending the
[AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) `DagImporter` interface:

1. A `StubDagImporter` subclassing the Python importer, which after parsing resolves each stub
   Dag's backing artifact within the bundle and captures its source.
2. `get_source_code` becoming **per-Dag and multi-file**
   (`get_source_code(file_path, dag_id, *, bundle_path, bundle_name) -> dict[str, DagSource]`),
   so sibling Dags defined in one Python file do not leak each other's artifacts into the
   Code view.
3. A schema change allowing **multiple `DagCode` rows per `DagVersion`** — dropping the unique
   constraint on `DagCode.dag_version_id`, adding `source_file_name`, `language`, and an
   entrypoint marker — with artifact digests participating in `dag_hash` so that a JAR rebuild
   produces a new `DagVersion`.
4. REST API and UI changes: `GET /dagSources/{dag_id}` gaining a file dimension, and the Code
   tab gaining a file tab bar with per-file language instead of the hardcoded Python.

After discussion, we decided not to pursue this.

## Decision

**We will not support viewing the Lang-SDK-side source code for mixed-language Dags.** For a
Python Dag containing `@task.stub` tasks backed by a Lang-SDK artifact, the Code view shows
only the Python Dag file — the existing single-source, per-`DagVersion` behavior.

Concretely, none of the following is implemented:

- A `StubDagImporter` (or `CrossLangDagImporter`) that resolves and captures Lang-SDK artifact
  source for stub-backed Dags.
- Per-Dag, multi-file semantics for `get_source_code`.
- Multiple `DagCode` rows per `DagVersion`, or `source_file_name` / `language` / entrypoint
  columns on `DagCode`.
- A file dimension on `GET /dagSources/{dag_id}` or a multi-file Code tab in the UI for this
  use case.
- Stamping artifact digests onto the serialized Dag model *for the purpose of source display*.
  (Whether artifact digests should participate in Dag versioning for execution correctness is
  a separate question, out of scope here.)

### Why Not

1. **A Dag with stub tasks is not a distinct Dag category.** A stub operator is just another
   operator inside an ordinary Python Dag. A single Dag may soon mix tasks in several
   languages, so there is no principled, bounded set of "the other side's" files to display.
   Designing importer interfaces around a special stub-Dag concept bakes an artificial
   distinction into AIP-85.
2. **It degenerates into storing a directory tree in `dag_code`.** Showing anything genuinely
   useful for the Java side means more than one file — Java code in particular tends to split
   each task into its own small file/class. Followed to its conclusion, this means persisting
   an entire directory tree in the `dag_code` table, which is not what that table is for.
3. **The source view is already best-effort in pure Python.** Dags produced by factory
   functions already display a file that does not contain the "real" definition. Mixed-language
   Dags are another instance of a known, accepted limitation — not a new gap that must be
   closed.
4. **The problem is transient and will be rare.** Once Dags can be natively authored in other
   languages (pure Java Dags per [ADR-0003](0003-pure-java-dags.md) /
   [ADR-0004](0004-dag-parsing.md), likely after AIP-85 stabilises), authors who care about
   seeing Lang-SDK code in the UI can write native Lang-SDK Dags, whose entrypoint source is
   shown as that Dag's source through the normal single-file path. Tying the `DagImporter`
   interface, the `DagCode` schema, the versioning write path, and the REST/UI contract in
   knots for a shrinking edge case is a poor trade.

### Alternatives Considered

- **Full proposal (per-Dag multi-file `get_source_code` + multiple `DagCode` rows per
  `DagVersion` + REST/UI changes).** Rejected: interface and schema complexity, atomic
  multi-row write semantics, and artifact-hash coupling into `dag_hash` — all for a rare and
  transient case.
- **Show only the Lang-SDK entrypoint file as a second tab.** Rejected: Java code tends to be
  spread across many small files, so the entrypoint alone has limited value, yet this still
  requires nearly all of the same schema, API, and UI changes.
- **Status quo — show the Python file only.** Accepted.

## Consequences

- AIP-85's `DagImporter` interface, the `DagCode` schema (one row per `DagVersion`), the
  `/dagSources` API contract, and the Code tab UI all stay unchanged.
- No cross-language source reconciliation, multi-row atomic write semantics, or
  artifact-hash-driven `DagVersion` churn needs to be built, tested, or maintained for source
  display.
- Users of mixed-language Dags see only the Python stub declarations in the Code view and must
  consult their repository or build artifact for the Java implementation. The Java SDK user
  documentation states this explicitly so the behavior is expected rather than reported as a
  bug.
- [apache/airflow#67260](https://github.com/apache/airflow/issues/67260) is closed as
  won't-fix with a pointer to this ADR.
- The source-packing mechanism of [ADR-0003](0003-pure-java-dags.md)
  (`Airflow-Java-SDK-Dag-Code`) is unaffected: it remains the mechanism for displaying source
  of *natively authored* Lang-SDK Dags if pure Java Dag authoring is revisited in a follow-up
  proposal.
