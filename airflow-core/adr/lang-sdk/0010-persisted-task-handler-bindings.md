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

# ADR-0010: Persisted Task-Handler Bindings — Resolving Lang-SDK Artifacts at Parse Time

## Status

Proposed

## Context

A mixed-language Dag is authored in Python with `@task.stub` tasks whose bodies live in a Lang-SDK
artifact — a packed Go binary, a JAR, a minified `.min.mjs`. Nothing in the Dag says *which* artifact.
Nothing is recorded about that artifact when the Dag is processed, so the link has to be
rediscovered on every single task execution by scanning a filesystem root:

```
DAG PROCESSING                                   stores nothing about the artifact
  DagFileProcessorProcess(etl.py)
    └── PythonDagImporter → Dags with @task.stub tasks
          └── persist DagModel, SerializedDagModel, DagVersion, DagCode
                ┌──────────────────────────────────────────────────────────┐
                │  no artifact path recorded                               │
                │  no artifact bundle recorded                             │
                │  the parse never even looks at the Lang-SDK artifact     │
                └──────────────────────────────────────────────────────────┘

TASK EXECUTION                                   must therefore search, every time
  ExecutableCoordinator._build_execute_task_command(what=ti)
    └── _Bundle.find(executables_root, what.dag_id)
          └── walk every executable file under the root
                read its trailer, verify SHA-256 over the binary region
                parse its metadata, test `dag_id in metadata["dags"]`
```

Two problems compound here.

**The scan is per task.** Because Dag processing records nothing, every task execution re-walks the
root and re-hashes candidates to answer a question whose answer changed only when someone deployed.

**The index it scans is frozen at compile time, so dynamic Dag generation cannot work at all.** The
`dags: {dag_id: {tasks: [...]}}` mapping is written when the artifact is packed, by executing the
freshly built artifact and recording what `RegisterDags` produced
([`collectManifest`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/pkg/execution/metadata.go#L83-L115)). Those identifiers are then fixed
for the life of the artifact. Two coordinators route on them —
`ExecutableCoordinator` through [`_dag_ids`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py#L249-L254)
and [`_Bundle.find`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/executable/coordinator.py#L296-L322),
`NodeCoordinator` through the `task_handlers` mapping
([`_build_execute_task_command`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/node/coordinator.py#L125-L127),
[`_parse_bundle_metadata`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/node/_bundle_reader.py#L379-L383)) —
so a Dag id the artifact only decides on at runtime can never be matched.

That is the case where Dag ids are generated from data the artifact reads when it starts: an external
YAML listing them, say. The build environment holds different data, or none. And the failure is not a
silent mismatch at execution time — it is earlier and harder: an empty inventory is a fatal pack
error ([`empty-dags check`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/cmd/airflow-go-pack/pack.go#L166-L168)), so such an artifact
cannot be packed in the first place.

`JavaCoordinator` sidesteps the inventory only by having none: it matches on `Main-Class`, accepts
`what` and never reads `what.dag_id`
([`_build_execute_task_command`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/java/coordinator.py#L207-L215)),
with its own docstring conceding that with several executable JARs present "it may be
nondeterministic which one ends up being executed"
([`JavaCoordinator`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/java/coordinator.py#L180-L183)). The
Gradle plugin writes two Airflow manifest attributes and no inventory
([`Main-Class`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/java-sdk/plugin/src/main/kotlin/org/apache/airflow/sdk/plugin/AirflowSdkPlugin.kt#L113),
[`Airflow-Supervisor-Schema-Version`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/java-sdk/plugin/src/main/kotlin/org/apache/airflow/sdk/plugin/AirflowSdkPlugin.kt#L194-L195)).
So no coordinator can route a runtime-generated Dag id today: two consult a frozen index, and the
third does not route at all.

Separately, `[sdk] coordinators` locates artifacts through filesystem roots — `jars_root`,
`executables_root`, `bundles_root` ([ADR-0005](0005-coordinator-packaging.md)) — which are
unversioned mutable directories outside any `DagBundle`, with their own delivery problem. Deployments
already solve that delivery by staging a `DagBundle` *into* the root
([`stage_artifacts.py`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/kubernetes-tests/lang_sdk/stage_artifacts.py)), which makes the root a second addressing layer over
a mechanism that already addresses and versions artifacts.

This ADR replaces runtime discovery with a binding resolved once during Dag processing and persisted,
and replaces the filesystem root with a named `DagBundle`.

Terms follow the Language SDK spec ([`lang-sdk-spec.rst`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/lang-sdk-spec.rst)). Native Dags — a Dag authored
entirely in a Lang SDK — are **out of scope** here; they arrive through an ordinary `DagBundle` and a
Dag importer, and are not yet recorded in an ADR.

## Decision

### Artifacts live in a named DagBundle, not a filesystem root

`jars_root` / `executables_root` / `bundles_root` are replaced by a single coordinator kwarg naming a
`DagBundle`:

```ini
[dag_processor]
# The artifact bundle is an ordinary DagBundle, registered like any other.
dag_bundle_config_list = [
    {
        "name": "dags-folder",
        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
        "kwargs": {}
    },
    {
        "name": "java-task-handlers",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {"bucket_name": "artifacts", "prefix": "java", "aws_conn_id": "aws_default"}
    }
]

[sdk]
coordinators = {
    "jdk-17": {
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {
            "java_executable": "/usr/lib/jvm/java-17/bin/java",
            "task_handler_bundle_name": "java-task-handlers"
        }
    }
}
queue_to_coordinator = {"java": "jdk-17"}
```

```
@task.stub(queue="java")        the Dag author picks a queue
        │
        ▼  [sdk] queue_to_coordinator
   "jdk-17"                     the coordinator instance
        │
        ▼  [sdk] coordinators → kwargs.task_handler_bundle_name
   "java-task-handlers"         the bundle name
        │
        ▼  [dag_processor] dag_bundle_config_list
   S3DagBundle(bucket=artifacts, prefix=java)
        │
        ▼  DagBundlesManager().get_bundle(name).initialize()
   bundle.path / <artifact_rel_path>
```

The Python Dag file and the artifact sit in different bundles — `dags-folder` and
`java-task-handlers` above — and that is the expected layout, not a workaround. Binaries and JARs do
not belong in the bundle holding `.py` files. Both are registered with the Dag processor, because
registration is what makes `get_bundle(name)` resolvable on the worker.

The name is `task_handler_bundle_name`, not `..._bundle_path`: the point of routing through a bundle
is that `DagBundlesManager` owns download, refresh and versioning. A path would keep the unversioned
mutable directory and discard all of it. `sdk_` is omitted because the kwarg is already scoped to a
coordinator instance.

The kwarg is mixed-language only. A native Lang-SDK Dag is delivered by whichever `DagBundle` the Dag
processor is scanning, exactly like a `.py` file, and never by coordinator configuration. A single
coordinator instance can serve both roles at once, so nothing may assume one coordinator maps to one
bundle.

The name is validated eagerly when the coordinator registry is built, alongside the existing
validation of every `queue_to_coordinator` key
([`from_config`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/execution_time/coordinator.py#L262-L264)), so a typo surfaces at config load
rather than as a lazy `InvalidCoordinatorError` on the first task.

A deployment that wants to mount artifacts itself uses a `LocalDagBundle` pointed at the mount. That
is the supported replacement for an explicit root, and it is the same mechanism, not an exception to
it.

### Two tables

The resolved binding is persisted. The artifact is normalised out, because one artifact typically
backs many handlers and its fingerprint must have exactly one value.

```sql
CREATE TABLE lang_sdk_task_handler_artifact (
    id                UUID          NOT NULL,
    bundle_name       VARCHAR(250)  NOT NULL,   -- the task_handler_bundle_name it was found in
    relative_fileloc  VARCHAR(2000) NOT NULL,   -- path within that bundle
    size_bytes        BIGINT        NOT NULL,   -- cheap fingerprint tier
    cache_digest      VARCHAR(64)   NOT NULL,   -- content fingerprint tier; see "The fast path"
    last_probed_at    TIMESTAMP     NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT lstha_bundle_fileloc_uq UNIQUE (bundle_name, relative_fileloc)
);

CREATE TABLE lang_sdk_task_handler (
    dag_id                VARCHAR(250)  NOT NULL,
    task_id               VARCHAR(250)  NOT NULL,
    artifact_id           UUID          NOT NULL,
    dag_bundle_name       VARCHAR(250)  NOT NULL,   -- the *Python* file that owns this row
    dag_relative_fileloc  VARCHAR(2000) NOT NULL,   -- ditto
    handler_params        JSON          NOT NULL,   -- list[TaskHandlerParam], ordered
    PRIMARY KEY (dag_id, task_id),
    CONSTRAINT lsth_dag_fkey FOREIGN KEY (dag_id)
        REFERENCES dag (dag_id) ON DELETE CASCADE,
    CONSTRAINT lsth_artifact_fkey FOREIGN KEY (artifact_id)
        REFERENCES lang_sdk_task_handler_artifact (id)
);
CREATE INDEX idx_lsth_dag_file ON lang_sdk_task_handler (dag_bundle_name, dag_relative_fileloc);
CREATE INDEX idx_lsth_artifact_id ON lang_sdk_task_handler (artifact_id);
```

`PRIMARY KEY (dag_id, task_id)` is the conflict guard: two artifacts claiming the same task cannot
both be recorded, and the collision is detected during the parse and reported as an import error
rather than resolved by scan order. The shape mirrors [`TaskOutletAssetReference`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/models/asset.py#L657-L718), including its deliberate absence of a `task_id`
foreign key — there is no per-task table to reference.

`dag_bundle_name` / `dag_relative_fileloc` identify the Python file that owns the row. They exist so
the Dag processor manager can look up prior state by the file it is about to dispatch, **without**
joining through `DagModel`: `dag.relative_fileloc` is not indexed, and the codebase already notes
that querying it means "a sequential scan of dag"
([`reassign_dags_with_unconfigured_bundles`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/bundles/manager.py#L478)).

`handler_params` stores what the runtime declared, so a changed Python file can be re-validated
against a cached declaration with no subprocess. It is deliberately **not** called `arg_bindings`:
that name already denotes the Python side of the comparison — `XComArgBinding` / `LiteralArgBinding`,
carrying wiring and values ([`build_arg_bindings`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/serialization/stub_arg_bindings.py#L221-L286)) —
and reusing it would make the validation read as comparing a thing to itself.

`cache_digest` is **opaque and coordinator-defined**, not "SHA-256 of the file". Each runtime supplies
its own stable value ([ADR-0011](0011-bundle-metadata-and-cache-digest.md)). Nothing outside the
coordinator may assume how it was computed.

### Objects on the wire

Three hops carry the binding. Each is a distinct object; none reuses another's.

**Manager → Dag-parsing child.** [`DagFileParseRequest`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L113-L130) gains the artifacts the manager
already knows about. The parse child runs with `_AIRFLOW_PROCESS_CONTEXT = "client"` and speaks only
`ToDagProcessor` / `ToManager` over its comm socket ([`_parse_file_entrypoint`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L208-L232)) — it has no database, so
prior state must be pushed to it.

```python
class KnownSDKTaskHandlerArtifact(BaseModel):
    bundle_name: str
    relative_fileloc: str
    size_bytes: int
    cache_digest: str


class DagFileParseRequest(BaseModel):
    file: str
    bundle_path: Path
    bundle_name: str
    callback_requests: list[CallbackRequest]
    known_artifacts: list[KnownSDKTaskHandlerArtifact] = []  # new
    type: Literal["DagFileParseRequest"]
```

`known_artifacts` is scoped by *artifact* bundle, not by Dag file, so the manager reads it **once per
parsing loop** for every configured `task_handler_bundle_name` and pushes the same list to every
child. Ten Dag files resolving against one twenty-jar bundle therefore probe that bundle once in
total, not once each.

**Dag-parsing child → coordinator subprocess.** Introduced here. The child spawns the runtime and
forwards bytes in both directions, decoding nothing; the process that spawned the parse decodes the
reply. `ToSDKTaskHandlerProcessor` is a new parent-to-child union differing from `ToDagProcessor` in
one member, and `ToManager` gains `SDKTaskHandlerParsingResult` — the runtime's `Get*` traffic for
connections, variables and XComs is identical either way and is relayed up unchanged.

```python
class SDKTaskHandlerParseRequest(BaseModel):  # parent -> runtime, on ToSDKTaskHandlerProcessor
    file: str  # the candidate artifact being probed
    dag_ids: list[str]  # every Dag in this file with stub tasks routed here
    bundle_path: Path
    bundle_name: str
    type: Literal["SDKTaskHandlerParseRequest"]


class SDKTaskHandlerParsingResult(BaseModel):  # runtime -> parent, on ToManager
    fileloc: str
    task_handlers: dict[str, list[TaskHandlerDeclaration]]  # dag_id -> declarations
    import_errors: dict[str, str] | None = None
    warnings: list | None = None
    type: Literal["SDKTaskHandlerParsingResult"]


class TaskHandlerDeclaration(BaseModel):
    task_id: str
    params: list[TaskHandlerParam]  # ordered; arg bindings are positional


class TaskHandlerParam(BaseModel):
    name: str
    value_schema: ArgValueSchema | None = None  # an open-vocabulary JSON Schema fragment
    required: bool  # the handler declares no default
```

`ArgValueSchema` is [ADR-0007](0007-taskflow-across-language-boundary.md)'s shipped type, reused here
so both sides of a comparison are the same type. Despite the name it carries **JSON Schema**: a
pydantic-generated fragment that ships verbatim and that runtimes are required to treat as
open-vocabulary
([`_infer_value_schema`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/serialization/stub_arg_bindings.py#L95-L140)).
The name is not changed here: the type is already code-generated into all three SDKs
([`ArgValueSchema`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/execution_time/schema/schema.json#L4600-L4606)),
so renaming it is a supervisor-schema change belonging to ADR-0007, not to this one.

A `dag_id` the artifact registers nothing for is **omitted** from `task_handlers` rather than returned
empty, so a probe that matches nothing is distinguishable from a probe that matched a Dag with zero
tasks.

**Dag-parsing child → manager.** [`DagFileParsingResult`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L133-L145) gains the resolved
bindings.

```python
class SDKTaskHandlerBinding(BaseModel):
    dag_id: str
    task_id: str
    artifact_bundle_name: str
    artifact_rel_path: str
    artifact_size_bytes: int
    artifact_cache_digest: str
    handler_params: list[TaskHandlerParam]


class DagFileParsingResult(BaseModel):
    fileloc: str
    serialized_dags: list[LazyDeserializedDAG]
    warnings: list | None = None
    import_errors: dict[str, str] | None = None
    task_handler_bindings: list[SDKTaskHandlerBinding] | None = None  # new
```

`None` and `[]` mean different things, and the difference is load-bearing:

| value  | meaning                                  | manager does          |
|--------|------------------------------------------|-----------------------|
| `None` | handlers were not evaluated in this parse | **nothing** — no reconcile |
| `[]`   | evaluated, this file has no stub handlers | delete this file's rows    |
| `[…]`  | evaluated, these are the bindings         | reconcile to this set      |

`None` covers the stability-check early return ([`_parse_file`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L245-L251)), callback-only runs
([`_parse_file`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L260-L263)), and validation failure (below). It mirrors the `files_parsed=None` semantics
`persist_parsing_result` already uses ([`persist_parsing_result`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/manager.py#L1361-L1364)).
Without it, a transient parse failure would silently wipe every binding the file owns.

On a fast-path skip the child **re-emits the bindings it was given**, unchanged. It does not omit
them. Request and result carrying the same information makes that a copy rather than a special
"keep these" signal the reconcile could get wrong.

**Scheduler → worker.** `ExecuteTask` and `StartupDetails` each gain one optional reference. The
artifact bundle is a second, independent bundle, so it needs its own `BundleInfo`.

```python
class SDKTaskHandlerRef(BaseModel):
    bundle_info: BundleInfo  # the artifact bundle: name, version, version_data
    rel_path: str  # path within it


class ExecuteTask(BaseDagBundleWorkload):
    ti: TaskInstanceDTO
    dag_rel_path: os.PathLike[str]  # the Python Dag file, unchanged
    bundle_info: BundleInfo  # the Dag bundle, unchanged
    task_handler: SDKTaskHandlerRef | None = None  # new
    ...


class StartupDetails(BaseModel):
    ti: TaskInstance
    dag_rel_path: str
    bundle_info: BundleInfo
    task_handler: SDKTaskHandlerRef | None = None  # new
    ...
```

`None` means "this task needs no Lang-SDK artifact" — an ordinary Python task. It never means
"unknown": a stub task that failed to resolve is not queued at all (see "Failure handling").

### Flow 1 — Dag processing: the write path

Resolution is driven by the Python file's parse, after `PythonDagImporter` has produced the Dags.
The artifact never parses itself into these tables. That ordering is deliberate: a `dag_id` exists
before any row referencing it, so the foreign key holds and a cold start has no window in which a
stub Dag is validated against bindings that have not been written yet.

```
DagProcessorManager                                        [reads DB]
  │
  │  once per parsing loop, per configured task_handler_bundle_name:
  │    SELECT bundle_name, relative_fileloc, size_bytes, cache_digest
  │      FROM lang_sdk_task_handler_artifact
  │     WHERE bundle_name IN (:configured bundles)          ──▶ known_artifacts
  │
  │  per file about to be dispatched:
  │    (the child re-reads nothing; it has no DB)
  │
  ├── DagFileParseRequest(file=etl.py, bundle_*, known_artifacts=[...])
  ▼
DagFileProcessorProcess(etl.py)                            [no DB — client context]
  └── _parse_file_entrypoint → _parse_file
        │
        ├─1─ BundleDagBag → PythonDagImporter → airflow.sdk.DAG objects
        │
        ├─2─ _serialize_dags(bag)
        │      is_stub tasks now carry arg_bindings (ADR-0007)
        │
        ├─3─ collect stub tasks, group by coordinator
        │      stub task    queue     coordinator      task_handler_bundle_name
        │      ─────────────────────────────────────────────────────────────────
        │      extract   →  "java" →  jdk-17        →  "java-task-handlers"
        │      transform →  "java" →  jdk-17        →  "java-task-handlers"
        │      ingest    →  "go"   →  go-sdk        →  "go-task-handlers"
        │
        │      a queue with no coordinator entry is an import error here,
        │      not a silent Python fallback at execution time
        │
        ├─4─ per coordinator: list candidates in its bundle
        │      Go   → files carrying the AFBNDL01 trailer magic
        │      Java → *.jar with a Main-Class manifest attribute
        │      TS   → *.min.mjs with a valid //# airflowBundle= layout header
        │      walk order is deterministic, so conflicts reproduce
        │
        ├─5─ FAST PATH, per candidate  (see "The fast path")
        │      size + cache_digest match known_artifacts, and the candidate
        │      set is unchanged  ──▶ skip the launch, echo the bindings
        │      anything differs   ──▶ probe
        │
        ├─6─ PROBE, per differing candidate — one subprocess
        │      SDKTaskHandlerProcessorProcess.start(
        │          target=_parse_task_handler_entrypoint,
        │          coordinator=JavaCoordinator("jdk-17"),
        │          path=<candidate>)
        │        ──SDKTaskHandlerParseRequest(file=…, dag_ids=["etl"])──▶ runtime
        │        ◀─SDKTaskHandlerParsingResult(task_handlers={"etl": […]})── runtime
        │        Get* from the runtime is relayed up ToManager unchanged
        │
        ├─7─ VALIDATE per dag_id, unioned across coordinators
        │      task_id sets must match exactly
        │      arg_bindings[*].name    ↔ handler_params[*].name, in order
        │      arg_bindings[*].schema  ↔ handler_params[*].value_schema,
        │                                 compared only where neither is null
        │      two candidates claiming one (dag_id, task_id) → import error
        │                                                      naming both paths
        │
        └─8─ on success → task_handler_bindings=[…]
             on mismatch → import_errors[etl.py]=…  AND  bindings=None
        │
        ├── DagFileParsingResult(serialized_dags=[…], import_errors={…},
        ▼                        task_handler_bindings=[…] | [] | None)
DagProcessorManager.persist_parsing_result                 [writes DB]
  └── update_dag_parsing_results_in_db — one transaction, in this order:
        1. add_dags / update_dags               → DagModel rows exist
        2. asset reference tables               (existing)
        3. lang_sdk_task_handler_artifact       UPSERT by (bundle_name, relative_fileloc)   ← new
        4. lang_sdk_task_handler                reconcile by dag_id                          ← new
        5. SerializedDagModel / DagVersion / DagCode
        6. ParseImportError / DagWarning
```

Step 3 must upsert: two parse children can discover the same artifact in the same loop and race on
the unique key. `activate_assets_if_possible` is the in-repo precedent for the dialect-aware form
([`activate_assets_if_possible`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/collection.py#L908-L932)).

Step 4 reconciles **by the `dag_id`s in the result**, not by file path: delete rows for those
`dag_id`s whose `task_id` is absent from the returned set, then insert or update the rest. Path-keyed
eviction breaks when a Dag moves between files — the old rows stay keyed to a path nothing parses any
more, and the primary key then blocks the new insert. With `dag_id` as the key a move simply updates
`dag_relative_fileloc`, and a Dag that disappears entirely is reclaimed by the `ON DELETE CASCADE`.
The set-difference shape to copy is [`_add_dag_asset_references`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/collection.py#L1004-L1026).

Artifact rows are **never** evicted from one file's result. One artifact backs handlers owned by many
Python files, so this file seeing fewer candidates says nothing about another file's. They are a
cache; they are reclaimed by orphan sweep or by `db clean`, never by a per-file reconcile.

### Flow 2 — Scheduling: the read path

```
SchedulerJobRunner._executable_task_instances_to_queued        [reads DB]
  │
  ├── SELECT TI JOIN dag_run JOIN dag  … WHERE DR.state=RUNNING
  │                                       AND TI.state=SCHEDULED …
  │   .options(selectinload(TI.dag_model))
  │   .options(joinedload(TI.dag_run).selectinload(DagRun.created_dag_version)
  │                                  .load_only(DagVersion.version_data))
  │   .options(<the task-handler binding>)                                   ← new
  │
  │   the binding MUST be loaded here, before make_transient below.
  │   a lazy load on a transient object returns None silently instead of
  │   raising DetachedInstanceError, so a late read yields a workload with
  │   no artifact and no error.
  │
  ├── make_transient(ti) for every returned TI
  ▼
SchedulerJobRunner._enqueue_task_instances_with_queued_state   [no further DB reads on ti]
  └── for ti in task_instances:
        dag_run finished?        → set_state(None); continue     (existing)
        no dag_version_id?       → warn; continue                (existing)
        is_stub and no binding?  → FAIL the TI with the reason   ← new
                                   never queue it
        │
        └── ExecuteTask.make(ti, task_handler=SDKTaskHandlerRef(...))
              dag_rel_path   ← ti.dag_model.relative_fileloc      (existing)
              bundle_info    ← Dag bundle, pinned to the run       (existing)
              task_handler   ← artifact bundle + rel_path          ← new
              │
              └── executor.queue_workload(workload)
```

A stub task with no binding is **failed with its reason**, not skipped. Skipping is what the
`dag_version_id` branch above does, and its own log message concedes the task is then stuck until
something else repairs it — acceptable for a transient race, wrong for a configuration error that
cannot repair itself without a re-parse. Filtering it out of the queueing query instead, the way
`DM.bundle_name.is_not(None)` does, is worse still: the task silently never appears and nothing
explains why.

### Flow 3 — Task execution

```
executor worker process
  └── BaseExecutor.run_workload(workload)
        └── supervise_task(ti=…, bundle_info=…, dag_rel_path=…,
                           task_handler=workload.task_handler)        ← new
              │
              ├── coordinator = get_coordinator_manager().for_queue(ti.queue)
              │     unchanged: execution still routes on queue
              │
              └── coordinator.execute_task(what=ti, …, task_handler=…)
                    └── SubprocessCoordinator._build_execute_task_command(
                            what=ti, task_handler=task_handler)        ← signature change
                          │
                          ├── bundle = DagBundlesManager().get_bundle(
                          │       name=task_handler.bundle_info.name,
                          │       version=task_handler.bundle_info.version,
                          │       version_data=task_handler.bundle_info.version_data)
                          │   bundle.initialize()          ← the SECOND bundle
                          │
                          ├── artifact = bundle.path / task_handler.rel_path
                          │   NO directory walk. NO dag_id match. NO metadata["dags"].
                          │
                          ├── verify integrity, read supervisor_schema_version
                          │     Go   → AFBNDL01 trailer: SHA-256 over the binary
                          │            region, exactly as today
                          │     Java → Airflow-Supervisor-Schema-Version manifest attr
                          │     TS   → //# airflowBundle= layout header: SHA-256 over
                          │            all three regions, exactly as today
                          │
                          └── command
                                Go   → [artifact]
                                Java → [java, -classpath, <bundle root>/*, …, Main-Class]
                                TS   → [node, artifact]
                    │
                    └── _PopenActivitySubprocess.start(…)
                          ──StartupDetails(ti, dag_rel_path, bundle_info,
                                           task_handler)──▶ runtime
                          runtime looks up its own registration by
                          (ti.dag_id, ti.task_id) — unchanged
```

No database is read in this flow. The worker never queries the binding tables; everything it needs
arrived on the workload. That is the property the whole design exists to buy.

The integrity check is unchanged and stays on this path. It is a different concern from the
`cache_digest`: the digest answers "did this artifact change since I validated it", the integrity
hash answers "is this artifact intact". A truncated or half-downloaded file still reports a plausible
stored digest, so one cannot substitute for the other.

### The fast path

Validation is expensive — one subprocess per candidate — and a file is re-parsed every
`[dag_processor] min_file_process_interval` seconds, 30 by default. Re-probing unchanged artifacts
every 30 seconds forever is not acceptable, so the probe is skipped when nothing relevant changed.

Two tiers, cheapest first:

```
for each candidate in the coordinator's bundle:
    stat(candidate).st_size  ≠  known.size_bytes   →  PROBE
    read stored cache_digest ≠  known.cache_digest →  PROBE
    otherwise                                      →  SKIP, echo the binding
```

`mtime` is deliberately absent. It is reset by an object-store download and by container rebuilds, so
it produces churn without adding certainty; size plus digest is sufficient, with size acting only as
a free pre-filter.

Two conditions beyond the per-file comparison:

**The candidate set must be unchanged.** A newly deployed artifact has no known fingerprint, so a set
that differs from `known_artifacts` forces a probe. Without this, an added artifact that collides on
`(dag_id, task_id)` would never be detected, and the conflict rule above would be unenforceable.

**The Python side is re-validated regardless.** The skip avoids the *subprocess*, not the comparison.
`handler_params` is stored precisely so a changed `.py` — a stub task that gained an argument — is
compared against the cached declaration in process. Skipping the comparison as well would cache a
verdict for a signature that no longer exists, and ship the mismatch to a worker as a runtime
argument error instead of catching it as an import error.

### Failure handling

**Validation fails.** Record the import error; leave every existing row untouched. `bindings=None`
already expresses "do not reconcile", so this needs no additional mechanism.

Leaving the rows is not laxity. `DagModel.has_import_errors` is a scheduling gate — new Dag runs are
blocked ([`dags_needing_dagruns`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/models/dag.py#L799), via `dags_needing_dagruns`) and so are manual
triggers ([`trigger_dag_run`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/api_fastapi/core_api/routes/public/dag_run.py#L772),
[`trigger_dag_run`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/api_fastapi/execution_api/routes/dag_runs.py#L110), [`clear_dag_run`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/api_fastapi/execution_api/routes/dag_runs.py#L199)). What it does not gate is the tasks of a run
that was *already* in flight, which the queueing query admits on `DR.state == RUNNING` alone. Keeping
the last-known-good binding lets those runs finish, and is symmetric with how a `.py` that stops
importing keeps its last good `SerializedDagModel` rather than having it deleted.

**A stub task has no binding at all.** The scheduler fails it with the reason rather than queueing a
workload that would die on the worker at `ValueError("dag_path is required")`, far from the cause.

**Two artifacts claim one `(dag_id, task_id)`.** An import error against the Python file — the
definition whose author can act — naming both artifact paths, since the fix is in the deployment.

## Consequences

- Task execution stops searching. A worker resolves its artifact by path, from data that arrived on
  the workload, with no directory walk and no database read.
- Dag ids are never recorded at build time, so dynamic Dag rendering works: whatever the artifact
  registers when the Dag processor asks it is what gets recorded.
- `jars_root` / `executables_root` / `bundles_root` are removed. Artifacts inherit download, refresh
  and versioning from `DagBundle`. Deployments that mount artifacts themselves point a
  `LocalDagBundle` at the mount.
- Two new tables and one migration. `lang_sdk_task_handler` is reconciled on every parse of a file
  that owns rows in it; `lang_sdk_task_handler_artifact` is a cache with no per-file eviction.
- The artifact bundle is a second bundle on the execution path. `ExecuteTask` and `StartupDetails`
  each grow one optional `SDKTaskHandlerRef`, and the worker performs a second `initialize()`. Workload
  payloads grow by roughly one `BundleInfo`, which is serialized into executor argv for
  K8s/ECS/Batch/Lambda and into the message body for Celery/SQS.
- `DagFileParseRequest` and `DagFileParsingResult` each gain a field, and `ToSDKTaskHandlerProcessor`
  becomes a fifth union the supervisor-schema registry introspects. Both messages already appear in
  the generated schemas of all three SDKs, so the snapshot is regenerated and the two prek hooks
  guarding it run.
- Every Lang SDK must answer `SDKTaskHandlerParseRequest`. None does today: Go decodes
  `DagFileParseRequest` and drops it ([`TypeDagFileParseRequest`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/pkg/execution/messages.go#L78-L83)), TS answers with a
  documented empty stub ([`handleParse`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/ts-sdk/src/coordinator/runtime.ts#L265-L281)), Java has nothing. This is the
  critical path for the feature.
- A misrouted queue becomes an import error instead of a runtime failure. Today a stub task on a
  queue absent from `queue_to_coordinator` silently falls back to the Python coordinator
  ([`for_queue`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/execution_time/coordinator.py#L280-L284)) and dies in
  `_StubOperator.execute()`.
- One artifact bundle is one Java classpath. `_calculate_classpath` joins every JAR under the root
  ([`_calculate_classpath`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/java/coordinator.py#L85-L87)), so all handlers in a bundle
  share one dependency graph. Isolating conflicting dependency versions requires a second bundle, a
  second coordinator instance, and a second queue. This needs documenting, not just deciding.
- Steady-state parsing costs no subprocesses. A cold start — empty tables, or a bundle whose
  candidate set changed — costs one subprocess per changed candidate per coordinator, shared across
  all Dag files in that parsing loop through `known_artifacts`.
- No `DagVersion` coupling. An artifact rebuild does not bump a Dag's version, and a Dag edit does
  not invalidate an artifact fingerprint. The two change independently and are tracked
  independently, following the reasoning recorded in
  [`rollup_fingerprint`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/migrations/versions/0121_3_3_0_add_rollup_fingerprint_to_apdr.py#L22-L29),
  where a fingerprint was chosen over `dag_version_id` for exactly this reason.
- Mixed-language stays Python-primary. A Lang-SDK runtime cannot declare stub tasks, and a native Dag
  cannot delegate a task to Python.

## References

- [ADR-0011](0011-bundle-metadata-and-cache-digest.md) — what the artifact carries, and the cache digest
- [ADR-0003](0003-pure-java-dags.md) — `BundleScanner` / build-time artifact inventory, superseded here
- [ADR-0004](0004-dag-parsing.md) — the coordinator subprocess bridge and `can_handle_dag_file`
- [ADR-0005](0005-coordinator-packaging.md) — `[sdk] coordinators`, and the roots this ADR removes
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` / `ArgValueSchema`
- [`go-sdk ADR-0004`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/adr/0004-self-contained-executable-bundle.md) — the artifact format, and the
  "discovery without execution" requirement this ADR retracts
- [`processor.py`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py) — `_parse_file`, `DagFileParseRequest`, `DagFileParsingResult`
- [`collection.py`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/collection.py) — `update_dag_parsing_results_in_db`, the reconcile patterns
- [`scheduler_job_runner.py`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/jobs/scheduler_job_runner.py) — the queueing query and `make_transient`
- [`lang-sdk-spec.rst`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/lang-sdk-spec.rst) — Language SDK spec
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs

## Appendix

### Appendix A — Which records each flow touches

| Flow | Table | Access | Keyed by | Notes |
|---|---|---|---|---|
| Dag processing | `lang_sdk_task_handler_artifact` | READ | `bundle_name IN (configured)` | manager, once per parsing loop; pushed to every child as `known_artifacts` |
| Dag processing | `lang_sdk_task_handler_artifact` | UPSERT | `(bundle_name, relative_fileloc)` | dialect-aware; two children can race |
| Dag processing | `lang_sdk_task_handler` | READ | `(dag_bundle_name, dag_relative_fileloc)` | manager; prior bindings for the file being dispatched |
| Dag processing | `lang_sdk_task_handler` | RECONCILE | `dag_id` from the result | skipped entirely when `bindings is None` |
| Dag processing | `dag` | WRITE | `dag_id` | existing; must precede the handler rows for the FK |
| Dag processing | `parse_import_error` | WRITE | `(bundle_name, relative_fileloc)` | existing; carries mismatch and conflict errors |
| Scheduling | `lang_sdk_task_handler` | READ | `(dag_id, task_id)` | eager-loaded or bulk pre-queried **before** `make_transient` |
| Scheduling | `lang_sdk_task_handler_artifact` | READ | `artifact_id` | same load; supplies bundle name and path |
| Scheduling | `dag` | READ | `has_import_errors` | existing; gates new runs, not in-flight ones |
| Task execution | — | none | — | everything arrives on the workload |

How a record crosses each boundary:

```
lang_sdk_task_handler_artifact ──▶ KnownSDKTaskHandlerArtifact      ──▶ DagFileParseRequest.known_artifacts
                                                            (manager → parse child)

SDKTaskHandlerParsingResult       ──▶ SDKTaskHandlerBinding ──▶ DagFileParsingResult.task_handler_bindings
                                                            (parse child → manager → both tables)

both tables                    ──▶ SDKTaskHandlerRef     ──▶ ExecuteTask.task_handler
                                                            (scheduler → executor)
                                                      ──▶ StartupDetails.task_handler
                                                            (supervisor → runtime)
```

### Appendix B — Rejected alternatives

**The artifact writes its own rows.** Let the Dag processor parse each artifact as a first-class
input; the runtime self-reports its registrations and the rows are written by the artifact's own
parse. This removes discovery entirely — the artifact is addressed by the path the processor is
already iterating — and makes an artifact rebuild self-healing. It was rejected on ordering: on a
cold start a Python file can be parsed before the artifact that backs it, so validation would find no
rows and report an import error that is simply wrong until the next cycle. Repairing that needs
"absence is not evidence" semantics plus a re-parse trigger when rows appear, which is more machinery
than the Python-pulls flow costs.

**A single denormalised table.** Storing the artifact's fingerprint on every binding row avoids a
join. Rejected because the same artifact's digest would be stored many times and could disagree —
two parse children in one loop, one reading the file before a redeploy and one after, write different
digests, and the skip decision then depends on which row is read. It also forfeits the cross-file
cache: `known_artifacts` is scoped by artifact bundle, which a per-Dag-file table cannot answer
without de-duplicating in SQL and trusting the copies agree.

**A column on `DagModel`.** The first sketch was one nullable path column. It cannot express a Dag
whose stub tasks route to different queues and therefore different runtimes and different artifacts,
which the feature explicitly allows.

**Keying bindings to `dag_version_id`.** A `DagVersion` is created from the serialized Dag's hash
([`hash`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/models/serialized_dag.py#L378-L389), [`write_dag`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/models/serialized_dag.py#L697-L717)), which no artifact change
feeds. Tying bindings to it would mean putting the artifact reference inside the serialized payload,
making a rebuild look like a Dag edit and a Dag edit invalidate the binding. Both directions are
wrong.

**A dedicated Execution API for the binding.** Instead of shipping the artifact reference on the
workload, give the worker an Execution API route and let it fetch its own binding at launch.
Rejected on blast radius: the Execution API is a versioned contract every task SDK speaks, so a new
route there is a far larger commitment than a field on a workload the scheduler already builds. It
would also put a round trip, and a dependency on API availability, in front of every task launch —
to deliver data the scheduler held when it queued the task.

**A build timestamp in the artifact metadata, to force a new record per deploy.** Rejected on three
counts: it defeats reproducible builds, which Go, Gradle and Maven all work to provide; for Go it
would not even register, because the existing trailer digest covers the binary region only and not
the metadata; and the deployment signal it seeks already exists in the bundle.

### Appendix C — Suggested sequencing

The validation half requires a new wire message in three SDKs that have none. The recording half does
not, and is what removes the execution-time scan. They can ship separately:

**Phase 1 — resolution and consumption.** `task_handler_bundle_name` replaces the roots; the parse
resolves the artifact and writes both tables; the workload, `StartupDetails` and the coordinators
consume `SDKTaskHandlerRef`; `_dag_ids` and the root walk are deleted. No new wire message, no SDK work,
no subprocess during parsing. `handler_params` is written empty.

**Phase 2 — validation.** `SDKTaskHandlerParseRequest` / `SDKTaskHandlerParsingResult`,
`ToSDKTaskHandlerProcessor`, `SDKTaskHandlerProcessorProcess`, the three SDK implementations, and the
stub-to-handler comparison. `handler_params` is populated, and the fast path becomes load-bearing.

Phase 1 needs a stand-in for step 4 of Flow 1, since without a probe nothing reports which
`(dag_id, task_id)` an artifact serves. The narrowest one is a required `@task.stub(artifact=...)`
naming the path within the bundle, relaxed to optional in phase 2 once probing can infer it.
