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

# ADR-0013: Persisted Task-Handler Bindings (Resolving Lang-SDK Artifacts at Parse Time)

## Status

Accepted

## Context

A mixed-language Dag is authored in Python with `@task.stub` tasks whose bodies live in a Lang-SDK
artifact (a packed Go binary, a JAR, a minified `.min.mjs`). Nothing in the Dag says *which* artifact.
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

**The identifiers it scans are frozen when the artifact is built.** Packing an artifact records the
Dag ids and task ids it exposes into the artifact's own metadata at the packing stage, and they are
fixed from then on. Currently, a coordinator picks an artifact by looking a `dag_id` up in that
recorded list. A Dag whose id the artifact only decides on when it runs (for example: generated from
an external YAML) can never be matched to it.

Separately, `[sdk] coordinators` locates artifacts through filesystem roots (`jars_root`,
`executables_root`, `bundles_root` ([ADR-0005](0005-coordinator-packaging.md))) which are
unversioned mutable directories outside any `DagBundle`, with their own delivery problem. Deployments
already solve that delivery by staging a `DagBundle` *into* the root
([`stage_artifacts.py`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/kubernetes-tests/lang_sdk/stage_artifacts.py)), which makes the root a second addressing layer over
a mechanism that already addresses and versions artifacts.

This ADR replaces runtime discovery with a binding resolved once during Dag processing and persisted,
and replaces the filesystem root with a named `DagBundle`.

Native Dags (a Dag authored entirely in a Lang SDK) are **out of scope** here; they arrive through an
ordinary `DagBundle` and a Dag importer, and are not yet recorded in an ADR.

## Decision

### Artifacts live in a named DagBundle, not a filesystem root

`jars_root` / `executables_root` / `bundles_root` are replaced by a single coordinator kwarg naming a
`DagBundle`.

Nothing is taken away from deployments that want to place artifacts themselves. A `LocalDagBundle`
pointed at the mount does exactly what an explicit root did (the directory is still theirs to
manage) but it arrives through the same mechanism as every other bundle rather than beside it, so
it inherits refresh and the rest without special-casing:

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

The Python Dag file and the artifact sit in different bundles (`dags-folder` and
`java-task-handlers` above) and that is the expected layout, not a workaround. Binaries and JARs do
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

### Two tables

The resolved binding is persisted. The artifact is normalised out, because one artifact typically
backs many handlers and its fingerprint must have exactly one value.

```sql
CREATE TABLE lang_sdk_task_handler_artifact (
    id                     UUID          NOT NULL,
    bundle_name            VARCHAR(250)  NOT NULL,   -- the task_handler_bundle_name it was found in
    relative_fileloc       VARCHAR(2000) NOT NULL,   -- path within that bundle
    relative_fileloc_hash  VARCHAR(32)   NOT NULL,   -- md5 of relative_fileloc; the path is too long to index
    size_bytes             BIGINT        NOT NULL,   -- cheap fingerprint tier
    cache_digest           VARCHAR(128)  NULL,       -- content fingerprint tier, NULL if none is stored; see "The fast path"
    task_handlers          JSON          NOT NULL,   -- {dag_id: [TaskHandlerDeclaration, ...]}, the probe answer
    last_probed_at         TIMESTAMP     NOT NULL,
    CONSTRAINT lang_sdk_task_handler_artifact_pkey PRIMARY KEY (id),
    CONSTRAINT lang_sdk_task_handler_artifact_bundle_fileloc_uq UNIQUE (bundle_name, relative_fileloc_hash)
);

CREATE TABLE lang_sdk_task_handler (
    dag_id                     VARCHAR(250)  NOT NULL,
    task_id                    VARCHAR(250)  NOT NULL,
    artifact_id                UUID          NOT NULL,
    dag_bundle_name            VARCHAR(250)  NOT NULL,   -- the *Python* file that owns this row
    dag_relative_fileloc       VARCHAR(2000) NOT NULL,   -- ditto
    dag_relative_fileloc_hash  VARCHAR(32)   NOT NULL,   -- md5 of dag_relative_fileloc
    CONSTRAINT lang_sdk_task_handler_pkey PRIMARY KEY (dag_id, task_id),
    CONSTRAINT lang_sdk_task_handler_dag_id_fkey FOREIGN KEY (dag_id)
        REFERENCES dag (dag_id) ON DELETE CASCADE,
    CONSTRAINT lang_sdk_task_handler_artifact_id_fkey FOREIGN KEY (artifact_id)
        REFERENCES lang_sdk_task_handler_artifact (id)
);
CREATE INDEX idx_lang_sdk_task_handler_dag_file
    ON lang_sdk_task_handler (dag_bundle_name, dag_relative_fileloc_hash);
CREATE INDEX idx_lang_sdk_task_handler_artifact_id ON lang_sdk_task_handler (artifact_id);
```

`PRIMARY KEY (dag_id, task_id)` is the conflict guard: two artifacts claiming the same task cannot
both be recorded, and the collision is detected during the parse and reported as an import error
rather than resolved by scan order.

`dag_bundle_name` / `dag_relative_fileloc` identify the Python file that owns the row. They exist so
the Dag processor manager can look up prior state by the file it is about to dispatch, **without**
joining through `DagModel`: `dag.relative_fileloc` is not indexed, and the codebase already notes
that querying it means "a sequential scan of dag"
([`reassign_dags_with_unconfigured_bundles`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/bundles/manager.py#L478)).

`task_handlers` caches the artifact's whole probe answer, every handler it registers keyed by
`dag_id`, so a changed Python file can be re-validated against it with no subprocess. It is written
only from a probe, together with `size_bytes` and `cache_digest`, so the answer always belongs to the
fingerprint beside it. A binding row is then only a mapping from a stub task to its artifact.

`cache_digest` is **opaque and coordinator-defined**, not "SHA-256 of the file".

### Objects on the wire

**Manager → Dag-parsing child.** [`DagFileParseRequest`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L113-L130) gains the artifacts the manager
already knows about. The parse child processor subprocesses run in the client context without a
database connection ([`_parse_file_entrypoint`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L208-L232)),
so the prior cache state must be pushed down from the manager. The alternative is a dedicated
Execution API for the child processor process to retrieve `TaskHandlerArtifact` itself, which
was rejected on blast radius.

```python
class TaskHandlerArtifact(BaseModel):
    bundle_name: str
    relative_fileloc: str
    size_bytes: int
    cache_digest: str | None  # None: the artifact stores no fingerprint, so it is always probed
    task_handlers: dict[str, list[TaskHandlerDeclaration]]  # the probe answer: dag_id -> declarations


class DagFileParseRequest(BaseModel):
    file: str
    bundle_path: Path
    bundle_name: str
    callback_requests: list[CallbackRequest]
    known_artifacts: list[TaskHandlerArtifact] = []  # new
    type: Literal["DagFileParseRequest"]
```

`known_artifacts` is scoped by *artifact* bundle, not by Dag file, so the manager reads it **once per parsing loop** for every bundle that can hold task handlers: the `task_handler_bundle_name` of each coordinator a queue routes to, plus the Dag bundles it parses when one of them sets none and so reads the task's own Dag bundle. A coordinator no queue routes to runs no stub task, so its bundle is not read. Each child gets the rows of the named bundles whose team matches its Dag bundle's team, and of its own Dag bundle when a coordinator falls back to it, never those of another Dag bundle, which no coordinator reads for it. The team rule exists because an answer one file's parse records is trusted by every file that reads it. Teams match only when equal: without `[core] multi_team` every named bundle is in scope, and with it a team-less Dag bundle sees only team-less named bundles. One recorded answer serves every file, so ten Dag files resolving against one bundle probe a changed artifact once in total, apart from the children already started when it changed.

**Dag-parsing child → coordinator subprocess.** Introduced here. The child spawns the runtime and
forwards bytes in both directions, decoding nothing; the process that spawned the parse decodes the
reply. `ToSDKTaskHandlerProcessor` is a new parent-to-child union differing from `ToDagProcessor` in
one member, and `ToManager` gains `TaskHandlerParsingResult`.

```python
class TaskHandlerParseRequest(BaseModel):  # parent -> runtime, on ToSDKTaskHandlerProcessor
    file: str  # the candidate artifact being probed
    bundle_path: Path
    bundle_name: str
    type: Literal["TaskHandlerParseRequest"]


class TaskHandlerParsingResult(BaseModel):  # runtime -> parent, on ToManager
    fileloc: str
    task_handlers: dict[str, list[TaskHandlerDeclaration]]  # dag_id -> declarations
    import_errors: dict[str, str] | None = None
    warnings: list | None = None
    type: Literal["TaskHandlerParsingResult"]


class TaskHandlerDeclaration(BaseModel):
    task_id: str
    binding: Literal["positional", "named"]  # how arguments bind to params
    params: list[TaskHandlerParam] | None  # ordered; None: the runtime cannot list them


class TaskHandlerParam(BaseModel):
    name: str | None  # None: the runtime has no name for this positional parameter
    value_schema: JSONSchema | None = None
    exact_name: bool = False  # match as spelled, not case-insensitively with underscores ignored
```

`task_handlers` maps every `dag_id` the artifact registers a handler for, and is `{}` when it registers
none. The answer must not depend on the request, because the manager records it once and serves it to
every Dag file that resolves against the artifact.

**Dag-parsing child → manager.** [`DagFileParsingResult`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L133-L145) gains the resolved
bindings and the probed artifacts.

```python
class TaskHandlerBinding(BaseModel):
    dag_id: str
    task_id: str
    artifact_bundle_name: str
    artifact_rel_path: str


class DagFileParsingResult(BaseModel):
    fileloc: str
    serialized_dags: list[LazyDeserializedDAG]
    warnings: list | None = None
    import_errors: dict[str, str] | None = None
    task_handler_bindings: list[TaskHandlerBinding] | None = None  # new
    probed_artifacts: list[TaskHandlerArtifact] = []  # new
```

`probed_artifacts` holds every artifact the parse probed, with its answer; it is recorded even when the bindings are `None`, and it is the only way an answer gets written.

For `task_handler_bindings`, `None` and `[]` mean different things, and the difference is load-bearing:

| value  | meaning                                  | manager does          |
|--------|------------------------------------------|-----------------------|
| `None` | handlers were not evaluated in this parse | **nothing** — no reconcile |
| `[]`   | evaluated, this file has no stub handlers | delete the rows of this result's Dags |
| `[…]`  | evaluated, these are the bindings         | reconcile to this set      |

`None` covers the stability-check early return ([`_parse_file`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L245-L251)), callback-only runs
([`_parse_file`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/processor.py#L260-L263)), and validation failure (below). It mirrors the `files_parsed=None` semantics
`persist_parsing_result` already uses ([`persist_parsing_result`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/airflow-core/src/airflow/dag_processing/manager.py#L1361-L1364)).
Without it, a transient parse failure would silently wipe every binding the file owns.

A candidate the fast path skips is answered from its recorded `task_handlers`, so the child resolves and validates every stub task of the file on every parse, and a list always holds all of the file's bindings. There is no separate "keep these" signal for the reconcile to get wrong.

**Scheduler → worker.** `ExecuteTask` and `StartupDetails` each gain one optional reference to the artifact that implements the stub task.

```python
class TaskHandlerArtifactRef(BaseModel):
    bundle_info: BundleInfo | None = None  # the artifact bundle; None: the task's own Dag bundle
    rel_path: str  # POSIX path within it


class ExecuteTask(BaseDagBundleWorkload):
    ti: TaskInstanceDTO
    dag_rel_path: os.PathLike[str]  # the Python Dag file, unchanged
    bundle_info: BundleInfo  # the Dag bundle, unchanged
    task_handler_artifact: TaskHandlerArtifactRef | None = None  # new
    ...


class StartupDetails(BaseModel):
    ti: TaskInstance
    dag_rel_path: str
    bundle_info: BundleInfo
    task_handler_artifact: TaskHandlerArtifactRef | None = None  # new, as the workload carries it
    ...
```

The scheduler always names the artifact's bundle, by name only: it sends no version and no `version_data`, which for the Dag's own bundle can be a whole object manifest. A reference that leaves `bundle_info` unset names the task's own Dag bundle as well, and the worker reads it the same way.

The task's own bundle is read at the version the run uses: its pinned version, or the version current when the task starts if the run is not pinned. For a pinned run, the artifact then matches the Dag code the run is pinned to. A named bundle carries no version, since artifact rows record none, so it resolves to the version current when the task starts. Either way the resolved version is pinned for the whole task. The worker decides "own bundle" by name, so an artifact held by the Dag's own bundle is read at the version the run uses, whether the coordinator's `task_handler_bundle_name` names that bundle or leaves it unset.

A workload without a reference names no artifact. That is the case for a Python task, a stub task on a queue no coordinator serves, a task of a Dag defined in a Lang SDK, a stub task with no recorded binding, and any stub task queued by a scheduler that lacks or cannot read the `[sdk]` configuration or its task handler Dag bundles. The scheduler never fails a task for a missing binding (see "Failure handling").

### Flow 1) Dag processing: the write path

Resolution is driven by the Python file's parse, after `PythonDagImporter` has produced the Dags.
The artifact never parses itself into these tables. That ordering is deliberate: a `dag_id` exists
before any row referencing it, so the foreign key holds and a cold start has no window in which a
stub Dag is validated against bindings that have not been written yet.

```
DagProcessorManager                                        [reads DB]
  │
  │  once per parsing loop, for every bundle that can hold task handlers
  │  (each routed coordinator's task_handler_bundle_name, plus the Dag
  │  bundles when one sets none and reads the task's own Dag bundle):
  │    SELECT bundle_name, relative_fileloc, size_bytes, cache_digest,
  │           task_handlers
  │      FROM lang_sdk_task_handler_artifact
  │     WHERE bundle_name IN (:those bundles)               ──▶ known_artifacts
  │
  │  per file about to be dispatched: the rows of the bundles in its scope
  │    (named bundles of its team, plus its own Dag bundle when a routed
  │    coordinator sets none)
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
        │      a stub task on a queue with no coordinator entry is left to an
        │      external worker: not checked, not bound; without
        │      queue_to_coordinator nothing is checked
        │      each coordinator reads its task_handler_bundle_name, or the Dag's
        │      own bundle when unset; a bundle of another team is an import
        │      error, since teams must be equal
        │
        ├─4─ per coordinator: list candidates in its bundle; a coordinator that
        │    cannot list its artifacts is an import error for its stub tasks
        │      Go   → files carrying the AFBNDL01 trailer magic, whatever
        │             their executable bit (one without it is rejected)
        │      Java → *.jar whose manifest carries Airflow-Cache-Digest
        │             (dependency JARs may declare Main-Class too)
        │      TS   → *.min.mjs whose first line starts with //# airflowBundle=
        │             (one with an invalid layout line is rejected)
        │      walk order is deterministic, so conflicts reproduce
        │
        ├─5─ FAST PATH, per candidate  (see "The fast path")
        │      size + stored cache_digest equal a known artifact's
        │                          ──▶ skip the launch, use its task_handlers
        │      anything else, or no stored digest  ──▶ probe
        │      rejected by the listing             ──▶ broken, no probe
        │
        ├─6─ PROBE, per differing candidate: one subprocess each, one at a time,
        │    all within 90% of [dag_processor] dag_file_processor_timeout,
        │    counted from the creation of the parse child; an answer is shared by
        │    every coordinator that lists the artifact, and a failed probe is
        │    retried under the next coordinator that lists it; a candidate
        │    whose probes all fail, or that is left when time runs out, is broken
        │      LangSDKTaskHandlerProcessorProcess.run(
        │          coordinator="jdk-17",
        │          path=<candidate>, bundle_path=…, bundle_name=…,
        │          artifact_rel_path=…, deadline=…)
        │        ──TaskHandlerParseRequest(file=…)──▶ runtime
        │        ◀─TaskHandlerParsingResult(task_handlers={"etl": […]})── runtime
        │        Get* from the runtime is relayed up ToManager unchanged
        │
        ├─7─ VALIDATE per stub task, against the recorded and the fresh answers
        │    of the coordinator its queue routes to
        │      exactly one declaration of its (dag_id, task_id); none, or two
        │      candidates claiming it → import error naming the candidates
        │      a handler with no stub task → not an error
        │      arg_bindings[*]         ↔ declaration.params[*], per its binding:
        │                                 by position, or by folded or exact name;
        │                                 an argless call passes no arguments;
        │                                 defaulted ones are dropped if that
        │                                 makes a positional count match
        │      arg_bindings[*].schema  ↔ declaration.params[*].value_schema,
        │                                 top-level JSON types, where both exist
        │      mapped stub task, or params=None → the handler's presence only
        │
        └─8─ on success → task_handler_bindings=[…]
             on mismatch → import_errors[etl.py]=…  AND  bindings=None
             either way  → probed_artifacts=[every fresh answer]
        │
        ├── DagFileParsingResult(serialized_dags=[…], import_errors={…},
        ▼                        task_handler_bindings=[…] | [] | None,
                                 probed_artifacts=[…])
DagProcessorManager.handle_parsing_result                  [writes DB]
  ├── record the probed artifacts, a transaction of their own, skipped when there are none    ← new
  │     lang_sdk_task_handler_artifact   UPSERT by (bundle_name, relative_fileloc):
  │                                      fingerprint, answer, last_probed_at
  │
  └── persist_parsing_result → update_dag_parsing_results_in_db — one transaction, in this order:
        1. add_dags / update_dags               → DagModel rows exist
        2. asset reference tables               (existing)
        3. lang_sdk_task_handler                reconcile by dag_id, artifact ids looked up by key   ← new
        4. SerializedDagModel / DagVersion / DagCode
        5. ParseImportError / DagWarning
```

The probed-artifact write must upsert: two parse children can probe the same artifact in the same loop and race on the unique key. It commits before the persist starts, so its exclusive row locks are gone before step 3 takes shared ones, and an answer is kept even when the persist fails.

Step 3 reconciles **by the `dag_id`s in the result**, not by file path: delete rows for those
`dag_id`s whose `task_id` is absent from the returned set, then insert or update the rest. Path-keyed
eviction breaks when a Dag moves between files — the old rows stay keyed to a path nothing parses any
more, and the primary key then blocks the new insert. With `dag_id` as the key a move simply updates
`dag_relative_fileloc`. A Dag removed from its file is only marked stale, so its rows stay until its `dag` row is deleted, and the `ON DELETE CASCADE` then removes them. Until then they keep their artifacts from the orphan sweep.

Step 3 writes nothing to the artifact table. A Dag bound to an artifact outside the Dag file's scope, or to one with no row because a concurrent orphan sweep deleted it, keeps its rows as they are, with a warning; in the second case the next parse finds no recorded answer and probes again.

Artifact rows are **never** evicted from one file's result. One artifact backs handlers owned by many
Python files, so this file seeing fewer candidates says nothing about another file's. They are a
cache; they are reclaimed by orphan sweep, never by a per-file reconcile.

### Flow 2) Scheduling: the read path

```
SchedulerJobRunner._critical_section_enqueue_task_instances    [reads DB]
  │
  ├── _select_task_instances_to_queue(…)
  │     the selected TIs are QUEUED, then made transient
  │
  ├── get_task_handler_artifact_refs(queued_tis, session=session)           ← new
  │     keep the TIs whose queue [sdk] queue_to_coordinator routes to a coordinator
  │     none, or no [sdk]?   → no query
  │     otherwise one SELECT, for every executor and every TI:
  │       lang_sdk_task_handler JOIN lang_sdk_task_handler_artifact
  │       WHERE (dag_id, task_id) IN (…)                      no row locks
  │     → {(dag_id, task_id): TaskHandlerArtifactRef(BundleInfo(name=artifact bundle), rel_path)}
  │
  ▼
SchedulerJobRunner._enqueue_task_instances_with_queued_state   [no further DB reads on ti]
  └── for ti in task_instances:
        dag_run finished?        → set_state(None); continue     (existing)
        no dag_version_id?       → warn; continue                (existing)
        │
        └── ExecuteTask.make(ti, task_handler_artifact=<the TI's entry, or None>)
              dag_rel_path            ← ti.dag_model.relative_fileloc   (existing)
              bundle_info             ← the Dag bundle at the run's bundle_version,
                                        unset for a run that is not pinned   (existing)
              task_handler_artifact   ← artifact bundle name + rel_path      ← new
              │
              └── executor.queue_workload(workload)
```

The read follows the selection, so it covers only the task instances that were queued, once per scheduling loop. It needs only columns the selected task instances already hold (`dag_id`, `task_id`, `queue`), so there is no relationship to load and nothing to detach.

The scheduler never fails a task for a missing binding. A routed task without one, such as a Python task, a task of a Dag defined in a Lang SDK, a task of another coordinator, or a stub task the Dag processor has not bound yet, is queued without a reference. A stub task on a queue no coordinator serves is not looked up at all.

The scheduler needs the same `[sdk]` configuration as the Dag processor, including the Dag bundles its coordinators name in `task_handler_bundle_name` in `[dag_processor] dag_bundle_config_list`. When it cannot read that configuration, it logs a warning on each scheduling loop that queues a task and queues every task without a reference.

### Flow 3) Task execution

```
executor worker process
  └── BaseExecutor.run_workload(workload)
        └── supervise_task(ti=…, bundle_info=…, dag_rel_path=…,
                           task_handler_artifact=workload.task_handler_artifact)   ← new
              │
              ├── coordinator = get_coordinator_manager().for_queue(ti.queue)
              │     unchanged: execution still routes on queue
              │
              └── coordinator.execute_task(what=ti, …, task_handler_artifact=…)
                    ├── bundle = initialize(the task's bundle_info when the reference has no bundle_info,
                    │                       or names the task's own bundle without a version;
                    │                       otherwise the reference's bundle_info)
                    │                                             ← the artifact's bundle
                    │                                               (the task's own, or a named one)
                    │   pinned and held under BundleVersionLock for the whole task
                    ├── bundle.path / rel_path is not a file in it?  → fail the task
                    │
                    └── SubprocessCoordinator._build_execute_task_command(
                            what=ti, task_handler_artifact=…)        ← signature change
                          │
                          ├── artifact = bundle.path / task_handler_artifact.rel_path
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
                                           task_handler_artifact)──▶ runtime
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

Validation is expensive (one subprocess per candidate) and a file is re-parsed every
`[dag_processor] min_file_process_interval` seconds, 30 by default. Re-probing unchanged artifacts
every 30 seconds forever is not acceptable, so a candidate is not probed while its recorded answer still holds.

The probe asks for every handler and the answer depends only on the artifact, so the answer recorded with a fingerprint holds for every Dag file that later sees that fingerprint, whichever file probed it. Each candidate is decided on its own, cheapest check first:

```
for each candidate in the coordinator's bundle:
    rejected by the listing (unusable artifact)          →  REPORT, never probe
    no known artifact at (bundle, relative path)         →  PROBE
    stat(candidate).st_size  ≠  known.size_bytes         →  PROBE
    stored cache_digest missing, or ≠ known.cache_digest →  PROBE
    otherwise                                            →  SKIP, use known.task_handlers
```

`mtime` is deliberately absent. It is reset by an object-store download and by container rebuilds, so it produces churn without adding certainty. Size plus digest is sufficient: the stored digest is read, not recomputed, so an artifact edited in place without a repack keeps it, and the free size check catches most such edits. Execution's integrity check stays the safety net.

**A new artifact is probed by the first file that lists it.** It has no recorded answer yet. Its answer lists every Dag it registers, so a collision on `(dag_id, task_id)` with any file's Dag is found when that file is next validated, even when another file probed the artifact first.

**The Python side is re-validated regardless.** The skip avoids the *subprocess*, not the comparison. The recorded `task_handlers` are kept precisely so a changed `.py` (a stub task that gained an argument) is compared against the cached declaration in process. Skipping the comparison as well would cache a verdict for a signature that no longer exists, and ship the mismatch to a worker as a runtime argument error instead of catching it as an import error.

### Failure handling

**Validation fails.** Record the import error; leave every binding row untouched. `bindings=None`
already expresses "do not reconcile", so this needs no additional mechanism. The answers probed for it are still recorded, so the next parse validates against them without probing. Like any import error, it stops every Dag of the file from being scheduled until it clears.

**A candidate is broken.** It is rejected by the listing, its probe fails or raises, or the parse runs out of time first. It is ignored: a stub task that finds its handler elsewhere is checked and bound as usual, and one left without a handler fails, its import error naming the broken candidate and why.

**A coordinator cannot be evaluated.** It cannot be built or cannot list its artifacts, or its bundle is missing, cannot be read or belongs to another team. Its stub tasks are not checked, and it is an import error naming it. An error the check does not expect is an import error of each Dag file with a routed stub task, and the serialized Dags are still sent.

**A name mismatch under named binding.** A passed argument no param takes, or a param no argument fills, is a warning in the Dag file's parse log; the stub task is still bound.

**A stub task has no binding at all.** The scheduler queues it without a reference and never fails a task. The binding can be missing for ordinary reasons: the Dag processor has not parsed the file since an upgrade, or a newer parse left the task unbound.

**Two artifacts claim one `(dag_id, task_id)`.** An import error against the Python file (the
definition whose author can act) naming both artifact paths, since the fix is in the deployment.

## Consequences

- Task execution stops searching. A worker resolves its artifact by path, from data that arrived on
  the workload, with no directory walk and no database read.
- Dag ids are never recorded at build time, so dynamic Dag rendering works: whatever the artifact
  registers when the Dag processor asks it is what gets recorded.
- `jars_root` / `executables_root` / `bundles_root` are removed. Artifacts inherit download, refresh
  and versioning from `DagBundle`. Deployments that mount artifacts themselves point a `LocalDagBundle` at the mount.
- Two new tables and one migration. `lang_sdk_task_handler` is reconciled on every parse of a file
  that owns rows in it; `lang_sdk_task_handler_artifact` is a cache with no per-file eviction.
- `ExecuteTask` and `StartupDetails` each grow one optional `TaskHandlerArtifactRef`. On the coordinator
  path the worker initializes the artifact's bundle, the task's own or a named one, in place of the Dag bundle, not in addition to it. Workload payloads grow by an artifact path and a bundle name.
- `DagFileParseRequest` gains a field and `DagFileParsingResult` two, and `ToSDKTaskHandlerProcessor`
  becomes a fifth union the supervisor-schema registry introspects. Both messages already appear in
  the generated schemas of all three SDKs, so the snapshot is regenerated and the two prek hooks guarding it run.
- Every Lang SDK runtime must answer `TaskHandlerParseRequest`.
- A stub task on a queue absent from `queue_to_coordinator` is left to a worker outside Airflow's coordinators:
  the parse neither checks nor binds it, and the scheduler does not look it up. Without `queue_to_coordinator`
  nothing is checked, so Python-only deployments are unchanged.
- The scheduler reads `[sdk]` and needs the task handler Dag bundles in `[dag_processor] dag_bundle_config_list`,
  but not the artifacts or a language runtime. Without `queue_to_coordinator`, scheduling is unchanged and costs
  no query. With it, a scheduling loop that queues a task on a routed queue costs one query, whatever the number
  of tasks or executors.
- A coordinator serving stub tasks must list and probe its artifacts, since a stub task it cannot bind cannot
  run.
- An artifact whose probe fails, or that the parse runs out of time for, has no recorded answer, so every Dag
  file with stub tasks on its coordinator probes it again on each parse until it is fixed or removed. One that
  several coordinators list is probed under each of them until one answers, so it can be probed once per
  coordinator on every parse. One the listing rejects is never probed: it is logged on each parse and named when
  a stub task finds no handler.
- The parse's team check only reports. The parse runs Dag code, so the manager enforces the scope when it
  records answers and bindings.
- Probes count towards `[dag_processor] dag_file_processor_timeout`, counted from the creation of the parse
  child, and each is also limited by `[core] dagbag_import_timeout`, which `get_dagbag_import_timeout` can set
  per artifact path. They run one at a time; the answers a parse got before running out of time are still
  recorded, so a cold start with many artifacts converges over parses, unless an artifact whose probe keeps
  failing slowly is probed ahead of them and leaves too little time. That artifact has no recorded answer, so
  it is probed first again on every parse, and the artifacts after it are asked only once it is fixed or removed,
  or the timeouts are raised. Guaranteed convergence would need the manager to record failed probes so that
  they are probed last, or a probe order that rotates between parses.
  If the manager kills the parse anyway, the kernel kills the runtime with it on Linux; processes the runtime
  started itself are not covered.
- One artifact bundle is one Java classpath. `_calculate_classpath` joins every JAR under the root
  ([`_calculate_classpath`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/java/coordinator.py#L85-L87)), so all handlers in a bundle
  share one dependency graph. Isolating conflicting dependency versions requires a second bundle, a
  second coordinator instance, and a second queue. This needs documenting.
- Steady-state parsing costs no subprocesses. A new or changed artifact costs one subprocess for each Dag file parsed before its answer is recorded, at most the children the manager starts in one loop iteration, and none after that. A Dag that fails validation does not cause a probe on every parse, since the answers it was checked against stay recorded, and an artifact that stores no cache digest is probed on every parse.
- No `DagVersion` coupling. An artifact rebuild does not bump a Dag's version, and a Dag edit does
  not invalidate an artifact fingerprint.
- Mixed-language stays Python-primary. A Lang-SDK runtime cannot declare stub tasks, and a native Dag
  cannot delegate a task to Python.
