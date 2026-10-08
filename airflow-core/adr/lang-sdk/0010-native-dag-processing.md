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

# ADR-0010: Native Dag Processing — DagImporter Registration and Routing

## Status

Proposed

## Context

`AbstractDagImporter` (AIP-85, `task-sdk/src/airflow/sdk/importers/`) makes the Dag processor aware of source formats beyond `.py`. It exposes `can_handle`, `list_dag_definitions`,
`import_definition` and `get_source_code`, and returns `DagImportResult.dags: list[DAG]` — `airflow.sdk.DAG` objects.

A Lang-SDK artifact can be one of those sources: a Dag authored entirely in Go or Java, with `Dag(spec)` on the SDK side. Parsing it means launching a runtime, which is what a
coordinator does. This ADR settles where that importer comes from, which coordinator instance backs it, and how the two avoid being configured twice.

`JavaDagImporter` and `JavaCoordinator` are the examples throughout. `ExecutableDagImporter` (Go) and `NodeDagImporter` (TypeScript) follow the same shape.

## Decision

### The importer finds its coordinator

```
CoordinatorDagImporter                        one subclass per runtime
    coordinator_classpath                     the coordinator class that parses and runs its files
    get_parsing_coordinator()                 the coordinator that parses its files in its Dag bundle
JavaDagImporter.coordinator_classpath = "airflow.sdk.coordinators.java.JavaCoordinator"
```

A coordinator does not hand out an importer. The importer names the class of coordinator that parses its files, and the registry builds it for one Dag bundle, as `cls(bundle_name=...)`. It looks its coordinator up in `[sdk] coordinators` when it needs one, so no coordinator is built to register it. `[sdk] coordinators` stays the one place a runtime is declared.

Which coordinator parses the importer's files in a Dag bundle depends on how many coordinators of its class are configured.

| Coordinators of the importer's class | What happens to the importer's files in the bundle |
|---|---|
| 0 | The importer is not registered, so the files are not listed. A deployment with no coordinator of the class is unchanged. |
| 1 | That coordinator parses them, in every Dag bundle. `[sdk] dag_bundle_to_coordinator` is not read. |
| 2 or more | `dag_bundle_to_coordinator[bundle]` must name one of them, and that one parses them. Otherwise each parse fails with an import error that says why: there is no entry, the entry names a coordinator of another class, the entry cannot be loaded, or the option is not a JSON object of strings. |

A coordinator is of the importer's class when its class is that class or a subclass of it. Both classpaths are resolved with `import_string` and compared with `issubclass`. Comparing strings would miss `airflow.sdk.coordinators.java.coordinator.JavaCoordinator`, which `airflow.sdk.coordinators.java.JavaCoordinator` re-exports, and a user subclass. A configured classpath that cannot be imported, fails while importing or is not a class matches nothing, and is logged once.

`task_handler_bundle_name` has nothing to do with parsing. It only names the Dag bundle that holds the artifacts of stub tasks.

### Registration order

```
DagImporterRegistry.from_config(bundle_name)
  ├── defaults                              PythonDagImporter, ZipImporter
  ├── COORDINATOR_DAG_IMPORTERS                                     (new)
  │     └── register(cls(bundle_name=bundle_name)) for each class that has a configured coordinator
  ├── [dag_processor] dag_importer_configs             (global, unchanged)
  └── that bundle's own `importers` list                   (unchanged)
```

`COORDINATOR_DAG_IMPORTERS` is a tuple of importer classpaths next to `CoordinatorDagImporter`. A bundle can still override `.jar` in a later tier.

The importer is tied to its bundle because the Java importer's listing filter needs the parsing coordinator's `main_class`, and `might_contain_dag` gets no bundle argument. For the same reason it cannot come in through `dag_importer_configs`, which cannot pass a bundle name. A third-party runtime registers its `CoordinatorDagImporter` subclass in each bundle's `importers` list, with `"kwargs": {"bundle_name": "<bundle>"}`. `dag_importer_configs` remains the door for importers with no runtime behind them, a YAML importer say.

When the tier fails:

- If `[sdk] coordinators` cannot be loaded, the tier logs it and registers nothing. Tasks cannot start in that case anyway, because `for_queue` fails first.
- Any other error is logged and kept on the registry, and `find_claiming_importer` re-raises it. A task routed to a coordinator then fails before its runtime starts, instead of running its file as a Python file. The Dag processor and the Python task runner log the error and treat the file as a Python file.

### `CoordinatorManager`

```
CoordinatorManager
  ├── for_queue(queue)                                  → one coordinator   (shipped, task execution)
  ├── get_coordinator(key)                              → the coordinator under a key
  ├── get_coordinator_keys_for_class(classpath)         → the keys of that class, building nothing
  └── get_dag_parsing_coordinator_key(classpath, bundle) → the key that parses the bundle's files of that class   (new)
```

`for_queue` answers "who runs this task" from `[sdk] queue_to_coordinator` alone. Tasks never read `dag_bundle_to_coordinator`: the supervisor and KubernetesExecutor pick the coordinator and the worker pod by queue, before the task runs. A native task therefore needs a coordinator of its file's class, not the one that parsed the file.

`from_config` does not read `dag_bundle_to_coordinator` either, because it runs for every task and a typo in a setting that only parsing uses must not fail Python tasks. The option is read on first use, and a bad value is reported as an import error on the files that need it.

### A Dag bundle maps to one coordinator

One entry in `dag_bundle_to_coordinator` picks one coordinator, so it only decides for that coordinator's runtime. With `{"dags-folder": "java-native"}`, four `JavaCoordinator`s and one `NodeCoordinator` named `ts`, the `.jar` files in `dags-folder` go to `java-native` and the `.min.mjs` files go to `ts`. With two `NodeCoordinator`s, the `.min.mjs` files get the import error until they move to a bundle of their own. An entry never breaks a runtime that has only one coordinator, even when it names a key that does not exist.

### The integration point is `import_definition`

An importer runs inside the Dag-parsing child, so a native Lang-SDK Dag is parsed by a process the importer itself starts. That process is
`LangSDKDagFileProcessorProcess`, which differs from the one the manager started only in the target callable it runs
([ADR-0012](0012-lang-sdk-parse-protocol.md)).

```
DagFileProcessorProcess(analytics.jar)                       ← manager spawns, as for any file
  └── _parse_file_entrypoint → _parse_file
        └── BundleDagBag → DagImporterRegistry.get_importer(".jar") → JavaDagImporter
              │
              └── JavaDagImporter.import_definition(definition, bundle=...)
                    │
                    ├── LangSDKDagFileProcessorProcess.start(
                    │       target=_parse_lang_sdk_dag_entrypoint,
                    │       coordinator=self.get_parsing_coordinator(), path=analytics.jar)
                    │     │
                    │     ├── in the child: _build_parse_dag_command() → (command, schema_version)
                    │     │                 coordinator.parse_dag() — spawn JVM, fd 0 ⇄ comm socket
                    │     │
                    │     │     ──DagFileParseRequest───▶ JVM        (ToDagProcessor)
                    │     │     ◀─DagFileParsingResult─── JVM        (ToManager)
                    │     │       ┌────────────────────────────────────────────────────┐
                    │     │       │  serialized_dags: ["java_report"]  (@Builder.Dag)  │
                    │     │       └────────────────────────────────────────────────────┘
                    │     │       TaskHandler registrations have no Dag to serialize —
                    │     │       no "etl" entry exists to be discarded.
                    │     │
                    │     └── Get* from the JVM relayed up to the manager unchanged
                    │
                    ├── LazyDeserializedDAG(data=...) → airflow.sdk.DAG
                    └── DagImportResult(dags=[DAG("java_report")])
              ▼
        _serialize_dags(bag) → DagFileParsingResult → manager
              ▼
        DagModelOperation → PERSIST "java_report" only
```

The coordinator is reached through the importer, never through the manager's file-to-process routing. ADR-0004's `_resolve_processor_target` scan, which picks a coordinator by
asking each one `can_handle_dag_file`, is superseded here: extension-keyed importer registration has already decided that `.jar` belongs to this coordinator, and two mechanisms
claiming the same file would have to agree.

For comparison, a pure Python file with no stub tasks:

```
PythonDagImporter.import_definition(definition, bundle=...)
  │
  ├── Parse → DAG objects
  ├── serialize_dag(dag)  →  no stub tasks, nothing to cross-validate
  ├── Return DagImportResult(dags=[dag])
  ▼
DagModelOperation → PERSIST
```

`DagImportResult.dags` is `list[DAG]`, so the importer wraps each serialized entry as a `LazyDeserializedDAG` and transforms it into an `airflow.sdk.DAG`.

### Interim: the Dag processor manager routes claimed files

Until importers own their parse process (#73457), the Dag processor manager routes a coordinator-claimed file itself, as of #74035. When a file's importer in the bundle's
registry is a coordinator's, the manager starts `LangSDKDagFileProcessorProcess` for it instead of `DagFileProcessorProcess`. That process execs the runtime, which connects
back to it. The importer's `import_definition` only reports that the Dag processor parses the file, which is what a Dag bag outside the Dag processor sees by default.

A Dag bag built with `parse_lang_sdk_files=True`, as `airflow tasks list` builds one, does not go through the importer for a coordinator-claimed file. It runs
`LangSDKDagFileProcessorProcess` itself, without an API client, and bags the Dags the runtime serialized as `SerializedDAG` objects. `DagImportResult.dags` stays `list[DAG]`.

This is a bridge, not the end state. Running the runtime inside the Python parse child would need that child to relay every request and reply between the runtime and the
manager over its fd 0. Starting the runtime from the manager lets it reach the manager's request handlers directly, with no relay.

## Consequences

- The Java and TypeScript importers are never configured by hand. The runtime is declared once, in `[sdk] coordinators`, and the importer follows from it.
- Routing a Lang-SDK artifact to its runtime becomes the importer registry's job. ADR-0004's `can_handle_dag_file` / `_resolve_processor_target` scan no longer decides which
  process parses a file, and the coordinator method it drove is replaced by `parse_dag` ([ADR-0012](0012-lang-sdk-parse-protocol.md)).
- A runtime with several coordinators needs one `dag_bundle_to_coordinator` entry for each Dag bundle that holds its native Dag files, and such a bundle holds the native Dag files of only that runtime.
- `task_handler_bundle_name` only names the bundle that holds the artifacts of stub tasks. It does not decide which coordinator parses a file.
- `CoordinatorManager` gains `get_coordinator`, `get_coordinator_keys_for_class` and `get_dag_parsing_coordinator_key`, a lookup by Dag bundle beside `for_queue`.
- A third-party runtime registers its importer in each bundle's `importers` list, because `dag_importer_configs` cannot pass a bundle name.
- A packed Go bundle claims the empty extension, which three call sites currently treat as absent rather than as a key. Appendix B lists them.
- `get_source_code` is abstract, so every Lang-SDK importer must implement it, and a native Lang-SDK Dag has no Python source to return. What it should return, and how that squares
  with [ADR-0006](0006-no-lang-sdk-source-display.md), is not settled here.
- A native Dag crosses serialization twice over: the runtime serializes it, the importer deserializes it into an `airflow.sdk.DAG`, and the enclosing parse serializes it again.
  That round trip is the price of `DagImportResult.dags` being `list[DAG]` — the same cost AIP-85's `list[DAG]` / `list[LazyDeserializedDAG]` question is about.
- Parsing a native Dag costs two processes, not one: the Dag-processing child the manager already starts, plus the runtime the importer starts inside it. The inner one inherits its
  request, result and logging from `DagFileProcessorProcess`, so the only Lang-SDK-specific code is the target callable.

## References

- [ADR-0012](0012-lang-sdk-parse-protocol.md) — `parse_dag` and the coordinator interface it belongs to
- [ADR-0011](0011-mixed-language-dag-processing.md) — why `TaskHandler` registrations never reach a `DagImporter`
- [ADR-0003](0003-pure-java-dags.md) — `BundleScanner` / `BuilderProcessor`, build-time artifact inventory
- [ADR-0004](0004-dag-parsing.md) — `can_handle_dag_file`, the subprocess bridge
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
- `task-sdk/src/airflow/sdk/importers/` — `AbstractDagImporter`, `DagImportResult`, `DagImporterRegistry`
- `airflow-core/src/airflow/dag_processing/processor.py` — `DagFileProcessorProcess`, the class `LangSDKDagFileProcessorProcess` extends
- [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) — DagImporter
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs

## Appendix

### Appendix A: Two or more coordinators and no usable entry

Each file of the importer fails its parse with an import error. Three outcomes were possible:

- **Skip the importer.** The files stop being listed. Their Dags are marked stale and their import errors are deleted, with only a log line to show for it.
- **Raise while building the registry.** The manager skips the whole bundle, Python files included.
- **Import error per file (chosen).** The Dags also go stale, but the UI says why, and the next good parse brings them back.

### Appendix B: Extensionless artifacts

A packed Go bundle has no suffix. `ExecutableDagImporter` claims the empty extension as a first-class key rather than depending on a `can_handle` scan, whose winner varies with
registration order because `_ordered_importers` is scanned in reverse.

Three places assume a non-empty suffix today:

- `_normalize_extensions` rewrites `""` to `"."`.
- `get_importer` and `can_handle` guard on `if suffix:`, which skips the extension map entirely for an extensionless file.
- `find_file_dag_definitions` filters on `path.suffix.lower()`.

Empty has to pass through all three, with the guards testing `suffix is not None`.

### Appendix C: Resolving artifact roots at parse time

`_resolve_artifact_bundle` resolves the Dag bundle of a task, and `execute_task` publishes its root through `_get_scan_roots()`, which is scoped to an active task and raises outside one.
Both parse-side commands need the same roots with no `TaskInstance` in hand.
For a parse they are the root of the Dag bundle the Dag processor is parsing, whatever the coordinator's `task_handler_bundle_name` says.
The scope that publishes the roots has to open for a parse as well as for a task.
