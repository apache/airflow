/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Turns a Dag declared in TypeScript into Airflow's DagSerialization v3 JSON —
// what the Dag processor stores and the scheduler reads. The format is
// Airflow-internal rather than an SDK schema, so it is reimplemented per
// language against `airflow-core/src/airflow/serialization/schema.json`; see
// `airflow-core/adr/lang-sdk/0004-dag-parsing.md` for the field table this
// follows, and the Java SDK's `Serde.kt` for the same job in another language.
//
// Byte-parity with Python's serializer is not the goal — Python omits fields
// against a `client_defaults` table this SDK does not receive. What has to hold
// is that `DagSerialization.from_dict` rebuilds the same Dag, which
// tests/coordinator/conformance.test.ts pins against recorded Python output.
//
// This module only produces the payload. Answering a Dag-parsing request with
// it is the bundle's job, once the coordinator has a parse request to answer.

import { relative as relativePath } from "node:path";

import {
  DAG_SCHEMA_FIELDS,
  TASK_SCHEMA_FIELDS,
  type SchemaField,
} from "../generated/dag-schema-fields.js";
import type { JsonValue } from "../sdk/client-types.js";
import {
  getDagOrderEdges,
  getDagTaskGroups,
  getDagTaskInputs,
  getDagTaskRecords,
  isTaskRef,
  type Dag,
  type RecordedInputs,
  type TaskGroupRecord,
  type TaskRecord,
} from "../sdk/dag.js";
import type { OperatorRef } from "../sdk/operators.js";

/** A serialized Dag: JSON, by the time it reaches the supervisor as msgpack. */
type SerializedValue = JsonValue;

/** Airflow's type/var encoding, as `BaseSerialization.serialize()` emits it. */
interface TypeEncoded {
  readonly __type: string;
  readonly __var: SerializedValue;
}

/**
 * Identity every TypeScript task carries, in place of the Python operator class
 * a Python Dag would name.
 *
 * Fixed rather than derived: nothing on the Airflow side imports `_task_module`
 * — `SerializedBaseOperator.populate_operator` only compares the pair as
 * strings when matching plugin extra links — so the pair is free to name the
 * coordinator that actually runs the task, which makes every TypeScript task
 * greppable in the UI and the metadata DB.
 */
const TASK_TYPE = "TypeScriptOperator";
const TASK_MODULE = "airflow.sdk.coordinators.node";

/**
 * Marks the tasks this SDK serialized, as the Java SDK marks its own.
 *
 * Nothing in airflow-core reads it today; the `operator` schema definition
 * allows additional properties, so it rides along as a marker for tooling that
 * wants to tell language-native tasks apart without parsing `_task_module`.
 */
const TASK_LANGUAGE = "typescript";

/**
 * The Dags this one reaches, as the UI's dependency graph and
 * `airflow dags show-dependencies` read them.
 *
 * Python derives these from the live operator with `detect_dependencies`. Only
 * a trigger task creates one here, since it is the one operator this SDK
 * declares that names another Dag.
 */
function serializeDagDependencies(dag: Dag): SerializedValue {
  const dependencies: SerializedValue[] = [];
  for (const [taskId, record] of getDagTaskRecords(dag)) {
    const target = record.operator && PYTHON_OPERATORS[record.operator.taskType]?.triggersDagIn;
    if (!target) continue;
    const triggered = record.operator?.args[target];
    if (typeof triggered !== "string") continue;
    dependencies.push({
      source: dag.dagId,
      target: triggered,
      label: taskId,
      dependency_type: "trigger",
      dependency_id: taskId,
    });
  }
  return dependencies;
}

/** What a serialized task needs from a Python operator class this SDK declares. */
interface PythonOperator {
  /** Fields the server renders Jinja in, as the class lists them. */
  readonly templateFields: readonly string[];
  /** Node colour in the graph view. */
  readonly uiColor: string;
  /** How the UI renders a field's value, keyed by field. */
  readonly templateFieldsRenderers?: Readonly<Record<string, string>>;
  /** Extra links on the task, as `_serialize_operator_extra_links` writes them. */
  readonly extraLinks?: Readonly<Record<string, string>>;
  /** Argument naming the Dag this operator triggers, for `dag_dependencies`. */
  readonly triggersDagIn?: string;
}

/**
 * Each Python operator this SDK can declare, as Airflow would serialize it.
 *
 * Python's serializer reads all of this off the live operator class. A
 * TypeScript bundle cannot import one, and the Dag processor takes these from
 * the serialized task rather than re-importing, so they are written here. Keep
 * in step with the class the DSL is written against; the values are those of
 * the Python class of the same name.
 */
const PYTHON_OPERATORS: Readonly<Record<string, PythonOperator>> = {
  // airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator
  TriggerDagRunOperator: {
    templateFields: [
      "trigger_dag_id",
      "trigger_run_id",
      "logical_date",
      "conf",
      "wait_for_completion",
      "skip_when_already_exists",
    ],
    uiColor: "#ffefeb",
    templateFieldsRenderers: { conf: "py" },
    extraLinks: { "Triggered DAG": "_link_TriggerDagRunLink" },
    triggersDagIn: "trigger_dag_id",
  },
};

// Python resolves these from [core]/[scheduler] config when the Dag leaves them
// unset, and its serializer always writes the resolved value — there is no
// schema default to omit against. A bundle cannot read airflow.cfg, so the
// stock defaults stand in.
const DAG_CONFIG_FALLBACKS: Readonly<Record<string, SerializedValue>> = {
  max_active_tasks: 16, // [core] max_active_tasks_per_dag
  max_active_runs: 16, // [core] max_active_runs_per_dag
  max_consecutive_failed_dag_runs: 0,
  catchup: false, // [scheduler] catchup_by_default
  disable_bundle_versioning: false,
};

/** How one set of authoring fields is written into a serialized object. */
interface FieldRules {
  readonly fields: Readonly<Record<string, SchemaField>>;
  /**
   * Fields whose {__type, __var} wrapper survives; every other field is
   * unwrapped to the bare __var, as Python's `serialize_to_json` does.
   *
   * Neither set overlaps the generated authoring fields today, so in practice
   * everything is unwrapped. They are named so that a decorated field added to
   * the schema later takes the right path rather than silently losing its
   * wrapper. Source: `DagSerialization._decorated_fields` and
   * `OperatorSerialization._decorated_fields`.
   */
  readonly decorated: ReadonlySet<string>;
  /** Fields never written, whatever the author set. */
  readonly omitted: ReadonlySet<string>;
}

const DAG_FIELD_RULES: FieldRules = {
  fields: DAG_SCHEMA_FIELDS,
  decorated: new Set(["default_args", "access_control"]),
  omitted: new Set(),
};

const TASK_FIELD_RULES: FieldRules = {
  fields: TASK_SCHEMA_FIELDS,
  decorated: new Set(["executor_config"]),
  // Python drops both unless the operator names an email recipient
  // (`OperatorSerialization._serialize_node`). A TypeScript task has no `email`
  // field to name one, so writing them would describe a notification that can
  // never be sent.
  omitted: new Set(["email_on_failure", "email_on_retry"]),
};

const NULL_TIMETABLE = "airflow.timetables.simple.NullTimetable";
const ONCE_TIMETABLE = "airflow.timetables.simple.OnceTimetable";
const CONTINUOUS_TIMETABLE = "airflow.timetables.simple.ContinuousTimetable";
const CRON_TIMETABLE = "airflow.timetables.trigger.CronTriggerTimetable";

/** Serialize one Dag to the `dag` object of a DagSerialization v3 payload. */
export function serializeDag(
  dag: Dag,
  fileloc: string,
  relativeFileloc: string,
): Record<string, SerializedValue> {
  const graph = buildDagGraph(dag);
  const inputs = getDagTaskInputs(dag);
  const data: Record<string, SerializedValue> = {
    dag_id: dag.dagId,
    fileloc,
    relative_fileloc: relativeFileloc,
    timezone: "UTC",
    timetable: serializeTimetable(dag.spec.schedule, dag.dagId),
    tasks: [...getDagTaskRecords(dag)].map(([taskId, record]) =>
      serializeTask(
        dag.dagId,
        taskId,
        record,
        graph.downstreamTaskIds.get(taskId),
        inputs.get(taskId),
        dag.spec.queue,
      ),
    ),
    dag_dependencies: serializeDagDependencies(dag),
    task_group: serializeTaskGroups(dag, graph),
    edge_info: {},
    params: [],
    // Always written by Python's serializer, so a Dag without either still
    // round-trips to the same object.
    deadline: null,
    allowed_run_types: null,
  };
  applySchemaFields(data, dag.spec, DAG_FIELD_RULES, `Dag "${dag.dagId}"`);
  for (const [key, fallback] of Object.entries(DAG_CONFIG_FALLBACKS)) {
    data[key] ??= fallback;
  }
  return data;
}

/**
 * The task's spec with the Dag's queue filled in, when the task named none.
 *
 * Merged before the fields are written rather than after, so a queue that
 * happens to equal the schema default is omitted the way any other defaulted
 * field is.
 */
function withDagQueue(spec: object, dagQueue: string | undefined): object {
  if (dagQueue === undefined || "queue" in spec) return spec;
  return { ...spec, queue: dagQueue };
}

/** Serialize one task, with its downstream edges sorted for a stable payload. */
function serializeTask(
  dagId: string,
  taskId: string,
  record: TaskRecord,
  downstream: ReadonlySet<string> | undefined,
  inputs: RecordedInputs | undefined,
  dagQueue: string | undefined,
): SerializedValue {
  const { operator } = record;
  const data: Record<string, SerializedValue> = operator
    ? {
        task_id: taskId,
        // The Python class the worker imports, and the arguments it is built
        // with. No `language` marker: this task does not run in TypeScript.
        task_type: operator.taskType,
        _task_module: operator.taskModule,
        ...serializePythonOperator(operator, taskId, dagId),
      }
    : {
        task_id: taskId,
        task_type: TASK_TYPE,
        _task_module: TASK_MODULE,
        language: TASK_LANGUAGE,
        // Python's operator serializer always emits this — its list value never
        // matches the tuple default it is compared against. A TypeScript task has
        // no Jinja templating, so the list is empty rather than absent.
        template_fields: [],
        // What marks a task whose arguments the API server resolves per instance
        // and sends to a foreign runtime, as `@task.stub` does on the Python side.
        // `get_arg_bindings` reads nothing without it.
        is_stub: true,
      };
  if (!operator) {
    const bindings = serializeArgBindings(inputs);
    if (bindings) data["_arg_bindings"] = bindings;
  }
  // What Python writes for a SkipMixin operator, and what makes
  // `NotPreviouslySkippedDep` consult this task's `skipmixin_key` XCom when one
  // of its skipped downstream tasks is cleared.
  if (record.canSkipDownstream === true) data["_can_skip_downstream"] = true;
  // A Python worker has to be able to pick an operator task up, so it inherits
  // no queue from a Dag whose other tasks are routed to this coordinator. An
  // explicit `queue` on its own spec still stands.
  const spec = operator ? record.spec : withDagQueue(record.spec, dagQueue);
  applySchemaFields(data, spec, TASK_FIELD_RULES, `task "${taskId}" of Dag "${dagId}"`);
  if (downstream?.size) {
    data["downstream_task_ids"] = [...downstream].sort();
  }
  return { __type: "operator", __var: data };
}

/**
 * The fields a Python operator task carries beyond its class: the arguments it
 * is built with, and what the UI needs to draw it.
 *
 * Python's serializer takes the last three off the live operator class, which a
 * TypeScript bundle cannot import, so they come from the same table the
 * operator's own `template_fields` do. Every argument goes through
 * {@link serializeValue}, so a value the Dag JSON cannot carry is rejected here
 * rather than reaching the scheduler.
 */
function serializePythonOperator(
  operator: OperatorRef,
  taskId: string,
  dagId: string,
): Record<string, SerializedValue> {
  const python = PYTHON_OPERATORS[operator.taskType];
  if (python === undefined) {
    throw new Error(
      `Task "${taskId}" of Dag "${dagId}" names the Python operator "${operator.taskType}", ` +
        "which this SDK has no serialization table for",
    );
  }
  const args: Record<string, SerializedValue> = {};
  for (const [keyword, value] of Object.entries(operator.args)) {
    args[keyword] = plainJsonArg(value, `${keyword} of task "${taskId}" of Dag "${dagId}"`);
  }
  return {
    // Rendering stays server-side, where it already happens, so a Jinja string
    // in an argument passes through untouched.
    template_fields: [...python.templateFields],
    ui_color: python.uiColor,
    ...(python.templateFieldsRenderers && {
      template_fields_renderers: { ...python.templateFieldsRenderers },
    }),
    ...(python.extraLinks && { _operator_extra_links: { ...python.extraLinks } }),
    ...args,
  };
}

/**
 * One Python operator argument, checked to be JSON the Dag JSON can carry.
 *
 * Plain JSON, not {@link serializeValue}'s `{__type, __var}` encoding: Python's
 * serializer writes an operator's arguments through as they are, so a `conf`
 * dict is a dict on the wire. A value JSON cannot carry — a `Date`, a `Map`,
 * a function — is rejected here, because msgpack would otherwise hand Airflow
 * something it cannot store.
 */
function plainJsonArg(value: unknown, label: string): SerializedValue {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error(`${label} is not a finite number`);
    return value;
  }
  if (Array.isArray(value))
    return value.map((item, index) => plainJsonArg(item, `${label}[${index}]`));
  if (isPlainObject(value)) {
    const copy: Record<string, SerializedValue> = {};
    for (const [key, item] of Object.entries(value))
      copy[key] = plainJsonArg(item, `${label}.${key}`);
    return copy;
  }
  throw new Error(
    `${label} is ${describeType(value)}, which a Python operator argument cannot carry; ` +
      "pass a string, a number, a boolean, null, an array or a plain object",
  );
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/**
 * The task's arguments as the binding spec the API server hands back at run
 * time, one entry per argument in the order the call listed them.
 *
 * A reference becomes an `xcom` binding naming the upstream task, and anything
 * else a `literal` carrying the value, matching the `TaskArgBinding` union the
 * execution API declares. `value_schema` is left out: it constrains the decode
 * side, and nothing records a TypeScript argument's type at pack time, so
 * omitting it says "unconstrained" rather than asserting a wrong type.
 *
 * `undefined` for a task called with no arguments, which needs no spec.
 */
function serializeArgBindings(inputs: RecordedInputs | undefined): SerializedValue | undefined {
  if (inputs === undefined) return undefined;
  const entries = Object.entries(inputs);
  if (entries.length === 0) return undefined;
  return entries.map(([name, value]): SerializedValue =>
    isTaskRef(value)
      ? { name, kind: "xcom", task_id: value.taskId }
      : { name, kind: "literal", value: serializeValue(value) },
  );
}

/**
 * The Dag's task-group tree, rooted at the group Python builds for every Dag.
 *
 * `children[label] = [kind, value]`, as `serialize_for_task_group()` writes it:
 * a task is `["operator", task_id]` and a nested group is `["taskgroup", ...]`
 * holding that group's own object, so the tree nests by embedding rather than
 * by reference. `_group_id` is the group's *local* segment, not its qualified
 * id, matching what Python records.
 */
function serializeTaskGroups(dag: Dag, graph: DagGraph): SerializedValue {
  const groups = getDagTaskGroups(dag);
  const grouped = new Set<string>();
  for (const group of groups.values()) {
    for (const taskId of group.taskIds) grouped.add(taskId);
  }
  const rootTaskIds = [...getDagTaskRecords(dag).keys()].filter((id) => !grouped.has(id));
  const rootGroupIds = [...groups.values()]
    .filter((group) => group.parentGroupId === undefined)
    .map((group) => group.groupId);

  return taskGroupObject(null, rootTaskIds, rootGroupIds, groups, graph);
}

/** One group object: the root when `groupId` is null, otherwise a nested one. */
function taskGroupObject(
  groupId: string | null,
  taskIds: readonly string[],
  childGroupIds: readonly string[],
  groups: ReadonlyMap<string, TaskGroupRecord>,
  graph: DagGraph,
): SerializedValue {
  const children: Record<string, SerializedValue> = {};
  for (const taskId of taskIds) {
    children[taskId] = ["operator", taskId];
  }
  for (const childId of childGroupIds) {
    const child = groups.get(childId)!;
    children[childId] = [
      "taskgroup",
      taskGroupObject(childId, child.taskIds, child.childGroupIds, groups, graph),
    ];
  }
  const own = groupId === null ? undefined : graph.groupEdges.get(groupId);
  return {
    // The local segment: Python's TaskGroup stores the id it was given, and
    // rebuilds the qualified one from where the group sits in the tree.
    _group_id: groupId === null ? null : localGroupId(groupId),
    group_display_name: "",
    prefix_group_id: true,
    tooltip: "",
    ui_color: "CornflowerBlue",
    ui_fgcolor: "#000",
    children,
    upstream_group_ids: sorted(own?.upstreamGroups),
    downstream_group_ids: sorted(own?.downstreamGroups),
    upstream_task_ids: sorted(own?.upstreamTasks),
    downstream_task_ids: sorted(own?.downstreamTasks),
  };
}

function localGroupId(groupId: string): string {
  const cut = groupId.lastIndexOf(GROUP_SEPARATOR);
  return cut === -1 ? groupId : groupId.slice(cut + 1);
}

function sorted(values: ReadonlySet<string> | undefined): string[] {
  return [...(values ?? [])].sort();
}

const GROUP_SEPARATOR = ".";

interface GroupEdgeSets {
  readonly upstreamGroups: Set<string>;
  readonly downstreamGroups: Set<string>;
  readonly upstreamTasks: Set<string>;
  readonly downstreamTasks: Set<string>;
}

/** Both views of a Dag's edges: the task graph, and what each group records. */
interface DagGraph {
  /** Each task's downstream task IDs, with every group endpoint expanded. */
  readonly downstreamTaskIds: Map<string, Set<string>>;
  readonly groupEdges: Map<string, GroupEdgeSets>;
}

/**
 * Resolve a Dag's two kinds of edge into the two views a serialized Dag holds.
 *
 * An order-only edge with a group at either end lands in both: the group object
 * records it for the UI, and the task graph records it expanded, because the
 * scheduler only ever reads task-to-task edges. A group expands to its *roots*
 * when it is downstream and its *leaves* when it is upstream — an edge into a
 * group reaches the tasks that start it, and one out of a group leaves from the
 * tasks that finish it — which is what `TaskGroup.set_upstream` does in Python.
 */
function buildDagGraph(dag: Dag): DagGraph {
  const groups = getDagTaskGroups(dag);
  const downstreamTaskIds = new Map<string, Set<string>>();
  const groupEdges = new Map<string, GroupEdgeSets>();
  const link = (upstream: string, downstream: string): void => {
    // Two arguments fed by the same upstream are one edge, as is an order-only
    // edge redeclaring one the wiring already drew.
    const edges = downstreamTaskIds.get(upstream) ?? new Set<string>();
    edges.add(downstream);
    downstreamTaskIds.set(upstream, edges);
  };

  for (const [taskId, inputs] of getDagTaskInputs(dag)) {
    for (const value of Object.values(inputs)) {
      if (isTaskRef(value)) link(value.taskId, taskId);
    }
  }
  const orderEdges = getDagOrderEdges(dag);
  for (const { upstream, downstream } of orderEdges) {
    if (!groups.has(upstream) && !groups.has(downstream)) link(upstream, downstream);
  }

  // Roots and leaves are read off the task-to-task graph, which holds every
  // edge that can sit inside a group by now: wiring, and any order-only edge
  // between two tasks. Python resolves them at `>>` time and so is equally
  // order-sensitive, which is what keeps the two in step.
  const ends = new GroupEnds(groups, downstreamTaskIds);

  // Which group edges each endpoint has, for stepping over a group that holds
  // no tasks.
  const upstreamsOf = new Map<string, Set<string>>();
  const downstreamsOf = new Map<string, Set<string>>();
  for (const { upstream, downstream } of orderEdges) {
    addTo(downstreamsOf, upstream, downstream);
    addTo(upstreamsOf, downstream, upstream);
  }

  /**
   * The tasks an edge endpoint stands for: the task itself, or a group's leaves
   * when it is upstream and its roots when it is downstream.
   *
   * A group holding no tasks has neither, so the edge steps over it and
   * continues along the group edges beyond — `x >> empty >> y` still runs `y`
   * after `x`, as Python's `find_leaves` walk does.
   */
  const tasksAt = (id: string, side: "upstream" | "downstream"): string[] => {
    if (!groups.has(id)) return [id];
    const own = side === "upstream" ? ends.leaves(id) : ends.roots(id);
    return own.length > 0 ? own : tasksBeyond(id, side, new Set());
  };
  const tasksBeyond = (
    id: string,
    side: "upstream" | "downstream",
    seen: Set<string>,
  ): string[] => {
    if (seen.has(id)) return [];
    seen.add(id);
    const next = side === "upstream" ? upstreamsOf.get(id) : downstreamsOf.get(id);
    return [...(next ?? [])].flatMap((other) => {
      if (!groups.has(other)) return [other];
      const own = side === "upstream" ? ends.leaves(other) : ends.roots(other);
      return own.length > 0 ? own : tasksBeyond(other, side, seen);
    });
  };
  const setsFor = (groupId: string): GroupEdgeSets => {
    let sets = groupEdges.get(groupId);
    if (!sets) {
      sets = {
        upstreamGroups: new Set(),
        downstreamGroups: new Set(),
        upstreamTasks: new Set(),
        downstreamTasks: new Set(),
      };
      groupEdges.set(groupId, sets);
    }
    return sets;
  };

  for (const { upstream, downstream } of orderEdges) {
    const upstreamIsGroup = groups.has(upstream);
    const downstreamIsGroup = groups.has(downstream);
    if (!upstreamIsGroup && !downstreamIsGroup) continue;

    const from = tasksAt(upstream, "upstream");
    const to = tasksAt(downstream, "downstream");
    for (const tail of from) {
      for (const head of to) link(tail, head);
    }

    if (downstreamIsGroup) {
      const sets = setsFor(downstream);
      for (const tail of from) sets.upstreamTasks.add(tail);
      if (upstreamIsGroup) sets.upstreamGroups.add(upstream);
    }
    // Only a group whose downstream is a plain task records it as a task; when
    // both ends are groups the pair is recorded as a group edge on this side
    // and as the expanded tasks on the other, which is how Python leaves it.
    if (upstreamIsGroup) {
      const sets = setsFor(upstream);
      if (downstreamIsGroup) sets.downstreamGroups.add(downstream);
      else sets.downstreamTasks.add(downstream);
    }
  }
  return { downstreamTaskIds, groupEdges };
}

function addTo(index: Map<string, Set<string>>, key: string, value: string): void {
  const existing = index.get(key) ?? new Set<string>();
  existing.add(value);
  index.set(key, existing);
}

/** The tasks an edge reaches when it points at a group, cached per group. */
class GroupEnds {
  readonly #groups: ReadonlyMap<string, TaskGroupRecord>;
  readonly #downstream: ReadonlyMap<string, ReadonlySet<string>>;
  readonly #members = new Map<string, Set<string>>();

  constructor(
    groups: ReadonlyMap<string, TaskGroupRecord>,
    downstream: ReadonlyMap<string, ReadonlySet<string>>,
  ) {
    this.#groups = groups;
    this.#downstream = downstream;
  }

  /** Tasks in the group with no upstream inside it: where an edge in arrives. */
  roots(groupId: string): string[] {
    const members = this.#membersOf(groupId);
    const hasInternalUpstream = new Set<string>();
    for (const [upstream, downstream] of this.#downstream) {
      if (!members.has(upstream)) continue;
      for (const task of downstream) {
        if (members.has(task)) hasInternalUpstream.add(task);
      }
    }
    return [...members].filter((task) => !hasInternalUpstream.has(task));
  }

  /** Tasks in the group with no downstream inside it: where an edge out leaves. */
  leaves(groupId: string): string[] {
    const members = this.#membersOf(groupId);
    return [...members].filter(
      (task) => ![...(this.#downstream.get(task) ?? [])].some((other) => members.has(other)),
    );
  }

  /** Every task the group holds, nested groups included. */
  #membersOf(groupId: string): Set<string> {
    const cached = this.#members.get(groupId);
    if (cached) return cached;
    const members = new Set<string>();
    const pending = [groupId];
    for (let i = 0; i < pending.length; i += 1) {
      const group = this.#groups.get(pending[i]!);
      if (!group) continue;
      for (const taskId of group.taskIds) members.add(taskId);
      pending.push(...group.childGroupIds);
    }
    this.#members.set(groupId, members);
    return members;
  }
}

/**
 * Lower a `schedule` onto the timetable the scheduler reconstructs.
 *
 * Only the four schedules that map to a stock timetable are accepted. Anything
 * else — an asset expression, a custom timetable — is a Python object the
 * scheduler has to import, which a TypeScript bundle cannot name, so it is
 * rejected here rather than serialized into a Dag that fails to deserialize.
 */
function serializeTimetable(schedule: unknown, dagId: string): SerializedValue {
  if (schedule === undefined || schedule === null) {
    return simpleTimetable(NULL_TIMETABLE);
  }
  if (typeof schedule !== "string") {
    throw new Error(
      `schedule for Dag "${dagId}" must be "@once", "@continuous", or a cron expression; ` +
        `${describeType(schedule)} schedule names a Python object this SDK cannot serialize`,
    );
  }
  if (schedule.trim() === "") {
    throw new Error(
      `schedule for Dag "${dagId}" is empty; leave it unset for a Dag with no schedule`,
    );
  }
  if (schedule === "@once") return simpleTimetable(ONCE_TIMETABLE);
  if (schedule === "@continuous") return simpleTimetable(CONTINUOUS_TIMETABLE);
  const expression = CRON_PRESETS[schedule] ?? schedule;
  if (!isCronExpression(expression)) {
    throw new Error(
      `schedule ${JSON.stringify(schedule)} for Dag "${dagId}" is not a cron expression or a ` +
        `preset (${Object.keys(CRON_PRESETS).join(", ")}, @once, @continuous); a schedule the ` +
        "scheduler cannot parse would leave the Dag unschedulable",
    );
  }
  // TODO: honour [scheduler] create_cron_data_intervals, which switches Python
  // to CronDataIntervalTimetable. A bundle cannot read airflow.cfg, so the
  // supervisor has to send the flag first; tracked at
  // https://github.com/apache/airflow/issues/67938
  return {
    __type: CRON_TIMETABLE,
    __var: { expression, timezone: "UTC", interval: 0, run_immediately: false },
  };
}

/**
 * Presets expanded the way `CronMixin.__init__` expands them, so the serialized
 * expression is the one Python records — which the Dag's summary and its hash
 * are both taken from. Mirrors `airflow.utils.dates.cron_presets`.
 */
const CRON_PRESETS: Readonly<Record<string, string>> = {
  "@hourly": "0 * * * *",
  "@daily": "0 0 * * *",
  "@weekly": "0 0 * * 0",
  "@monthly": "0 0 1 * *",
  "@quarterly": "0 0 1 */3 *",
  "@yearly": "0 0 1 1 *",
};

/**
 * Whether `expression` has the shape croniter accepts: five or six
 * space-separated fields of cron characters.
 *
 * A shape check, not a parse: croniter validates the ranges, and repeating that
 * here would be a second implementation to keep in step. What it does catch is
 * prose — `"every tuesday"` — which would otherwise be written into a Dag that
 * the scheduler then fails to build a timetable for.
 */
function isCronExpression(expression: string): boolean {
  const fields = expression.trim().split(/\s+/);
  if (fields.length !== 5 && fields.length !== 6) return false;
  return fields.every(
    (field) => /^[\d*,\-/?LW#]+$/i.test(field) || /^[A-Z]{3}(-[A-Z]{3})?$/i.test(field),
  );
}

function simpleTimetable(type: string): SerializedValue {
  return { __type: type, __var: {} };
}

/**
 * Write the fields a spec set onto `data`, skipping any left at its schema
 * default — Python's serializer omits what the scheduler re-derives.
 */
function applySchemaFields(
  data: Record<string, SerializedValue>,
  spec: object,
  rules: FieldRules,
  label: string,
): void {
  const values = spec as Record<string, unknown>;
  for (const [name, field] of Object.entries(rules.fields)) {
    // A virtual field names a key the serializer derives rather than writes;
    // `schedule` becomes `timetable`.
    if (field.virtual || rules.omitted.has(field.key)) continue;
    const value = values[name];
    if (value === undefined || value === field.default) continue;
    const encoded = encodeField(field, value, `${name} for ${label}`);
    data[field.key] = rules.decorated.has(field.key) ? encoded : unwrapTypeEncoding(encoded);
  }
}

/** Encode one authoring value as the schema's type for that field. Rejects a
 *  value of the wrong type: this is the last point before the scheduler, and a
 *  mistyped field would otherwise surface as an unreadable Dag. */
function encodeField(field: SchemaField, value: unknown, label: string): SerializedValue {
  switch (field.type) {
    case "string":
      if (typeof value !== "string") throw typeError(label, "a string", value);
      return value;
    case "boolean":
      if (typeof value !== "boolean") throw typeError(label, "a boolean", value);
      return value;
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw typeError(label, "a finite number", value);
      }
      return value;
    case "timedelta":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw typeError(label, "a duration in seconds", value);
      }
      return { __type: "timedelta", __var: value };
    case "datetime":
      if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
        throw typeError(label, "a valid Date", value);
      }
      return serializeValue(value);
    case "string[]": {
      if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
        throw typeError(label, "an array of strings", value);
      }
      // Python holds these in a set, so duplicates collapse and the order is
      // the sorted one that keeps a Dag's hash stable across runs.
      return serializeValue(new Set(value as string[]));
    }
  }
}

function typeError(label: string, expected: string, value: unknown): Error {
  return new Error(`${label} must be ${expected}, not ${describeType(value)}`);
}

function describeType(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "an array";
  if (value instanceof Date) return "a Date";
  if (value instanceof Set) return "a Set";
  const noun = typeof value;
  return `${/^[aeiou]/.test(noun) ? "an" : "a"} ${noun}`;
}

/**
 * Encode a value the way `BaseSerialization.serialize()` does.
 *
 * A duration has no distinct runtime type in TypeScript — it is a number of
 * seconds — so `timedelta` is applied by {@link encodeField} from the schema
 * rather than inferred here.
 */
export function serializeValue(value: unknown): SerializedValue {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`Cannot serialize the non-finite number ${String(value)}`);
    }
    return value;
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new Error("Cannot serialize an invalid Date");
    return { __type: "datetime", __var: value.getTime() / 1000 };
  }
  if (value instanceof Set) {
    return {
      __type: "set",
      __var: [...value].map(serializeValue).sort(compareSerialized),
    };
  }
  if (Array.isArray(value)) return value.map(serializeValue);
  if (value instanceof Map) {
    return { __type: "dict", __var: serializeEntries(value.entries()) };
  }
  if (typeof value === "object") {
    return { __type: "dict", __var: serializeEntries(Object.entries(value)) };
  }
  throw new Error(`Cannot serialize a ${typeof value}`);
}

function serializeEntries(entries: Iterable<[unknown, unknown]>): Record<string, SerializedValue> {
  const encoded: Record<string, SerializedValue> = {};
  for (const [key, item] of entries) {
    encoded[String(key)] = serializeValue(item);
  }
  return encoded;
}

// Python sorts a set's members before writing them; JSON's default sort is
// lexicographic on the string form, which matches for the string sets this
// SDK produces and stays total for anything else.
function compareSerialized(left: SerializedValue, right: SerializedValue): number {
  const a = typeof left === "string" ? left : JSON.stringify(left);
  const b = typeof right === "string" ? right : JSON.stringify(right);
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Strip the type encoding from a non-decorated field, as Python's
 * `serialize_to_json` does: it serializes every field, then keeps only the
 * `__var` of the ones outside its decorated set.
 */
export function unwrapTypeEncoding(value: SerializedValue): SerializedValue {
  if (!isTypeEncoded(value)) return value;
  return value.__var;
}

function isTypeEncoded(value: SerializedValue): value is TypeEncoded & SerializedValue {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "__type" in value &&
    "__var" in value
  );
}

/** Where the Dag file sits inside its bundle, as Airflow records it. */
export function computeRelativeFileloc(fileloc: string, bundlePath: string): string {
  if (!fileloc) return "";
  if (!bundlePath) return ".";
  const result = relativePath(bundlePath, fileloc);
  return result === "" ? "." : result;
}
