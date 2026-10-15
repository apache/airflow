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

// Serializes the Dags of scripts/ci/lang_sdk_serialization/test_dags.yaml with this SDK, as
// serialize_python.py there does with Airflow's serializer, and writes them keyed by Dag id. The
// check-ts-sdk-serialization-conformance hook has that directory's compare.py run it as:
//
//   pnpm --dir ts-sdk exec tsx tests/conformance/serialize_typescript.ts <test_dags.yaml> <output.json>

import { readFileSync, writeFileSync } from "node:fs";
import { argv } from "node:process";

import { parse, type ScalarTag } from "yaml";

import { serializeDag } from "../../src/coordinator/serde.js";
import {
  DAG_SCHEMA_FIELDS,
  SERIALIZATION_VERSION,
  TASK_SCHEMA_FIELDS,
  type SchemaField,
} from "../../src/generated/dag-schema-fields.js";
import {
  Dag,
  type DagSpec,
  type TaskGroupRef,
  type TaskRef,
  type TaskSpec,
} from "../../src/index.js";

interface TaskCase {
  readonly task_id: string;
  readonly group?: string;
  readonly upstream?: readonly string[];
  readonly spec?: Readonly<Record<string, unknown>>;
}

interface DagCase {
  readonly dag_id: string;
  readonly spec?: Readonly<Record<string, unknown>>;
  readonly groups?: readonly string[];
  readonly tasks: readonly TaskCase[];
  readonly order_edges?: readonly (readonly [string, string])[];
}

// A duration is already a number of seconds in this SDK's authoring API.
const TAGS: ScalarTag[] = [
  { tag: "!datetime", resolve: (value) => new Date(value) },
  { tag: "!timedelta", resolve: (value) => Number(value) },
];

/**
 * Map each schema key of a generated table to the authoring name it is set by.
 *
 * A virtual field is keyed by its authoring name instead, since its schema key names what the
 * serializer derives: `schedule` is spelled that way in both Python's `DAG()` and this SDK, but
 * its schema key is `timetable`.
 */
function authoringNames(table: Readonly<Record<string, SchemaField>>): Map<string, string> {
  return new Map(
    Object.entries(table).map(([name, field]) => [field.virtual ? name : field.key, name]),
  );
}

const DAG_NAMES = authoringNames(DAG_SCHEMA_FIELDS);
const TASK_NAMES = authoringNames(TASK_SCHEMA_FIELDS);

function toAuthoringSpec(
  spec: Readonly<Record<string, unknown>>,
  names: Map<string, string>,
  label: string,
): Record<string, unknown> {
  const authoring: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(spec)) {
    const name = names.get(key);
    if (name === undefined) throw new Error(`${label}: "${key}" is not an authoring field`);
    authoring[name] = value;
  }
  return authoring;
}

function qualifiedTaskId(task: TaskCase): string {
  return task.group === undefined ? task.task_id : `${task.group}.${task.task_id}`;
}

function buildDag(dagCase: DagCase): Dag {
  const dag = new Dag(
    dagCase.dag_id,
    toAuthoringSpec(dagCase.spec ?? {}, DAG_NAMES, dagCase.dag_id) as DagSpec,
  );
  // A group id is fully qualified, so its parent is whatever comes before the last dot.
  const groups = new Map<string, TaskGroupRef>();
  for (const groupId of dagCase.groups ?? []) {
    const cut = groupId.lastIndexOf(".");
    const scope = cut === -1 ? dag : groups.get(groupId.slice(0, cut))!;
    groups.set(groupId, scope.taskGroup(groupId.slice(cut + 1)));
  }

  const factories = new Map<string, (inputs: Record<string, TaskRef>) => TaskRef>();
  for (const task of dagCase.tasks) {
    const label = `${dagCase.dag_id}.${qualifiedTaskId(task)}`;
    const spec = toAuthoringSpec(task.spec ?? {}, TASK_NAMES, label) as TaskSpec;
    const scope = task.group === undefined ? dag : groups.get(task.group)!;
    factories.set(
      qualifiedTaskId(task),
      scope.task(task.task_id, async (_inputs: Record<string, TaskRef>) => undefined, spec),
    );
  }
  const refs = new Map<string, TaskRef>();
  for (const task of dagCase.tasks) {
    const inputs: Record<string, TaskRef> = {};
    for (const upstream of task.upstream ?? []) {
      const ref = refs.get(upstream);
      if (!ref)
        throw new Error(`${dagCase.dag_id}: "${upstream}" has to come before its downstream`);
      inputs[`from_${upstream}`] = ref;
    }
    refs.set(qualifiedTaskId(task), factories.get(qualifiedTaskId(task))!(inputs));
  }
  for (const [upstream, downstream] of dagCase.order_edges ?? []) {
    const from = groups.get(upstream) ?? refs.get(upstream);
    const to = groups.get(downstream) ?? refs.get(downstream);
    if (!from || !to)
      throw new Error(`${dagCase.dag_id}: no node "${from ? downstream : upstream}"`);
    from.before(to);
  }
  return dag;
}

const [testDags, output] = argv.slice(2);
if (testDags === undefined || output === undefined) {
  throw new Error("Usage: serialize_typescript.ts <test_dags.yaml> <output.json>");
}
const { dags } = parse(readFileSync(testDags, "utf-8"), { customTags: TAGS }) as {
  dags: DagCase[];
};
// compare.py leaves fileloc out, as it names the file a Dag was declared in, so any bundle path
// does. Airflow still needs one to load the Dag.
const serialized = Object.fromEntries(
  dags.map((dagCase) => [
    dagCase.dag_id,
    {
      __version: SERIALIZATION_VERSION,
      dag: serializeDag(buildDag(dagCase), "/bundles/app/bundle.mjs", "bundle.mjs"),
    },
  ]),
);
writeFileSync(output, `${JSON.stringify(serialized, null, 2)}\n`);
