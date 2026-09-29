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

// Compares this SDK's serializer against Python's, field by field, over the
// Dags both build from tests/conformance/test_dags.json.
//
// The Python side is recorded by tests/conformance/serialize_python.py; see its
// docstring for how to regenerate it and how to push this SDK's output back
// through Airflow's own deserializer. The cross-SDK harness that
// airflow-core/adr/lang-sdk/0004-dag-parsing.md describes is not in tree yet, so
// this is a local stand-in shaped like it.
//
// Every difference between the two outputs is listed below with the reason it
// is allowed. Anything not listed has to match exactly, so a serializer change
// that drifts from Python fails here.

import { describe, expect, it } from "vitest";

import { serializeDag } from "../../src/coordinator/serde.js";
import { SERIALIZATION_VERSION } from "../../src/generated/dag-schema-fields.js";
import { buildFixtureDag, FIXTURE_DAGS, TASK_FIELDS_BY_KEY } from "../conformance/fixtures.js";

import recorded from "../conformance/serialized_python.json" with { type: "json" };

/** Dag keys Python writes that describe the Python Dag file rather than the Dag. */
const DAG_KEYS_ONLY_PYTHON_HAS: ReadonlySet<string> = new Set([
  // All three name the Python source file. serialize_python.py strips them;
  // this SDK writes the bundle's own path, which has nothing to compare against.
  "fileloc",
  "relative_fileloc",
  "_processor_dags_folder",
]);

/** Task keys that legitimately differ, with why. */
const TASK_KEYS_NOT_COMPARED: ReadonlySet<string> = new Set([
  // Operator identity: Python names the Python class that ran, this SDK names a
  // fixed synthetic pair. Neither is ever imported on the Airflow side.
  "task_type",
  "_task_module",
  // This SDK's marker for a TypeScript task; Python has no equivalent.
  "language",
  // Python bookkeeping for mapped tasks and retry policies, neither of which is
  // authorable in TypeScript.
  "_needs_expansion",
  "has_retry_policy",
  // The lang-SDK stub contract: a task whose arguments the API server resolves
  // per instance. Every native task carries it, and the Python operator these
  // fixtures build has no counterpart — `serde.test.ts` covers the shape, and
  // `serialize_python.py --verify` pushes it through Airflow's deserializer.
  "is_stub",
  "_arg_bindings",
]);

type Json = Record<string, unknown>;

function taskVars(dag: Json): Map<string, Json> {
  const tasks = dag["tasks"] as { __type: string; __var: Json }[];
  return new Map(tasks.map((task) => [String(task.__var["task_id"]), task.__var]));
}

const pythonDags = (
  recorded as unknown as { dags: Record<string, { __version: number; dag: Json }> }
).dags;

describe("conformance with Python's DagSerialization", () => {
  it("covers exactly the Dags Python recorded", () => {
    expect(FIXTURE_DAGS.map((fixture) => fixture.dag_id).sort()).toEqual(
      Object.keys(pythonDags).sort(),
    );
  });

  describe.each(FIXTURE_DAGS.map((fixture) => [fixture.dag_id, fixture] as const))(
    "%s",
    (dagId, fixture) => {
      const python = pythonDags[dagId]!;
      // fileloc and relative_fileloc are compared against nothing, so any path does.
      const ours = serializeDag(
        buildFixtureDag(fixture),
        "/bundles/app/bundle.mjs",
        "bundle.mjs",
      ) as Json;

      it("is stamped with the version Python stamps", () => {
        expect(SERIALIZATION_VERSION).toBe(python.__version);
      });

      it("emits the same Dag-level keys", () => {
        const comparable = (dag: Json) =>
          Object.keys(dag)
            .filter((key) => !DAG_KEYS_ONLY_PYTHON_HAS.has(key))
            .sort();
        expect(comparable(ours)).toEqual(comparable(python.dag));
      });

      it("emits the same Dag-level values", () => {
        for (const [key, value] of Object.entries(python.dag)) {
          if (DAG_KEYS_ONLY_PYTHON_HAS.has(key) || key === "tasks") continue;
          expect({ [key]: ours[key] }).toEqual({ [key]: value });
        }
      });

      it("emits the same tasks, in the same order", () => {
        const pythonTasks = python.dag["tasks"] as { __var: Json }[];
        const ourTasks = ours["tasks"] as { __type: string; __var: Json }[];
        expect(ourTasks.map((task) => task.__var["task_id"])).toEqual(
          pythonTasks.map((task) => task.__var["task_id"]),
        );
        expect(ourTasks.map((task) => task.__type)).toEqual(ourTasks.map(() => "operator"));
      });

      it("emits the same task fields", () => {
        const pythonTasks = taskVars(python.dag);
        const ourTasks = taskVars(ours);
        for (const [taskId, pythonTask] of pythonTasks) {
          const ourTask = ourTasks.get(taskId)!;
          for (const [key, value] of Object.entries(pythonTask)) {
            if (TASK_KEYS_NOT_COMPARED.has(key)) continue;
            // Python keeps a field whose value equals the schema default when
            // its `client_defaults` table disagrees with that default. This SDK
            // is never sent that table and always omits such a field; dropping
            // a value the scheduler re-derives is semantically identical.
            if (!(key in ourTask) && TASK_FIELDS_BY_KEY.get(key)?.[1].default === value) continue;
            expect({ [`${taskId}.${key}`]: ourTask[key] }).toEqual({ [`${taskId}.${key}`]: value });
          }
          for (const key of Object.keys(ourTask)) {
            if (TASK_KEYS_NOT_COMPARED.has(key)) continue;
            expect({ task: taskId, key, inPython: key in pythonTask }).toEqual({
              task: taskId,
              key,
              inPython: true,
            });
          }
        }
      });
    },
  );
});
