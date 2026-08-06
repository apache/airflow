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

import { Dag, getDagTaskRecords, type Task } from "./dag.js";
import type { TaskHandler } from "./task.js";

/** A registered Dag with its task IDs, empty Dags included — used for
 *  manifests, where a task-less Dag must stay visible. */
export interface RegisteredDag {
  /** Identifier of the registered Dag. */
  readonly dagId: string;
  /** Airflow task IDs, including any TaskGroup prefix. */
  readonly tasks: string[];
}

/**
 * Registry of Dag instances keyed by Dag ID.
 *
 * Lookups delegate live to each Dag's task map, so tasks added to a Dag
 * after registration are visible — the registry records Dag identity, not
 * a snapshot of its tasks.
 */
export class DagRegistry {
  readonly #dags = new Map<string, Dag>();

  /** Register Dags. Registering an already-registered `dagId` throws,
   *  and a call that throws registers none of its Dags. */
  register(...dags: Dag[]): void {
    const incoming = new Set<string>();
    for (const dag of dags) {
      if (!(dag instanceof Dag)) {
        throw new Error("only Dag instances can be registered");
      }
      if (this.#dags.has(dag.dagId) || incoming.has(dag.dagId)) {
        throw new Error(`Dag "${dag.dagId}" is already registered`);
      }
      incoming.add(dag.dagId);
    }
    for (const dag of dags) {
      this.#dags.set(dag.dagId, dag);
    }
  }

  /** Look up a registered handler. Returns `undefined` when no handler exists. */
  getTaskHandler(dagId: string, taskId: string): TaskHandler | undefined {
    const dag = this.#dags.get(dagId);
    return dag ? getDagTaskRecords(dag).get(taskId)?.handler : undefined;
  }

  /** List the task handles across registered Dags. */
  listTasks(): Task[] {
    return [...this.#dags.values()].flatMap((dag) =>
      [...getDagTaskRecords(dag).values()].map((record) => record.task),
    );
  }

  /** List every registered Dag with its task IDs, empty Dags included. */
  listDags(): RegisteredDag[] {
    return [...this.#dags.values()].map((dag) => ({
      dagId: dag.dagId,
      tasks: [...getDagTaskRecords(dag).keys()],
    }));
  }
}

/** The registry `registerDags` writes to and the coordinator reads from. */
export const defaultRegistry = new DagRegistry();

/** Record Dags in the default registry so the coordinator can run their tasks. */
export function registerDags(...dags: Dag[]): void {
  defaultRegistry.register(...dags);
}
