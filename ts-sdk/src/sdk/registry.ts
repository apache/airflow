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

import { Dag, DUPLICATE_COPY_HINT, getDagTaskRecords, isDag, type TaskRef } from "./dag.js";
import type { TaskHandler } from "./task.js";

// Cross-copy brand, for the reason given on Dag's.
const REGISTRY_BRAND = Symbol.for("airflow.ts-sdk.DagRegistry");

// Assigned inside DagRegistry's static block: gives package-internal code read
// access to the #dags private field without a public accessor on the class.
let dagsOf: (registry: DagRegistry) => ReadonlyMap<string, Dag>;

/** Internal: whether `value` is a DagRegistry built by any copy of this package. */
export function isDagRegistry(value: unknown): value is DagRegistry {
  return typeof value === "object" && value !== null && REGISTRY_BRAND in value;
}

/** Internal: a registered Dag with its task IDs, as {@link listRegistryDags} reports it.
 *  A task-less Dag is included, so the bundle manifest keeps it visible. */
export interface RegisteredDag {
  /** Identifier of the registered Dag. */
  readonly dagId: string;
  /** Airflow task IDs, including any TaskGroup prefix. */
  readonly tasks: string[];
}

/**
 * The Dags a bundle process can execute, keyed by Dag ID.
 *
 * This is what a bundle entry point builds and hands to `serveDags(registry)`:
 *
 * ```ts
 * const dag = new Dag("my_dag");
 * dag.task("extract", extractFn);
 * await serveDags(new DagRegistry(dag));
 * ```
 *
 * It holds no sockets and starts nothing, so a test can build one and invoke a
 * handler through {@link getTaskHandler} without any runtime in scope.
 *
 * Lookups delegate live to each Dag's task map, so tasks added to a Dag
 * after registration are visible — the registry records Dag identity, not
 * a snapshot of its tasks.
 */
export class DagRegistry {
  readonly #dags = new Map<string, Dag>();

  static {
    dagsOf = (registry) => registry.#dags;
  }

  /** Registers `dags`, on the same terms as {@link register}. */
  constructor(...dags: Dag[]) {
    Object.defineProperty(this, REGISTRY_BRAND, { value: true });
    this.register(...dags);
  }

  /** Register Dags. Registering an already-registered `dagId` throws,
   *  and a call that throws registers none of its Dags.
   *
   *  The constructor covers the common case; this is for a bundle that
   *  collects its Dags across several modules. */
  register(...dags: Dag[]): void {
    const incoming = new Set<string>();
    for (const dag of dags) {
      // Typed as Dag, so narrowing it would collapse to never; the guards are
      // for callers that reach this from plain JavaScript.
      const candidate: unknown = dag;
      // Another copy's Dag cannot be registered: lookups read the Dag's private
      // task map, which is keyed to this copy's class object and would throw an
      // opaque TypeError. Reject it here but name the cause, since "only Dag
      // instances" is misleading for something that is plainly a Dag.
      if (!(candidate instanceof Dag)) {
        throw new Error(
          isDag(candidate)
            ? `Dag "${candidate.dagId}" ${DUPLICATE_COPY_HINT}`
            : "only Dag instances can be registered",
        );
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

  /** Look up a registered handler, the way the runtime dispatches a task.
   *  Returns `undefined` when no handler exists. */
  getTaskHandler(dagId: string, taskId: string): TaskHandler | undefined {
    const dag = this.#dags.get(dagId);
    return dag ? getDagTaskRecords(dag).get(taskId)?.handler : undefined;
  }
}

/**
 * Internal: the task handles across a registry's Dags.
 *
 * Not re-exported from the package root: a bundle author declares tasks and
 * serves them, and never has to enumerate what the runtime will dispatch.
 */
export function listRegistryTasks(registry: DagRegistry): TaskRef[] {
  return [...dagsOf(registry).values()].flatMap((dag) =>
    [...getDagTaskRecords(dag).values()].map((record) => record.task),
  );
}

/** Internal: every registered Dag with its task IDs, empty Dags included. */
export function listRegistryDags(registry: DagRegistry): RegisteredDag[] {
  return [...dagsOf(registry).values()].map((dag) => ({
    dagId: dag.dagId,
    tasks: [...dag.taskIds],
  }));
}
