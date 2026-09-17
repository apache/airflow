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

// The bundle: what a TypeScript bundle process provides, and how it serves it.

import { brand, DUPLICATE_COPY_HINT, hasBrand } from "./brand.js";
import { Dag, getDagTaskRecords, isDag, type TaskRef } from "./dag.js";
import type { TaskFunction } from "./task.js";

// Assigned inside Bundle's static block, as Dag does for its tasks.
let dagsOf: (bundle: Bundle) => ReadonlyMap<string, Dag>;

/**
 * Anything {@link Bundle.register} accepts.
 *
 * A union rather than a base class or an interface: TypeScript's equivalent of
 * the sealed interface the Go SDK uses for the same purpose. Registering gains
 * a kind by gaining an arm here, never a second verb.
 */
export type Registerable = Dag;

/** Internal: whether `value` is a Bundle built by any copy of this package. */
export function isBundle(value: unknown): value is Bundle {
  return hasBrand(value, "Bundle");
}

/** Internal: a registered Dag with its task IDs, as {@link listBundleDags} reports it.
 *  A task-less Dag is included, so the bundle manifest keeps it visible. */
export interface RegisteredDag {
  /** Identifier of the registered Dag. */
  readonly dagId: string;
  /** Airflow task IDs, including any TaskGroup prefix. */
  readonly tasks: string[];
}

/**
 * What a bundle process provides to Airflow, and the thing that serves it.
 *
 * A bundle entry point builds one, registers what it provides, and awaits
 * `serve()` at module top level:
 *
 * ```ts
 * const dag = new Dag("my_dag");
 * dag.task("extract", extractFn);
 *
 * const bundle = new Bundle();
 * bundle.register(dag);
 * await bundle.serve();
 * ```
 *
 * Registering holds no sockets and starts nothing, so a test can build a bundle
 * and invoke a handler through {@link getTaskHandler} without any runtime in
 * scope. Only `serve()` connects to Airflow.
 *
 * Lookups delegate live to each Dag's task map, so tasks added to a Dag
 * after registration are visible: the bundle records Dag identity, not
 * a snapshot of its tasks.
 */
export class Bundle {
  readonly #dags = new Map<string, Dag>();

  static {
    dagsOf = (bundle) => bundle.#dags;
  }

  /** Registers `items`, on the same terms as {@link register}. */
  constructor(...items: Registerable[]) {
    brand(this, "Bundle");
    this.register(...items);
  }

  /** Register what this bundle provides. Registering an already-registered
   *  `dagId` throws, and a call that throws registers none of its items.
   *
   *  The constructor covers the common case; this is for a bundle that
   *  collects what it provides across several modules. */
  register(...items: Registerable[]): void {
    const incoming = new Set<string>();
    for (const item of items) {
      // Typed as Registerable, so narrowing it would collapse to never; these
      // guard callers reaching this from plain JavaScript.
      const candidate: unknown = item;
      // Another copy's Dag cannot be registered, since lookups read a private
      // task map keyed to this copy's class, so it is rejected by its cause.
      if (!(candidate instanceof Dag)) {
        throw new Error(
          isDag(candidate)
            ? `Dag "${candidate.dagId}" ${DUPLICATE_COPY_HINT}`
            : "only Dag instances can be registered",
        );
      }
      if (this.#dags.has(item.dagId) || incoming.has(item.dagId)) {
        throw new Error(`Dag "${item.dagId}" is already registered`);
      }
      incoming.add(item.dagId);
    }
    for (const item of items) {
      this.#dags.set(item.dagId, item);
    }
  }

  /**
   * Serve this bundle to Airflow. The entry point of a TypeScript Dag bundle.
   *
   * A bundle process serves one supervisor request, so a second call, which
   * would connect a second pair of sockets, is rejected. A call that fails is
   * not a serve, and may be retried. Resolves once the supervisor has been sent
   * the terminal frame for the work this process was started for, and the same
   * call answers the build-time `--airflow-metadata` query `airflow-ts-pack`
   * makes.
   *
   * Everything this bundle provides must be registered before `serve()` is
   * awaited: what is left out is not part of the bundle, and its tasks are
   * marked removed at runtime.
   */
  async serve(): Promise<void> {
    // `const { serve } = bundle` detaches the method, which would otherwise
    // fail deep in the runtime on a missing private field rather than here.
    validateOwnBundle(this, "bundle.serve()");
    // Imported here rather than at module top: the coordinator reads a bundle,
    // so a static import would make the authoring surface and the coordinator
    // mutually dependent for the sake of one call.
    const { serveBundle } = await import("../coordinator/serve.js");
    await serveBundle(this);
  }

  /** Look up a registered handler, the way the runtime dispatches a task.
   *  Returns `undefined` when no handler exists. */
  getTaskHandler(dagId: string, taskId: string): TaskFunction | undefined {
    const dag = this.#dags.get(dagId);
    return dag ? getDagTaskRecords(dag).get(taskId)?.fn : undefined;
  }
}

/** Internal: reject a `this` that is not a Bundle built by this copy, naming
 *  the cause. */
export function validateOwnBundle(value: unknown, accessor: string): asserts value is Bundle {
  if (value instanceof Bundle) return;
  throw new Error(
    isBundle(value)
      ? `The bundle ${accessor} was called on ${DUPLICATE_COPY_HINT}`
      : `${accessor} must be called on a Bundle; build one with new Bundle(...)`,
  );
}

/** Internal: the task handles across a bundle's Dags. Not re-exported from the
 *  package root: enumerating what the runtime dispatches is the runtime's job. */
export function listBundleTasks(bundle: Bundle): TaskRef[] {
  return [...dagsOf(bundle).values()].flatMap((dag) =>
    [...getDagTaskRecords(dag).values()].map((record) => record.task),
  );
}

/** Internal: every registered Dag with its task IDs, empty Dags included. */
export function listBundleDags(bundle: Bundle): RegisteredDag[] {
  return [...dagsOf(bundle).values()].map((dag) => ({
    dagId: dag.dagId,
    tasks: [...dag.taskIds],
  }));
}
