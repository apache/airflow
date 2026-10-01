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

import { SUPERVISOR_API_VERSION } from "./protocol.js";
import { bundleDags, bundleDagTaskIds, finalizeBundleDags, type Bundle } from "../sdk/bundle.js";
import { getDagDefinedIn } from "../sdk/dag.js";

export const AIRFLOW_METADATA_FLAG = "--airflow-metadata";

/** Marks the manifest line on stdout, which import-time logging may also reach. */
export const AIRFLOW_METADATA_SENTINEL = "__AIRFLOW_METADATA__ ";

/** Bundle manifest fields only the built bundle itself knows: the schema version it was compiled
 *  against, and the task handlers it registered grouped by Dag. Named `task_handlers` because a
 *  TypeScript bundle provides handlers for Dags declared elsewhere, not Dag definitions. A Dag with
 *  no handlers keeps an empty `tasks` list so `airflow-ts-pack` can warn instead of dropping it.
 *
 *  `dag_source_paths` names the source file each *native* Dag was declared in — captured at
 *  construction time from `airflow-ts-pack`'s module-source tag. Mixed-lang Dags (owned by
 *  Python) are absent here. */
export interface BundleManifest {
  supervisor_schema_version: string;
  task_handlers: Record<string, { tasks: string[] }>;
  dag_source_paths: Record<string, string>;
}

export function buildBundleManifest(bundle: Bundle): BundleManifest {
  const taskHandlers: BundleManifest["task_handlers"] = {};
  const dagSourcePaths: BundleManifest["dag_source_paths"] = {};
  // The manifest is the bundle reporting what it provides, so this is where its
  // Dags are finalized: a Dag missing an edge is reported here rather than packed.
  finalizeBundleDags(bundle);
  for (const [dagId, tasks] of bundleDagTaskIds(bundle)) {
    if (typeof dagId !== "string") {
      throw new Error("Dag ID must be a string");
    }
    Object.defineProperty(taskHandlers, dagId, {
      configurable: true,
      enumerable: true,
      value: { tasks: [...tasks] },
      writable: true,
    });
  }
  // Second pass: only native Dags carry a source path. Mixed-lang Dags
  // (owned by Python) are absent.
  for (const [dagId, dag] of bundleDags(bundle)) {
    const source = getDagDefinedIn(dag);
    if (source === undefined) continue;
    Object.defineProperty(dagSourcePaths, dagId, {
      configurable: true,
      enumerable: true,
      value: source,
      writable: true,
    });
  }
  return {
    supervisor_schema_version: SUPERVISOR_API_VERSION,
    task_handlers: taskHandlers,
    dag_source_paths: dagSourcePaths,
  };
}
