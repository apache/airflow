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
import { listRegisteredTasks, type TaskRegistration } from "../sdk/registry.js";

export const AIRFLOW_METADATA_FLAG = "--airflow-metadata";

/** Marks the manifest line on stdout, which import-time logging may also reach. */
export const AIRFLOW_METADATA_SENTINEL = "__AIRFLOW_METADATA__ ";

/** Bundle manifest fields only the built bundle itself knows: the schema
 *  version it was compiled against and the Dag/task pairs it registered.
 *  `airflow-ts-pack` runs `node bundle.mjs --airflow-metadata` to read this. */
export interface BundleManifest {
  supervisor_schema_version: string;
  dags: Record<string, { tasks: string[] }>;
}

export function buildBundleManifest(
  registrations: readonly TaskRegistration[] = listRegisteredTasks(),
): BundleManifest {
  const dags: BundleManifest["dags"] = {};
  for (const { dagId, taskId } of registrations) {
    (dags[dagId] ??= { tasks: [] }).tasks.push(taskId);
  }
  return { supervisor_schema_version: SUPERVISOR_API_VERSION, dags };
}
