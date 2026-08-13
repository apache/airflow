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
import { listRegistryDags, type DagRegistry } from "../sdk/registry.js";

export const AIRFLOW_METADATA_FLAG = "--airflow-metadata";

/** Marks the manifest line on stdout, which import-time logging may also reach. */
export const AIRFLOW_METADATA_SENTINEL = "__AIRFLOW_METADATA__ ";

// Mirrors the Python task-SDK KEY_REGEX and validate_key in airflow.sdk.definitions._internal.node.
// Checked here rather than in Dag()/task() registration: that runs on every
// bundle module load, including at task-execution-runtime startup, whereas
// this only runs once, when the bundle is packed.
const KEY_REGEX = /^[\p{L}\p{N}_.-]+$/u;
const MAX_KEY_LENGTH = 250;

function validateKey(label: string, value: string): void {
  if (typeof value !== "string" || !KEY_REGEX.test(value)) {
    throw new Error(
      `${label} must be made of alphanumeric characters, dashes, dots, and underscores`,
    );
  }
  if (value.length > MAX_KEY_LENGTH) {
    throw new Error(`${label} must be less than ${MAX_KEY_LENGTH} characters, not ${value.length}`);
  }
}

/** Bundle manifest fields only the built bundle itself knows: the schema
 *  version it was compiled against and the Dag/task pairs it registered.
 *  Registered Dags without tasks appear with an empty `tasks` list so
 *  `airflow-ts-pack` (which runs `node bundle.mjs --airflow-metadata` to
 *  read this) can warn about them instead of silently dropping them. */
export interface BundleManifest {
  supervisor_schema_version: string;
  dags: Record<string, { tasks: string[] }>;
}

export function buildBundleManifest(registry: DagRegistry): BundleManifest {
  const dags: BundleManifest["dags"] = {};
  for (const { dagId, tasks } of listRegistryDags(registry)) {
    validateKey(`Dag "${dagId}"`, dagId);
    for (const taskId of tasks) {
      validateKey(`Task "${taskId}" of Dag "${dagId}"`, taskId);
    }
    Object.defineProperty(dags, dagId, {
      configurable: true,
      enumerable: true,
      value: { tasks: [...tasks] },
      writable: true,
    });
  }
  return {
    supervisor_schema_version: SUPERVISOR_API_VERSION,
    dags,
  };
}
