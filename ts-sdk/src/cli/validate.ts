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

import type { BundleManifest } from "../coordinator/manifest.js";

const MAX_ID_LENGTH = 250;
const ID_REGEX = /^[\p{L}\p{N}_.-]+$/u;

type WarnFn = (message: string) => void;

/**
 * Check every dag and task id in the manifest against the rules the Airflow
 * server enforces (`airflow.utils.helpers.validate_key`). Best-effort and
 * warn-only: the server validates authoritatively, and checks like the `..`
 * one depend on server configuration the packer cannot see.
 */
export function warnOnSuspiciousIds(
  dags: BundleManifest["dags"],
  warn: WarnFn = (message) => process.stderr.write(`${message}\n`),
): void {
  for (const dagId of Object.keys(dags).sort()) {
    warnOnSuspiciousId(`dag id ${JSON.stringify(dagId)}`, dagId, warn);
    for (const taskId of dags[dagId]!.tasks) {
      warnOnSuspiciousId(
        `task id ${JSON.stringify(taskId)} in dag ${JSON.stringify(dagId)}`,
        taskId,
        warn,
      );
    }
  }
}

function warnOnSuspiciousId(label: string, id: string, warn: WarnFn): void {
  // Count code points, not UTF-16 units, to match the server-side len().
  const length = [...id].length;
  if (length > MAX_ID_LENGTH) {
    warn(
      `warning: ${label} is longer than ${MAX_ID_LENGTH} characters (${length}); the Airflow server will reject it`,
    );
  }
  if (!ID_REGEX.test(id)) {
    warn(
      `warning: ${label} must be made of alphanumeric characters, dashes, dots, and underscores; the Airflow server will reject it`,
    );
  } else if (id.includes("..")) {
    warn(
      `warning: ${label} contains '..'; the Airflow server will reject it unless [core] allow_double_dot_in_ids is enabled`,
    );
  }
}
