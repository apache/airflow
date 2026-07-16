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

import { afterEach, describe, expect, it, vi } from "vitest";

import { AIRFLOW_METADATA_SENTINEL, buildBundleManifest } from "../../src/coordinator/manifest.js";
import { startCoordinator } from "../../src/coordinator/runtime.js";
import { SUPERVISOR_API_VERSION } from "../../src/coordinator/protocol.js";

describe("buildBundleManifest", () => {
  it("groups registrations by Dag ID under the SDK's schema version", () => {
    expect(
      buildBundleManifest([
        { dagId: "dag_a", taskId: "t1" },
        { dagId: "dag_b", taskId: "t2" },
        { dagId: "dag_a", taskId: "t3" },
      ]),
    ).toEqual({
      supervisor_schema_version: SUPERVISOR_API_VERSION,
      dags: {
        dag_a: { tasks: ["t1", "t3"] },
        dag_b: { tasks: ["t2"] },
      },
    });
  });
});

describe("startCoordinator --airflow-metadata", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("dumps the manifest to stdout and returns without connecting", async () => {
    const write = vi.spyOn(process.stdout, "write").mockReturnValue(true);

    await startCoordinator({ argv: ["node", "bundle.mjs", "--airflow-metadata"] });

    expect(write).toHaveBeenCalledTimes(1);
    const written = String(write.mock.calls[0]![0]);
    expect(written.startsWith(AIRFLOW_METADATA_SENTINEL)).toBe(true);
    const payload = JSON.parse(written.slice(AIRFLOW_METADATA_SENTINEL.length));
    expect(payload.supervisor_schema_version).toBe(SUPERVISOR_API_VERSION);
    expect(payload.dags).toEqual({});
  });
});
