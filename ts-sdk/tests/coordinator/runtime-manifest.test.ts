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
import { Dag } from "../../src/sdk/dag.js";
import { DagRegistry } from "../../src/sdk/registry.js";

function buildDag(dagId: string, ...taskIds: string[]): Dag {
  const dag = new Dag(dagId);
  for (const taskId of taskIds) {
    dag.task(taskId, async () => undefined);
  }
  return dag;
}

describe("buildBundleManifest", () => {
  it("maps a registry's Dags to their tasks under the SDK's schema version", () => {
    const registry = new DagRegistry(buildDag("dag_a", "t1", "t3"), buildDag("dag_b", "t2"));
    expect(buildBundleManifest(registry)).toEqual({
      supervisor_schema_version: SUPERVISOR_API_VERSION,
      dags: {
        dag_a: { tasks: ["t1", "t3"] },
        dag_b: { tasks: ["t2"] },
      },
    });
  });

  it("keeps a registered Dag without tasks visible in the manifest", () => {
    expect(buildBundleManifest(new DagRegistry(buildDag("empty_dag"))).dags).toEqual({
      empty_dag: { tasks: [] },
    });
  });

  it("keeps a Dag named __proto__ visible in serialized metadata", () => {
    const manifest = buildBundleManifest(new DagRegistry(buildDag("__proto__", "task")));
    const serializedDags = JSON.parse(JSON.stringify(manifest)).dags;

    expect(Object.keys(serializedDags)).toEqual(["__proto__"]);
    expect(serializedDags["__proto__"]).toEqual({ tasks: ["task"] });
  });

  it("reports only the Dags the registry was given", () => {
    const registry = new DagRegistry(buildDag("dag_a", "t1"));
    buildDag("dag_b", "t2");
    expect(Object.keys(buildBundleManifest(registry).dags)).toEqual(["dag_a"]);
  });

  // The server would reject these ids. The manifest keeps them and
  // airflow-ts-pack warns at build time instead of failing the pack.
  it.each(["", "   ", "\t", "my dag", "a/b", "task@1", "d".repeat(251)])(
    "keeps a dagId the server would reject visible in the manifest: %j",
    (dagId) => {
      const manifest = buildBundleManifest(new DagRegistry(buildDag(dagId, "t1")));
      expect(manifest.dags[dagId]).toEqual({ tasks: ["t1"] });
    },
  );

  it.each(["", "   ", "\t", "my task", "a/b", "task@1", "t".repeat(251)])(
    "keeps a taskId the server would reject visible in the manifest: %j",
    (taskId) => {
      const manifest = buildBundleManifest(new DagRegistry(buildDag("example_dag", taskId)));
      expect(manifest.dags["example_dag"]).toEqual({ tasks: [taskId] });
    },
  );
});

describe("startCoordinator --airflow-metadata", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("dumps the manifest to stdout and returns without connecting", async () => {
    const write = vi.spyOn(process.stdout, "write").mockReturnValue(true);

    await startCoordinator(new DagRegistry(buildDag("metadata_dag", "only")), {
      argv: ["node", "bundle.mjs", "--airflow-metadata"],
    });

    expect(write).toHaveBeenCalledTimes(1);
    const written = String(write.mock.calls[0]![0]);
    expect(written.startsWith(AIRFLOW_METADATA_SENTINEL)).toBe(true);
    const payload = JSON.parse(written.slice(AIRFLOW_METADATA_SENTINEL.length));
    expect(payload.supervisor_schema_version).toBe(SUPERVISOR_API_VERSION);
    expect(payload.dags).toEqual({ metadata_dag: { tasks: ["only"] } });
  });
});
