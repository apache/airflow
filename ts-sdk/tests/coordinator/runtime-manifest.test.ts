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

  it("rejects an empty dagId", () => {
    const registry = new DagRegistry(buildDag(""));
    expect(() => buildBundleManifest(registry)).toThrowError(/must be made of alphanumeric/);
  });

  it("rejects an empty taskId", () => {
    const registry = new DagRegistry(buildDag("example_dag", ""));
    expect(() => buildBundleManifest(registry)).toThrowError(/must be made of alphanumeric/);
  });

  it.each(["   ", "\t", "my dag", "a/b", "task@1"])(
    "rejects a dagId with characters no Python dag_id allows: %j",
    (dagId) => {
      const registry = new DagRegistry(buildDag(dagId));
      expect(() => buildBundleManifest(registry)).toThrowError(/must be made of alphanumeric/);
    },
  );

  it.each(["   ", "\t", "my task", "a/b", "task@1"])(
    "rejects a taskId with characters no Python task_id allows: %j",
    (taskId) => {
      const registry = new DagRegistry(buildDag("example_dag", taskId));
      expect(() => buildBundleManifest(registry)).toThrowError(/must be made of alphanumeric/);
    },
  );

  it("rejects a dagId longer than 250 characters", () => {
    const registry = new DagRegistry(buildDag("d".repeat(251)));
    expect(() => buildBundleManifest(registry)).toThrowError(
      /must be less than 250 characters, not 251/,
    );
  });

  it("rejects a taskId longer than 250 characters", () => {
    const registry = new DagRegistry(buildDag("example_dag", "t".repeat(251)));
    expect(() => buildBundleManifest(registry)).toThrowError(
      /must be less than 250 characters, not 251/,
    );
  });

  it("does not validate key format when Dags are only registered, not packed", () => {
    expect(() => new DagRegistry(buildDag("bad dag id", "bad task id"))).not.toThrow();
  });
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
