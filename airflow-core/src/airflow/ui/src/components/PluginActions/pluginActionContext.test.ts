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
import { describe, expect, it } from "vitest";

import type {
  DAGDetailsResponse,
  DAGRunResponse,
  DAGWithLatestDagRunsResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

import { pluginActionContext } from "./pluginActionContext";

const dagRun = {
  dag_id: "etl_sales",
  dag_run_id: "manual__2026-01-01",
} as unknown as DAGRunResponse;

const taskInstance = {
  dag_id: "etl_sales",
  dag_run_id: "manual__2026-01-01",
  map_index: -1,
  operator: "PythonOperator",
  task_id: "load",
} as unknown as TaskInstanceResponse;

describe("pluginActionContext", () => {
  it("gives a Dag row identity for scoping and tokens, with no full record", () => {
    const row = {
      dag_id: "etl_sales",
      tags: [{ dag_display_name: "etl_sales", dag_id: "etl_sales", name: "prod" }],
    } as unknown as DAGWithLatestDagRunsResponse;

    const { appliesTo, pluginProps, tokens } = pluginActionContext({ dag: row, destination: "dag" });

    expect(tokens).toEqual({ dagId: "etl_sales" });
    expect(appliesTo.dag).toBe(row);
    expect(pluginProps.dagId).toBe("etl_sales");
    // A list row is not a DAGDetailsResponse, so nothing may claim otherwise.
    expect(pluginProps.dag).toBeUndefined();
  });

  it("passes the full Dag record through when the surface has one", () => {
    const details = { dag_id: "etl_sales", tags: [] } as unknown as DAGDetailsResponse;

    const { pluginProps } = pluginActionContext({
      dag: details,
      dagDetails: details,
      destination: "dag",
    });

    expect(pluginProps.dag).toBe(details);
  });

  it("derives Dag Run tokens and offers the whole run record for scoping", () => {
    const { appliesTo, pluginProps, tokens } = pluginActionContext({
      dagRun,
      destination: "dag_run",
    });

    expect(tokens).toEqual({ dagId: "etl_sales", runId: "manual__2026-01-01" });
    // The whole record, so an `applies_to` path can address any field on it -- `state` being
    // the one this exists for.
    expect(appliesTo.dagRun).toBe(dagRun);
    // A run names its Dag but is not one, so a `dag.*` path stays unevaluable from a row.
    expect(appliesTo.dag).toBeUndefined();
    expect(pluginProps.dagRun).toBe(dagRun);
    expect(pluginProps.runId).toBe("manual__2026-01-01");
  });

  it("derives Task Instance tokens and offers the instance for operator scoping", () => {
    const { appliesTo, pluginProps, tokens } = pluginActionContext({
      destination: "task_instance",
      taskInstance,
    });

    expect(tokens).toEqual({
      dagId: "etl_sales",
      mapIndex: "-1",
      runId: "manual__2026-01-01",
      taskId: "load",
    });
    expect(appliesTo.taskInstance).toBe(taskInstance);
    expect(appliesTo.dag).toBeUndefined();
    expect(pluginProps.taskInstance).toBe(taskInstance);
  });

  it("stringifies an unmapped map_index rather than omitting it", () => {
    const { tokens } = pluginActionContext({ destination: "task_instance", taskInstance });

    // Omitting it would leave `{MAP_INDEX}` sitting in the resolved URL.
    expect(tokens.mapIndex).toBe("-1");
  });

  it("stringifies a real map index", () => {
    const { tokens } = pluginActionContext({
      destination: "task_instance",
      taskInstance: { ...taskInstance, map_index: 7 },
    });

    expect(tokens.mapIndex).toBe("7");
  });

  // Rows for different iterations of one looping task share every other identifier, so
  // without these a link action would give them all the same URL.
  it("passes the loop region through, so iterations of one task differ", () => {
    const { tokens } = pluginActionContext({
      destination: "task_instance",
      taskInstance: { ...taskInstance, region_id: "region-a", region_index: 3 },
    });

    expect(tokens.regionId).toBe("region-a");
    expect(tokens.regionIndex).toBe("3");
  });

  it("omits the region rather than stringifying a missing one", () => {
    const { tokens } = pluginActionContext({ destination: "task_instance", taskInstance });

    expect(tokens.regionId).toBeUndefined();
    expect(tokens.regionIndex).toBeUndefined();
  });
});
