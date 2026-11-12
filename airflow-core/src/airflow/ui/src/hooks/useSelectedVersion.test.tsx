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
import type { PropsWithChildren } from "react";

import { renderHook, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";

import {
  DagRunService,
  DagService,
  StructureService,
  TaskInstanceService,
  type DAGRunResponse,
  type TaskInstanceResponse,
} from "openapi/requests";

import { BaseWrapper } from "src/utils/Wrapper";

import useSelectedVersion from "./useSelectedVersion";

const REGION_ID = "11111111-1111-4111-8111-111111111111";

const wrapper = ({ children }: PropsWithChildren) => (
  <BaseWrapper>
    <MemoryRouter initialEntries={[`/dags/dag/runs/run/tasks/task?region_id=${REGION_ID}&region_index=2`]}>
      <Routes>
        <Route element={children} path="/dags/:dagId/runs/:runId/tasks/:taskId" />
      </Routes>
    </MemoryRouter>
  </BaseWrapper>
);

afterEach(() => vi.restoreAllMocks());

it("uses the version pinned by the execution addressed by the region coordinates", async () => {
  vi.spyOn(DagService, "getDagDetails").mockResolvedValue({
    latest_dag_version: { version_number: 9 },
  } as Awaited<ReturnType<typeof DagService.getDagDetails>>);
  vi.spyOn(DagRunService, "getDagRun").mockResolvedValue({
    dag_versions: [{ version_number: 5 }],
  } as unknown as DAGRunResponse);
  vi.spyOn(StructureService, "structureData").mockResolvedValue({
    edges: [],
    nodes: [{ id: "task", is_mapped: false, label: "task", type: "task" }],
  } as unknown as Awaited<ReturnType<typeof StructureService.structureData>>);
  const getMappedTaskInstance = vi
    .spyOn(TaskInstanceService, "getMappedTaskInstance")
    .mockResolvedValue({ dag_version: { version_number: 3 } } as TaskInstanceResponse);

  const { result } = renderHook(() => useSelectedVersion(), { wrapper });

  await waitFor(() => expect(result.current).toBe(3));
  expect(getMappedTaskInstance).toHaveBeenCalledWith(
    expect.objectContaining({ regionId: REGION_ID, regionIndex: 2 }),
  );
});
