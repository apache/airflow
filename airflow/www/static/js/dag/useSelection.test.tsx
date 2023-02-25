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

/* global describe, test, expect */

import React, { PropsWithChildren } from "react";
import { act, renderHook } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";

import useSelection from "./useSelection";

const Wrapper = ({ children }: PropsWithChildren) => (
  <MemoryRouter>{children}</MemoryRouter>
);

describe("Test useSelection hook", () => {
  test("Initial values", async () => {
    const { result } = renderHook(() => useSelection(), { wrapper: Wrapper });
    const {
      selected: { runId, taskId, mapIndex },
    } = result.current;

    expect(runId).toBeNull();
    expect(taskId).toBeNull();
    expect(mapIndex).toBeNull();
  });

  test.each([
    { taskId: "task_1", runId: "run_1", mapIndex: 2 },
    { taskId: null, runId: "run_1", mapIndex: null },
    { taskId: "task_2", runId: null, mapIndex: 1 },
    { taskId: "task_3", runId: null, mapIndex: -1 },
    { taskId: "task_4", runId: null, mapIndex: 0 },
  ])("Test onSelect() and clearSelection()", async (selected) => {
    const { result } = renderHook(() => useSelection(), { wrapper: Wrapper });

    await act(async () => {
      result.current.onSelect(selected);
    });

    expect(result.current.selected.taskId).toBe(selected.taskId);
    expect(result.current.selected.runId).toBe(selected.runId);
    expect(result.current.selected.mapIndex).toBe(selected.mapIndex);

    // clearSelection
    await act(async () => {
      result.current.clearSelection();
    });

    expect(result.current.selected.taskId).toBeNull();
    expect(result.current.selected.runId).toBeNull();
    expect(result.current.selected.mapIndex).toBeNull();
  });
});
