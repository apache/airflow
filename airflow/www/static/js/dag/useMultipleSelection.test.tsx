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
import type { GridData } from "../api/useGridData";

import useMultipleSelection from "./useMultipleSelection";

const Wrapper = ({ children }: PropsWithChildren) => (
  <MemoryRouter>{children}</MemoryRouter>
);
const mockGridData: GridData = {
  groups: {
    children: [
      {
        id: "task_a",
        instances: [
          {
            endDate: "2024-06-08T22:55:20.749988+00:00",
            note: null,
            queuedDttm: "2024-06-08T22:55:20.016931+00:00",
            runId: "run2",
            startDate: "2024-06-08T22:55:20.476427+00:00",
            state: "success",
            taskId: "task_a",
            tryNumber: 1,
          },
          {
            endDate: "2024-06-08T22:55:20.749826+00:00",
            note: null,
            queuedDttm: "2024-06-08T22:55:20.016931+00:00",
            runId: "run1",
            startDate: "2024-06-08T22:55:20.481613+00:00",
            state: "success",
            taskId: "task_a",
            tryNumber: 1,
          },
        ],
        isMapped: false,
        label: "task_a",
        operator: "PythonOperator",
      },
      {
        id: "task_b",
        instances: [
          {
            endDate: "2024-06-08T22:55:21.259311+00:00",
            note: null,
            queuedDttm: "2024-06-08T22:55:20.785072+00:00",
            runId: "run2",
            startDate: "2024-06-08T22:55:21.025994+00:00",
            state: "success",
            taskId: "task_b",
            tryNumber: 1,
          },
          {
            endDate: "2024-06-08T22:55:21.325288+00:00",
            note: null,
            queuedDttm: "2024-06-08T22:55:20.868299+00:00",
            runId: "run1",
            startDate: "2024-06-08T22:55:21.091137+00:00",
            state: "success",
            taskId: "task_b",
            tryNumber: 1,
          },
        ],
        isMapped: false,
        label: "task_b",
        operator: "PythonOperator",
      },
      {
        id: "task_c",
        instances: [
          {
            endDate: "2024-06-08T22:55:22.490842+00:00",
            note: null,
            queuedDttm: "2024-06-08T22:55:22.007508+00:00",
            runId: "run2",
            startDate: "2024-06-08T22:55:22.256614+00:00",
            state: "success",
            taskId: "task_c",
            tryNumber: 1,
          },
          {
            endDate: "2024-06-08T22:55:22.490833+00:00",
            note: null,
            queuedDttm: "2024-06-08T22:55:22.007508+00:00",
            runId: "run1",
            startDate: "2024-06-08T22:55:22.257144+00:00",
            state: "success",
            taskId: "task_c",
            tryNumber: 1,
          },
        ],
        isMapped: false,
        label: "task_c",
        operator: "PythonOperator",
      },
    ],
    id: null,
    instances: [],
    label: null,
  },
  dagRuns: [
    {
      conf: null,
      confIsJson: false,
      dataIntervalEnd: "2024-06-08T00:00:00+00:00",
      dataIntervalStart: "2024-06-07T00:00:00+00:00",
      endDate: "2024-06-08T22:55:23.156380+00:00",
      executionDate: "2024-06-07T00:00:00+00:00",
      externalTrigger: false,
      lastSchedulingDecision: "2024-06-08T22:55:23.151503+00:00",
      note: null,
      queuedAt: "2024-06-08T22:55:19.887507+00:00",
      runId: "run1",
      runType: "scheduled",
      startDate: "2024-06-08T22:55:19.928411+00:00",
      state: "success",
    },
    {
      conf: null,
      confIsJson: false,
      dataIntervalEnd: "2024-06-08T00:00:00+00:00",
      dataIntervalStart: "2024-06-07T00:00:00+00:00",
      endDate: "2024-06-08T22:55:23.134110+00:00",
      executionDate: "2024-06-08T22:55:19.315019+00:00",
      externalTrigger: true,
      lastSchedulingDecision: "2024-06-08T22:55:23.128811+00:00",
      note: null,
      queuedAt: "2024-06-08T22:55:19.368711+00:00",
      runId: "run2",
      runType: "manual",
      startDate: "2024-06-08T22:55:19.928888+00:00",
      state: "success",
    },
  ],
  ordering: ["dataIntervalStart"],
} as GridData;

const task1 = { taskId: "task_a", runId: "run1" };
const task2 = { taskId: "task_c", runId: "run2" };

const dagRunIds = mockGridData.dagRuns.map((dr) => dr.runId);

describe("Test useMultipleSelection hook", () => {
  test("Initial values", async () => {
    const { result } = renderHook(
      () => useMultipleSelection(mockGridData.groups, dagRunIds, dagRunIds),
      { wrapper: Wrapper }
    );

    expect(result.current.selectedTaskInstances).toEqual([]);
  });

  test("Adding single task", async () => {
    const { result } = renderHook(
      () => useMultipleSelection(mockGridData.groups, dagRunIds, dagRunIds),
      { wrapper: Wrapper }
    );

    await act(async () => {
      result.current.onAddSelectedTask(task1);
      result.current.onAddSelectedTask(task2);
    });

    expect(result.current.selectedTaskInstances.length).toBe(2);
    expect(result.current.selectedTaskInstances[0].taskId).toBe(task1.taskId);
    expect(result.current.selectedTaskInstances[0].runId).toBe(task1.runId);
    expect(result.current.selectedTaskInstances[1].taskId).toBe(task2.taskId);
    expect(result.current.selectedTaskInstances[1].runId).toBe(task2.runId);
  });

  test("Adding block of tasks", async () => {
    const { result } = renderHook(
      () => useMultipleSelection(mockGridData.groups, dagRunIds, dagRunIds),
      { wrapper: Wrapper }
    );

    await act(async () => {
      result.current.onAddSelectedTaskBlock(task1);
    });

    await act(async () => {
      result.current.onAddSelectedTaskBlock(task2);
    });

    expect(result.current.selectedTaskInstances.length).toBe(6);
  });
});
