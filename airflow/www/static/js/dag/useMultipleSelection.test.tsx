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

const mockGridData = {
  groups: {
    id: null,
    label: null,
    children: [
      {
        id: "group_1",
        label: "group_1",
        instances: [
          {
            endDate: "2021-10-26T15:42:03.391939+00:00",
            runId: "run1",
            startDate: "2021-10-26T15:42:03.391917+00:00",
            state: "success",
            taskId: "group_1",
            tryNumber: 1,
            note: "abc",
          },
        ],
        children: [
          {
            id: "group_1.task_1",
            operator: "DummyOperator",
            label: "task_1",
            instances: [
              {
                endDate: "2021-10-26T15:42:03.391939+00:00",
                runId: "run1",
                startDate: "2021-10-26T15:42:03.391917+00:00",
                state: "success",
                taskId: "group_1.task_1",
                tryNumber: 1,
                note: "abc",
              },
            ],
            children: [
              {
                id: "group_1.task_1.sub_task_1",
                label: "sub_task_1",
                extraLinks: [],
                operator: "DummyOperator",
                instances: [
                  {
                    endDate: "2021-10-26T15:42:03.391939+00:00",
                    runId: "run1",
                    startDate: "2021-10-26T15:42:03.391917+00:00",
                    state: "success",
                    taskId: "group_1.task_1.sub_task_1",
                    tryNumber: 1,
                    note: "abc",
                  },
                ],
              },
            ],
          },
        ],
      },
      {
        id: "group_2",
        label: "group_2",
        instances: [
          {
            endDate: "2021-10-26T15:42:03.391939+00:00",
            runId: "run1",
            startDate: "2021-10-26T15:42:03.391917+00:00",
            state: "success",
            taskId: "group_1",
            tryNumber: 1,
          },
        ],
        children: [
          {
            id: "group_2.task_1",
            operator: "DummyOperator",
            label: "task_1",
            instances: [
              {
                endDate: "2021-10-26T15:42:03.391939+00:00",
                runId: "run1",
                startDate: "2021-10-26T15:42:03.391917+00:00",
                state: "success",
                taskId: "group_1.task_1",
                tryNumber: 1,
              },
            ],
            children: [
              {
                id: "group_2.task_1.sub_task_1",
                label: "sub_task_1",
                extraLinks: [],
                operator: "DummyOperator",
                instances: [
                  {
                    endDate: "2021-10-26T15:42:03.391939+00:00",
                    runId: "run1",
                    startDate: "2021-10-26T15:42:03.391917+00:00",
                    state: "success",
                    taskId: "group_2.task_1.sub_task_1",
                    tryNumber: 1,
                  },
                ],
              },
            ],
          },
        ],
      },
    ],
    instances: [],
  },
  dagRuns: [
    {
      runId: "run1",
      dataIntervalStart: "2021-11-08T21:14:19.704433+00:00",
      dataIntervalEnd: "2021-11-08T21:17:13.206426+00:00",
      queuedAt: "2021-11-08T21:14:19.604433+00:00",
      startDate: "2021-11-08T21:14:19.704433+00:00",
      endDate: "2021-11-08T21:17:13.206426+00:00",
      state: "failed",
      runType: "scheduled",
      executionDate: "2021-11-08T21:14:19.704433+00:00",
      lastSchedulingDecision: "2021-11-08T21:14:19.704433+00:00",
      note: "myCoolDagRun",
      externalTrigger: false,
      conf: null,
      confIsJson: false,
    },
  ],
  ordering: ["dataIntervalStart"],
} as GridData;

const task1 = { taskId: "group_1.task_1.sub_task_1", runId: "run1" };
const task2 = { taskId: "group_2.task_1.sub_task_1", runId: "run1" };

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
    });

    expect(result.current.selectedTaskInstances[0].taskId).toBe(task1.taskId);
    expect(result.current.selectedTaskInstances[0].runId).toBe(task1.runId);
  });

  test("Adding block of tasks", async () => {
    const { result } = renderHook(
      () => useMultipleSelection(mockGridData.groups, dagRunIds, dagRunIds),
      { wrapper: Wrapper }
    );

    await act(async () => {
      result.current.onAddSelectedTask(task1);
    });

    await act(async () => {
      result.current.onAddSelectedTaskBlock(task2);
    });

    expect(result.current.selectedTaskInstances.length).toBe(3);
  });
});
