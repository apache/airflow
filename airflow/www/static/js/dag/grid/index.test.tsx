/* eslint-disable class-methods-use-this */
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

/* global describe, test, expect, beforeEach, beforeAll, jest, window */

import React from "react";
import { render, fireEvent, waitFor } from "@testing-library/react";

import { Wrapper } from "src/utils/testUtils";
import * as useGridDataModule from "src/api/useGridData";
import useToggleGroups from "src/dag/useToggleGroups";

import Grid from ".";

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
} as useGridDataModule.GridData;

const EXPAND = "Expand all task groups";
const COLLAPSE = "Collapse all task groups";

const GridWithGroups = () => {
  const { openGroupIds, onToggleGroups } = useToggleGroups();
  return <Grid openGroupIds={openGroupIds} onToggleGroups={onToggleGroups} />;
};

describe("Test ToggleGroups", () => {
  beforeAll(() => {
    class ResizeObserver {
      observe() {}

      unobserve() {}

      disconnect() {}
    }

    window.ResizeObserver = ResizeObserver;
  });

  beforeEach(() => {
    const returnValue = {
      data: mockGridData,
      isSuccess: true,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any;

    jest
      .spyOn(useGridDataModule, "default")
      .mockImplementation(() => returnValue);
  });

  test("Group defaults to closed", () => {
    const { getByTestId, getByText, getAllByTestId } = render(
      <Grid openGroupIds={[]} onToggleGroups={() => {}} />,
      { wrapper: Wrapper }
    );

    const groupName = getByText("group_1");

    expect(getAllByTestId("task-instance")).toHaveLength(1);
    expect(groupName).toBeInTheDocument();
    expect(getByTestId("open-group")).toBeInTheDocument();
  });

  test("Buttons are disabled if all groups are expanded or collapsed", () => {
    const { getByTitle, queryAllByTestId } = render(<GridWithGroups />, {
      wrapper: Wrapper,
    });

    const expandButton = getByTitle(EXPAND);
    const collapseButton = getByTitle(COLLAPSE);

    expect(expandButton).toBeEnabled();
    expect(collapseButton).toBeDisabled();

    const taskElements = queryAllByTestId("task-instance");
    expect(taskElements).toHaveLength(1);

    fireEvent.click(expandButton);

    const newTaskElements = queryAllByTestId("task-instance");
    expect(newTaskElements).toHaveLength(3);

    expect(collapseButton).toBeEnabled();
    expect(expandButton).toBeDisabled();
  });

  test("Expand/collapse buttons toggle nested groups", async () => {
    const { getByText, queryAllByTestId, getByTitle } = render(
      <GridWithGroups />,
      { wrapper: Wrapper }
    );

    const expandButton = getByTitle(EXPAND);
    const collapseButton = getByTitle(COLLAPSE);

    const groupName = getByText("group_1");

    expect(queryAllByTestId("task-instance")).toHaveLength(3);
    expect(groupName).toBeInTheDocument();

    expect(queryAllByTestId("close-group")).toHaveLength(2);
    expect(queryAllByTestId("open-group")).toHaveLength(0);

    fireEvent.click(collapseButton);

    await waitFor(() =>
      expect(queryAllByTestId("task-instance")).toHaveLength(1)
    );
    expect(queryAllByTestId("close-group")).toHaveLength(0);
    // Since the groups are nested, only the parent row is rendered
    expect(queryAllByTestId("open-group")).toHaveLength(1);

    fireEvent.click(expandButton);

    await waitFor(() =>
      expect(queryAllByTestId("task-instance")).toHaveLength(3)
    );
    expect(queryAllByTestId("close-group")).toHaveLength(2);
    expect(queryAllByTestId("open-group")).toHaveLength(0);
  });

  test("Hovered effect on task state", async () => {
    const openGroupIds = ["group_1", "task_1"];
    const { rerender, queryAllByTestId } = render(
      <Grid openGroupIds={openGroupIds} onToggleGroups={() => {}} />,
      { wrapper: Wrapper }
    );

    const taskElements = queryAllByTestId("task-instance");
    expect(taskElements).toHaveLength(3);

    taskElements.forEach((taskElement) => {
      expect(taskElement).toHaveStyle("opacity: 1");
    });

    rerender(
      <Grid
        hoveredTaskState="success"
        openGroupIds={openGroupIds}
        onToggleGroups={() => {}}
      />
    );

    taskElements.forEach((taskElement) => {
      expect(taskElement).toHaveStyle("opacity: 1");
    });

    rerender(
      <Grid
        hoveredTaskState="failed"
        openGroupIds={openGroupIds}
        onToggleGroups={() => {}}
      />
    );

    taskElements.forEach((taskElement) => {
      expect(taskElement).toHaveStyle("opacity: 0.3");
    });
  });
});
