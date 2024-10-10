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

import React from "react";
import { fireEvent, render } from "@testing-library/react";

import { Wrapper } from "src/utils/testUtils";

import type { NodeProps } from "reactflow";
import type { Task, TaskInstance } from "src/types";
import type { CustomNodeProps } from "./Node";
import DagNode from "./DagNode";

const mockNode: NodeProps<CustomNodeProps> = {
  id: "task_id",
  data: {
    label: "task_id",
    height: 50,
    width: 200,
    class: "dag",
    instance: {
      state: "success",
      runId: "run_id",
      taskId: "task_id",
      startDate: "",
      endDate: "",
      note: "",
    },
    task: {
      id: "task_id",
      label: "task_id",
      instances: [],
      operator: "operator",
    },
    isSelected: false,
    latestDagRunId: "run_id",
    onToggleCollapse: () => {},
    isActive: true,
    isZoomedOut: false,
  },
  selected: false,
  zIndex: 0,
  type: "custom",
  isConnectable: false,
  xPos: 0,
  yPos: 0,
  dragging: false,
};

describe("Test Graph Node", () => {
  test("Renders normal task correctly", async () => {
    const { getByText, getByTestId } = render(<DagNode {...mockNode} />, {
      wrapper: Wrapper,
    });

    expect(getByTestId("node")).toHaveStyle("opacity: 1");
    expect(getByText("success")).toBeInTheDocument();
    expect(getByText("task_id")).toBeInTheDocument();
    expect(getByText("operator")).toBeInTheDocument();
  });

  test("Renders mapped task correctly", async () => {
    const { getByText } = render(
      <DagNode
        {...mockNode}
        data={{
          ...mockNode.data,
          task: { ...mockNode.data.task, isMapped: true } as Task,
          instance: {
            ...mockNode.data.instance,
            mappedStates: { success: 4 },
          } as TaskInstance,
        }}
      />,
      {
        wrapper: Wrapper,
      }
    );

    expect(getByText("success")).toBeInTheDocument();
    expect(getByText("task_id [4]")).toBeInTheDocument();
  });

  test("Renders task group correctly", async () => {
    const { getByText } = render(
      <DagNode
        {...mockNode}
        data={{ ...mockNode.data, childCount: 5, isOpen: false }}
      />,
      {
        wrapper: Wrapper,
      }
    );

    expect(getByText("success")).toBeInTheDocument();
    expect(getByText("task_id")).toBeInTheDocument();
  });

  test("Renders normal task correctly", async () => {
    const { getByTestId } = render(
      <DagNode {...mockNode} data={{ ...mockNode.data, isActive: false }} />,
      {
        wrapper: Wrapper,
      }
    );

    expect(getByTestId("node")).toHaveStyle("opacity: 0.3");
  });

  test("Clicks on taskName", async () => {
    const { getByText } = render(<DagNode {...mockNode} />, {
      wrapper: Wrapper,
    });

    const taskName = getByText("task_id");

    fireEvent.click(taskName);

    expect(taskName).toBeInTheDocument();
  });
});
