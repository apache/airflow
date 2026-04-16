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
import { render } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { Task } from "./Task";

let mockParams: Record<string, string | undefined> = {
  dagId: "example_dag",
  groupId: "group_a",
  runId: undefined,
  taskId: undefined,
};

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: () => mockParams,
  };
});

vi.mock("openapi/queries", () => ({
  useTaskServiceGetTask: vi.fn(() => ({
    data: undefined,
    error: undefined,
    isLoading: false,
  })),
}));

vi.mock("src/hooks/usePluginTabs", () => ({
  usePluginTabs: () => [],
}));

vi.mock("src/hooks/useRequiredActionTabs", () => ({
  useRequiredActionTabs: (_args: unknown, tabs: Array<unknown>) => ({ tabs }),
}));

vi.mock("src/layouts/Details/DetailsLayout", () => ({
  DetailsLayout: ({ children }: { readonly children: ReactNode }) => <div>{children}</div>,
}));

vi.mock("src/queries/useGridStructure.ts", () => ({
  useGridStructure: () => ({ data: undefined }),
}));

vi.mock("src/utils/groupTask", () => ({
  getGroupTask: () => undefined,
}));

vi.mock("./Header", () => ({
  Header: () => <div>Header</div>,
}));

vi.mock("./GroupTaskHeader", () => ({
  GroupTaskHeader: () => <div>GroupTaskHeader</div>,
}));

const { useTaskServiceGetTask } = await import("openapi/queries");

describe("Task page API behavior", () => {
  beforeEach(vi.clearAllMocks);

  it("does not issue task endpoint call for task-group route", () => {
    mockParams = {
      dagId: "example_dag",
      groupId: "group_a",
      runId: undefined,
      taskId: undefined,
    };

    render(
      <Wrapper>
        <Task />
      </Wrapper>,
    );

    expect(useTaskServiceGetTask).toHaveBeenCalledWith({ dagId: "example_dag", taskId: "" }, undefined, {
      enabled: false,
    });
    expect(useTaskServiceGetTask).toHaveBeenCalledTimes(1);
  });

  it("fetches task details on task route", () => {
    mockParams = {
      dagId: "example_dag",
      groupId: undefined,
      runId: undefined,
      taskId: "task_a",
    };

    render(
      <Wrapper>
        <Task />
      </Wrapper>,
    );

    expect(useTaskServiceGetTask).toHaveBeenCalledWith(
      { dagId: "example_dag", taskId: "task_a" },
      undefined,
      { enabled: true },
    );
    expect(useTaskServiceGetTask).toHaveBeenCalledTimes(1);
  });

  it("keeps task query disabled when group and task params both exist", () => {
    mockParams = {
      dagId: "example_dag",
      groupId: "group_a",
      runId: undefined,
      taskId: "task_a",
    };

    render(
      <Wrapper>
        <Task />
      </Wrapper>,
    );

    expect(useTaskServiceGetTask).toHaveBeenCalledWith(
      { dagId: "example_dag", taskId: "task_a" },
      undefined,
      { enabled: false },
    );
    expect(useTaskServiceGetTask).toHaveBeenCalledTimes(1);
  });
});
