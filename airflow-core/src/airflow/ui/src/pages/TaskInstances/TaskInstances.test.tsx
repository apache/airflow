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
import type { ReactNode } from "react";

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import type * as ReactI18Next from "react-i18next";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { TaskInstances } from "./TaskInstances";

let mockParams: Record<string, string | undefined> = {
  dagId: "example_dag",
  runId: "manual__2026-06-07T00:00:00+00:00",
  taskId: "mapped_task",
};
let mockSearchParams = new URLSearchParams();

vi.mock("react-i18next", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactI18Next>();

  return {
    ...actual,
    useTranslation: () => ({
      i18n: { language: "en" },
      // eslint-disable-next-line id-length
      t: (key: string) => key,
    }),
  };
});

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return {
    ...actual,
    useParams: () => mockParams,
    useSearchParams: () => [mockSearchParams, vi.fn()] as const,
  };
});

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useTaskInstanceServiceGetTaskInstances: vi.fn(),
  };
});

vi.mock("./TaskInstancesFilter", () => ({
  TaskInstancesFilter: () => null,
}));

vi.mock("src/components/DataTable", () => ({
  DataTable: ({
    columns,
    data,
  }: {
    readonly columns: ReadonlyArray<{
      accessorKey?: string;
      cell?: (props: { row: { original: TaskInstanceResponse } }) => ReactNode;
      header?: unknown;
    }>;
    readonly data: ReadonlyArray<TaskInstanceResponse>;
  }) => {
    const renderedColumns = columns.filter(
      (column) => column.accessorKey === "rendered_map_index" || column.accessorKey === "dag_run_id",
    );
    const iterationColumn = columns.find((column) => column.accessorKey === "loop_iteration");

    return (
      <table>
        <thead>
          <tr>
            {iterationColumn === undefined ? undefined : <th>{iterationColumn.header as ReactNode}</th>}
          </tr>
        </thead>
        <tbody>
          {data.map((taskInstance) => (
            <tr key={`${taskInstance.task_id}-${taskInstance.map_index}`}>
              {renderedColumns.map((column) => (
                <td key={column.accessorKey}>{column.cell?.({ row: { original: taskInstance } })}</td>
              ))}
              <td>{iterationColumn?.cell?.({ row: { original: taskInstance } })}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  },
}));

const { useTaskInstanceServiceGetTaskInstances } = await import("openapi/queries");

const getTaskInstancesResponse = (taskInstances: Array<TaskInstanceResponse>) =>
  ({
    data: {
      task_instances: taskInstances,
    },
    error: null,
    isLoading: false,
  }) as ReturnType<typeof useTaskInstanceServiceGetTaskInstances>;

const mappedTaskInstance = {
  dag_display_name: "example_dag",
  dag_id: "example_dag",
  dag_run_id: "manual__2026-06-07T00:00:00+00:00",
  id: "mapped-task-instance-id",
  map_index: 1,
  region_id: "11111111-1111-4111-8111-111111111111",
  region_index: 1,
  rendered_map_index: "1",
  start_date: null,
  state: "queued",
  task_display_name: "mapped_task",
  task_id: "mapped_task",
  try_number: 2,
} as TaskInstanceResponse;

describe("TaskInstances", () => {
  beforeEach(() => {
    mockParams = {
      dagId: "example_dag",
      runId: "manual__2026-06-07T00:00:00+00:00",
      taskId: "mapped_task",
    };
    mockSearchParams = new URLSearchParams();
  });

  it.each([
    {
      description: "with a selected task but no run",
      params: {
        dagId: "example_dag",
        taskId: "mapped_task",
      },
    },
    {
      description: "without a selected task or run",
      params: {},
    },
  ])("links mapped task instances from the map index column $description", ({ params }) => {
    mockParams = params;
    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue(
      getTaskInstancesResponse([mappedTaskInstance]),
    );

    render(<TaskInstances />, { wrapper: Wrapper });

    expect(screen.getByRole("link", { name: "1" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/manual__2026-06-07T00:00:00+00:00/tasks/mapped_task/mapped/1?region_id=11111111-1111-4111-8111-111111111111&region_index=1&try_number=2",
    );
  });

  it("does not link non-mapped task instances from the map index column", () => {
    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue(
      getTaskInstancesResponse([{ ...mappedTaskInstance, map_index: -1, rendered_map_index: null }]),
    );

    render(<TaskInstances />, { wrapper: Wrapper });

    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });

  it("links task instances to their Dag run from the Dag run ID column", () => {
    mockParams = {};
    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue(
      getTaskInstancesResponse([mappedTaskInstance]),
    );

    render(<TaskInstances />, { wrapper: Wrapper });

    expect(screen.getByRole("link", { name: "manual__2026-06-07T00:00:00+00:00" })).toHaveAttribute(
      "href",
      "/dags/example_dag/runs/manual__2026-06-07T00:00:00+00:00",
    );
  });

  it("shows the loop iteration number separately from mapped slots", () => {
    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue(
      getTaskInstancesResponse([
        {
          ...mappedTaskInstance,
          loop_iteration: { iteration: 2, loop_id: "loop" },
        },
      ]),
    );
    render(<TaskInstances />, { wrapper: Wrapper });
    expect(screen.getByRole("link", { name: "1" })).toBeInTheDocument();
    expect(screen.getByText("2")).toBeInTheDocument();
    expect(screen.queryByText("loop: 2")).not.toBeInTheDocument();
  });

  it.each([
    { loopIteration: { iteration: 0, loop_id: "loop" }, shown: true },
    { loopIteration: null, shown: false },
    { loopIteration: undefined, shown: false },
  ])("shows the iteration column only when a task instance has one: $shown", ({ loopIteration, shown }) => {
    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue(
      getTaskInstancesResponse([
        { ...mappedTaskInstance, loop_iteration: undefined },
        { ...mappedTaskInstance, loop_iteration: loopIteration, map_index: 2 },
      ]),
    );
    render(<TaskInstances />, { wrapper: Wrapper });
    expect(screen.queryByText("taskInstance.iteration") !== null).toBe(shown);
  });

  it("shows the stored rendered map index label of an unmapped task instance", () => {
    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue(
      getTaskInstancesResponse([{ ...mappedTaskInstance, map_index: -1, rendered_map_index: "eu-west" }]),
    );
    render(<TaskInstances />, { wrapper: Wrapper });
    expect(screen.getByText("eu-west")).toBeInTheDocument();
  });
});
