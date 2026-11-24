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

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { BaseWrapper } from "src/utils/Wrapper";

import { Task } from "./Task";

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return { ...actual, useTaskServiceGetTask: vi.fn() };
});
vi.mock("src/hooks/usePluginTabs", () => ({ usePluginTabs: vi.fn(() => []) }));
vi.mock("src/hooks/useRequiredActionTabs", () => ({
  useRequiredActionTabs: vi.fn((_params: unknown, tabs: Array<unknown>) => ({ tabs })),
}));
vi.mock("src/queries/useGridStructure.ts", () => ({ useGridStructure: vi.fn(() => ({ data: undefined })) }));
vi.mock("src/layouts/Details/DetailsLayout", () => ({
  DetailsLayout: ({ children }: PropsWithChildren) => children,
}));
vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return { ...actual, useDocumentTitle: vi.fn() };
});

const { useTaskServiceGetTask } = await import("openapi/queries");

const mockTaskQuery = (result: { error: unknown; isLoading: boolean }) =>
  vi
    .mocked(useTaskServiceGetTask)
    .mockReturnValue({ data: undefined, ...result } as unknown as ReturnType<typeof useTaskServiceGetTask>);

const renderTask = (path: string) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route element={<Task />} path="/dags/:dagId/tasks/group/:groupId" />
          <Route element={<Task />} path="/dags/:dagId/tasks/:taskId" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

describe("Task header", () => {
  it("holds the card's space while the task loads", () => {
    mockTaskQuery({ error: null, isLoading: true });

    renderTask("/dags/my_dag/tasks/my_task");

    expect(screen.getByTestId("header-card-skeleton")).toBeVisible();
  });

  it("says why the task could not be loaded, keeping the id from the URL", () => {
    mockTaskQuery({ error: new Error("request failed"), isLoading: false });

    renderTask("/dags/my_dag/tasks/my_task");

    expect(screen.getByTestId("error-alert")).toHaveTextContent("request failed");
    expect(screen.getByTestId("header-card")).toHaveTextContent("my_task");
  });

  // A group has no task to fetch, so neither the task skeleton nor its error card applies.
  it("shows neither the skeleton nor an error card for a task group", () => {
    mockTaskQuery({ error: null, isLoading: true });

    renderTask("/dags/my_dag/tasks/group/my_group");

    expect(screen.queryByTestId("header-card-skeleton")).not.toBeInTheDocument();
    expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument();
  });
});
