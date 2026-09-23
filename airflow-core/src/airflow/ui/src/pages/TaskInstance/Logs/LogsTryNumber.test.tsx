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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { UseTaskInstanceServiceGetMappedTaskInstanceKeyFn } from "openapi/queries";
import { TaskInstanceService, type TaskInstanceResponse } from "openapi/requests";

import { useLogs } from "src/queries/useLogs";
import { BaseWrapper } from "src/utils/Wrapper";

import { Logs } from "./Logs";
import type { TaskLogHeaderProps } from "./TaskLogHeader";

vi.mock("src/queries/useConfig", () => ({ useConfig: () => false }));
vi.mock("src/hooks/useShortcut", () => ({ useShortcut: vi.fn() }));
vi.mock("src/utils", async () => ({
  ...(await vi.importActual("src/utils")),
  useAutoRefresh: () => false,
}));
vi.mock("src/queries/useLogs", () => ({
  useLogs: vi.fn(() => ({
    fetchedData: undefined,
    parsedData: { parsedLogs: [], searchableText: [], sources: [] },
  })),
}));
vi.mock("./TaskLogContent", () => ({ TaskLogContent: () => <div data-testid="log-content" /> }));
vi.mock("./TaskLogHeader", () => ({
  TaskLogHeader: ({ onSelectTryNumber }: TaskLogHeaderProps) => (
    <>
      {[1, 2, 3].map((tryNumber) => (
        <button key={tryNumber} onClick={() => onSelectTryNumber(tryNumber)} type="button">
          {tryNumber}
        </button>
      ))}
    </>
  ),
}));

afterEach(() => vi.restoreAllMocks());

const SearchParams = () => <div data-testid="search-params">{useLocation().search}</div>;

const renderLogs = (state: TaskInstanceResponse["state"], search = "", tryNumber = 3) => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });

  vi.spyOn(TaskInstanceService, "getMappedTaskInstance").mockResolvedValue({
    dag_id: "dag",
    dag_run_id: "run",
    map_index: -1,
    state,
    task_id: "task",
    try_number: tryNumber,
  } as TaskInstanceResponse);

  vi.spyOn(TaskInstanceService, "getMappedTaskInstanceTries").mockResolvedValue({
    task_instances: [],
    total_entries: 0,
  });
  render(
    <BaseWrapper>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={[`/${search}`]}>
          <SearchParams />
          <Logs />
        </MemoryRouter>
      </QueryClientProvider>
    </BaseWrapper>,
  );

  return { queryClient };
};

const expectLogTry = async (tryNumber: number) => {
  await waitFor(() =>
    expect(useLogs).toHaveBeenLastCalledWith(expect.objectContaining({ tryNumber }), expect.any(Object)),
  );
};

describe("Task log try selection", () => {
  it.each(["up_for_retry", null] as const)("disables pending log fetching in %s", async (state) => {
    renderLogs(state);
    await waitFor(() =>
      expect(useLogs).toHaveBeenLastCalledWith(expect.objectContaining({ tryNumber: 3 }), { enabled: false }),
    );
    expect(
      screen.getByText(state === "up_for_retry" ? "logs.waitingToRetry" : "logs.tryNotStarted"),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("log-content")).not.toBeInTheDocument();
  });
  it.each([
    ["up_for_retry", 3],
    ["running", 3],
    ["failed", 3],
    ["success", 3],
    ["scheduled", 3],
    ["queued", 3],
    ["deferred", 3],
    ["up_for_reschedule", 3],
  ] as const)("uses the appropriate default try for %s", async (state, expectedTry) => {
    renderLogs(state);

    await expectLogTry(expectedTry);
  });

  it.each([1, 2, 3])("honors explicit try %i while waiting for retry", async (tryNumber) => {
    renderLogs("up_for_retry", `?try_number=${tryNumber}`);

    await expectLogTry(tryNumber);
  });

  it("allows selecting the upcoming try and returning to the failed try", async () => {
    renderLogs("up_for_retry");
    await expectLogTry(3);

    fireEvent.click(screen.getByRole("button", { name: "logs.viewFailedTry" }));
    await expectLogTry(2);
    expect(useLogs).toHaveBeenLastCalledWith(expect.objectContaining({ tryNumber: 2 }), { enabled: true });
    expect(screen.getByTestId("search-params")).toHaveTextContent("?try_number=2");

    fireEvent.click(screen.getByRole("button", { name: "3" }));
    await expectLogTry(3);
    expect(screen.getByTestId("search-params")).toBeEmptyDOMElement();
    expect(useLogs).toHaveBeenLastCalledWith(expect.objectContaining({ tryNumber: 3 }), { enabled: false });
  });

  it("starts fetching logs when the pending try starts running", async () => {
    const { queryClient } = renderLogs("up_for_retry");

    await expectLogTry(3);

    await act(() =>
      queryClient.setQueryData(
        UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({
          dagId: "",
          dagRunId: "",
          mapIndex: -1,
          taskId: "",
        }),
        { dag_id: "dag", dag_run_id: "run", map_index: -1, state: "running", task_id: "task", try_number: 3 },
      ),
    );
    await waitFor(() =>
      expect(useLogs).toHaveBeenLastCalledWith(expect.objectContaining({ tryNumber: 3 }), { enabled: true }),
    );
    expect(screen.queryByText("logs.waitingToRetry")).not.toBeInTheDocument();
  });

  it("does not offer previous logs for a task that has never run", async () => {
    renderLogs(null, "", 0);
    await expectLogTry(0);
    expect(screen.getByText("logs.tryNotStarted")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "logs.viewPreviousTry" })).not.toBeInTheDocument();
  });
});
