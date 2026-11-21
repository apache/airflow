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
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useDurableExecutionServiceGetDurableExecution } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests";

import { Wrapper } from "src/utils/Wrapper";

import { DurableBanner } from "./DurableBanner";

vi.mock("src/utils", async () => ({
  ...(await vi.importActual("src/utils")),
  useAutoRefresh: () => 3000,
}));

vi.mock("openapi/queries", async () => ({
  ...(await vi.importActual("openapi/queries")),
  useDurableExecutionServiceGetDurableExecution: vi.fn(),
}));

const TRIES = [
  {
    decision: "submitted",
    external_id: "run_1",
    saved_seconds: null,
    status: null,
    try_number: 1,
  },
  {
    decision: "reconnected",
    external_id: "run_1",
    saved_seconds: 90,
    status: "success",
    try_number: 2,
  },
  {
    decision: "already_succeeded",
    external_id: "run_1",
    saved_seconds: null,
    status: "success",
    try_number: 3,
  },
  {
    decision: "reconnected",
    external_id: "run_1",
    saved_seconds: null,
    status: "running",
    try_number: 4,
  },
  {
    decision: "reconnected",
    external_id: "run_1",
    saved_seconds: null,
    status: "success",
    try_number: 5,
  },
  {
    decision: "reconnected",
    external_id: "run_1",
    saved_seconds: null,
    status: "failed",
    try_number: 6,
  },
];

const renderFor = (tryNumber: number, taskInstance: Partial<TaskInstanceResponse> = {}) => {
  vi.mocked(useDurableExecutionServiceGetDurableExecution).mockReturnValue({
    data: { tries: TRIES },
  } as unknown as ReturnType<typeof useDurableExecutionServiceGetDurableExecution>);

  return render(
    <Wrapper>
      <DurableBanner
        taskInstance={
          {
            dag_id: "d",
            dag_run_id: "r",
            map_index: -1,
            task_id: "t",
            ...taskInstance,
          } as TaskInstanceResponse
        }
        tryNumber={tryNumber}
      />
    </Wrapper>,
  );
};

describe("DurableBanner", () => {
  it.each([
    [2, "logs.durable.title.reconnected", "logs.durable.detail.reconnected"],
    [3, "logs.durable.title.already_succeeded", "logs.durable.detail.already_succeeded"],
    [4, "logs.durable.title.reconnected", "logs.durable.detail.reconnectedRunning"],
    [5, "logs.durable.title.reconnected", "logs.durable.detail.reconnectedNoDuration"],
    [6, "logs.durable.title.reconnected", "logs.durable.detail.reconnectedFailed"],
  ])("shows what durable execution did on try %i", (tryNumber, title, detail) => {
    renderFor(tryNumber);

    expect(screen.getByText(title)).toBeInTheDocument();
    expect(screen.getByText(detail)).toBeInTheDocument();
  });

  it("shows the job status as an icon-only badge", () => {
    renderFor(4);

    const badge = screen.getByTestId("state-badge");

    expect(badge).toHaveAccessibleName("common:states.running");
    expect(badge).not.toHaveTextContent(/\S/u);
  });

  it.each([
    {
      endDate: null,
      expected: 3000,
      fetchedAt: "2026-11-10T10:00:00Z",
      name: "the task is still running",
      state: "running",
    },
    {
      endDate: "2026-11-10T10:00:05Z",
      expected: 3000,
      fetchedAt: "2026-11-10T10:00:00Z",
      name: "the last fetch predates the task end",
      state: "success",
    },
    {
      endDate: "2026-11-10T10:00:05Z",
      expected: false,
      fetchedAt: "2026-11-10T10:00:10Z",
      name: "the last fetch came after the task end",
      state: "success",
    },
  ] as const)("refetch interval when $name", ({ endDate, expected, fetchedAt, state }) => {
    renderFor(4, { end_date: endDate, state });

    const options = vi.mocked(useDurableExecutionServiceGetDurableExecution).mock.lastCall?.[2];
    const refetchInterval = options?.refetchInterval as (query: {
      state: { dataUpdatedAt: number };
    }) => number | false;

    expect(refetchInterval({ state: { dataUpdatedAt: Date.parse(fetchedAt) } })).toBe(expected);
  });

  it("does not fetch the record on a first try", () => {
    renderFor(1);

    expect(vi.mocked(useDurableExecutionServiceGetDurableExecution)).toHaveBeenLastCalledWith(
      expect.anything(),
      undefined,
      expect.objectContaining({ enabled: false }),
    );
    expect(screen.queryByText("logs.durable", { exact: false })).not.toBeInTheDocument();
  });
});
