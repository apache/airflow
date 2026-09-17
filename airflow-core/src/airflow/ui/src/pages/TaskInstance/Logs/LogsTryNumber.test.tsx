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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { TaskInstanceService, type TaskInstanceResponse } from "openapi/requests";

import { useLogs } from "src/queries/useLogs";
import { BaseWrapper } from "src/utils/Wrapper";

import { Logs } from "./Logs";
import type { TaskLogHeaderProps } from "./TaskLogHeader";

vi.mock("src/queries/useConfig", () => ({ useConfig: () => false }));
vi.mock("src/hooks/useShortcut", () => ({ useShortcut: vi.fn() }));
vi.mock("src/queries/useLogs", () => ({
  useLogs: vi.fn(() => ({
    fetchedData: undefined,
    parsedData: { parsedLogs: [], searchableText: [], sources: [] },
  })),
}));
vi.mock("./TaskLogContent", () => ({ TaskLogContent: () => null }));
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

const renderLogs = (state: TaskInstanceResponse["state"], search = "") => {
  vi.spyOn(TaskInstanceService, "getMappedTaskInstance").mockResolvedValue({
    state,
    try_number: 3,
  } as TaskInstanceResponse);

  return render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/${search}`]}>
        <Logs />
      </MemoryRouter>
    </BaseWrapper>,
  );
};

const expectLogTry = async (tryNumber: number) => {
  await waitFor(() => expect(useLogs).toHaveBeenLastCalledWith(expect.objectContaining({ tryNumber })));
};

describe("Task log attempt selection", () => {
  it.each([
    ["up_for_retry", 2],
    ["running", 3],
    ["failed", 3],
    ["success", 3],
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
    await expectLogTry(2);

    fireEvent.click(screen.getByRole("button", { name: "3" }));
    await expectLogTry(3);

    fireEvent.click(screen.getByRole("button", { name: "2" }));
    await expectLogTry(2);
  });
});
