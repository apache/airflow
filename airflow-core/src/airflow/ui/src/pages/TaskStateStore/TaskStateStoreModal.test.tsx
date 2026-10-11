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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";

import { TaskStateStoreService } from "openapi/requests";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import { TaskStateStoreModal } from "./TaskStateStoreModal";

dayjs.extend(utc);

vi.mock("src/components/JsonEditor", () => ({
  JsonEditor: ({ value }: { readonly value: string }) => <div data-testid="editor">{value}</div>,
}));
afterEach(() => vi.restoreAllMocks());

it("updates the exact regional store selected in the task URL", async () => {
  const regionId = "11111111-1111-1111-1111-111111111111";
  const value = { expires_at: null, key: "progress", updated_at: "2026-01-01T00:00:00Z", value: 3 };
  const fetch = vi.spyOn(TaskStateStoreService, "getTaskStateStore").mockResolvedValue(value);
  const update = vi.spyOn(TaskStateStoreService, "setTaskStateStore").mockResolvedValue(undefined);

  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/?region_id=${regionId}&region_index=3`]}>
        <TimezoneProvider>
          <TaskStateStoreModal
            dagId="dag"
            isOpen
            mapIndex={-1}
            mode="edit"
            onClose={vi.fn()}
            runId="run"
            storeKey="progress"
            taskId="member"
          />
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );

  await waitFor(() => expect(screen.getByTestId("editor")).toHaveTextContent("3"));
  fireEvent.click(screen.getByRole("button", { name: "common:modal.save" }));
  const coordinates = {
    dagId: "dag",
    dagRunId: "run",
    key: "progress",
    mapIndex: -1,
    regionId,
    regionIndex: 3,
    taskId: "member",
  };

  await waitFor(() =>
    expect(update).toHaveBeenCalledWith({ ...coordinates, requestBody: { expires_at: null, value: 3 } }),
  );
  expect(fetch).toHaveBeenCalledWith(coordinates);
});
