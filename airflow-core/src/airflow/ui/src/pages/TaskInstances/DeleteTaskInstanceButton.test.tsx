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
import { afterEach, describe, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { BaseWrapper } from "src/utils/Wrapper";

import DeleteTaskInstanceButton from "./DeleteTaskInstanceButton";

afterEach(() => vi.restoreAllMocks());

const regionId = "22222222-2222-4222-8222-222222222222";
const taskInstance = { dag_id: "dag", dag_run_id: "run", map_index: -1, task_id: "work" };

describe("DeleteTaskInstanceButton", () => {
  it.each([
    ["a loop pass", { region_id: regionId, region_index: 2 }, { regionId, regionIndex: 2 }],
    ["a task outside any region", {}, { regionId: undefined, regionIndex: undefined }],
  ])("deletes %s by its coordinates", async (_name, region, expected) => {
    const deleteTaskInstance = vi.spyOn(TaskInstanceService, "deleteTaskInstance").mockResolvedValue(null);

    render(
      <BaseWrapper>
        <DeleteTaskInstanceButton taskInstance={{ ...taskInstance, ...region } as TaskInstanceResponse} />
      </BaseWrapper>,
    );
    fireEvent.click(screen.getByRole("button"));
    fireEvent.click(await screen.findByTestId("delete-confirm-button"));

    await waitFor(() => expect(deleteTaskInstance).toHaveBeenCalled());
    expect(deleteTaskInstance.mock.lastCall?.[0]).toStrictEqual({
      dagId: "dag",
      dagRunId: "run",
      mapIndex: -1,
      taskId: "work",
      ...expected,
    });
  });
});
