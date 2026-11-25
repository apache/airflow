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
import { QueryClient } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { useDurableExecutionServiceGetDurableExecutionKey } from "openapi/queries";
import { TaskStateStoreService } from "openapi/requests";

import { Wrapper } from "src/utils/Wrapper";

import { ClearAllTaskStateStoreButton } from "./ClearAllTaskStateStoreButton";

afterEach(() => vi.restoreAllMocks());

it("refreshes the durable execution banner after clearing", async () => {
  vi.spyOn(TaskStateStoreService, "clearTaskStateStore").mockResolvedValue(undefined);
  const invalidate = vi.spyOn(QueryClient.prototype, "invalidateQueries");

  render(
    <Wrapper>
      <ClearAllTaskStateStoreButton dagId="dag" mapIndex={-1} runId="run" taskId="task" />
    </Wrapper>,
  );
  fireEvent.click(screen.getByRole("button", { name: /taskStateStore.clearAll.title/u }));
  const dialog = await screen.findByRole("dialog");

  fireEvent.click(within(dialog).getByRole("button", { name: /taskStateStore.clearAll.title/u }));

  await waitFor(() =>
    expect(invalidate).toHaveBeenCalledWith({ queryKey: [useDurableExecutionServiceGetDurableExecutionKey] }),
  );
});
