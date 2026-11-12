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
import { http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { DAGRunResponse } from "openapi/requests/types.gen";

import { CLEAR_KEEP_TASK_STATE_KEY } from "src/constants/localStorage";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import BulkClearDagRunsButton from "./BulkClearDagRunsButton";

const dagRuns = [
  { dag_id: "test_dag", dag_run_id: "run_1" },
  { dag_id: "test_dag", dag_run_id: "run_2" },
] as unknown as Array<DAGRunResponse>;

vi.mock("src/queries/useBulkClearDagRunsDryRun", () => ({
  useBulkClearDagRunsDryRun: () => ({
    data: { task_instances: [{ dag_run_id: "run_1", task_id: "t" }], total_entries: 1 },
    isFetching: false,
  }),
  useBulkClearDagRunsDryRunKey: "bulkClearDagRunsDryRun",
}));

let clearBody: Record<string, unknown> | undefined;

beforeEach(() => {
  server.use(
    http.post("/api/v2/dags/~/clearDagRuns", async ({ request }) => {
      clearBody = (await request.json()) as Record<string, unknown>;

      return HttpResponse.json({ dag_runs: [], total_entries: 0 });
    }),
  );
});
afterEach(() => {
  clearBody = undefined;
  localStorage.clear();
});

describe("BulkClearDagRunsButton", () => {
  it.each([
    { expected: undefined, stored: false, tick: false },
    { expected: true, stored: false, tick: true },
    { expected: true, stored: true, tick: false },
  ])(
    "sends keep_task_state=$expected with default=$stored, ticked=$tick",
    async ({ expected, stored, tick }) => {
      localStorage.setItem(CLEAR_KEEP_TASK_STATE_KEY, JSON.stringify(stored));
      render(<BulkClearDagRunsButton deselectKeys={vi.fn()} selectedDagRuns={dagRuns} />, {
        wrapper: Wrapper,
      });

      fireEvent.click(screen.getByRole("button", { name: /runAndTaskActions\.clear\.button/iu }));
      const checkbox = await screen.findByRole("checkbox", { name: /keepTaskState/iu });

      expect((checkbox as HTMLInputElement).checked).toBe(stored);
      if (tick) {
        fireEvent.click(checkbox);
      }
      fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));

      await waitFor(() => expect(clearBody).toBeDefined());
      expect(clearBody?.keep_task_state).toBe(expected);
    },
  );
});
