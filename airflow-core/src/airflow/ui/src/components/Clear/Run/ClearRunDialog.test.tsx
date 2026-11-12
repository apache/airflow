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

import ClearRunDialog from "./ClearRunDialog";

const DAG_ID = "test_dag";
const DAG_RUN_ID = "run_1";

const dagRun = {
  bundle_version: null,
  dag_id: DAG_ID,
  dag_run_id: DAG_RUN_ID,
  dag_versions: [],
  note: null,
} as unknown as DAGRunResponse;

vi.mock("src/queries/useClearDagRunDryRun", () => ({
  useClearDagRunDryRun: () => ({ data: { task_instances: [{ task_id: "t" }], total_entries: 1 } }),
  useClearDagRunDryRunKey: "clearDagRunDryRun",
}));

let clearBody: Record<string, unknown> | undefined;

beforeEach(() => {
  server.use(
    http.get(`/api/v2/dags/${DAG_ID}/details`, () => HttpResponse.json({})),
    http.post(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}/clear`, async ({ request }) => {
      clearBody = (await request.json()) as Record<string, unknown>;

      return HttpResponse.json({ dag_id: DAG_ID, dag_run_id: DAG_RUN_ID });
    }),
  );
});
afterEach(() => {
  clearBody = undefined;
  localStorage.clear();
});

describe("ClearRunDialog", () => {
  it.each([
    { expected: undefined, stored: false, tick: false },
    { expected: true, stored: false, tick: true },
    { expected: true, stored: true, tick: false },
  ])(
    "sends keep_task_state=$expected with default=$stored, ticked=$tick",
    async ({ expected, stored, tick }) => {
      localStorage.setItem(CLEAR_KEEP_TASK_STATE_KEY, JSON.stringify(stored));
      render(<ClearRunDialog dagRun={dagRun} onClose={vi.fn()} open />, { wrapper: Wrapper });

      const checkbox = screen.getByRole("checkbox", { name: /keepTaskState/iu });

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
