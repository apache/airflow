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
import "@testing-library/jest-dom";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import type { DAGRunResponse } from "openapi/requests/types.gen";

import { CLEAR_KEEP_TASK_STATE_KEY } from "src/constants/localStorage";
import { handlers } from "src/mocks/handlers";
import { Wrapper } from "src/utils/Wrapper";

import ClearRunDialog from "./ClearRunDialog";

const dagRun: DAGRunResponse = {
  bundle_version: null,
  conf: {},
  dag_display_name: "test_dag",
  dag_id: "test_dag",
  dag_run_id: "run_1",
  dag_versions: [],
  data_interval_end: null,
  data_interval_start: null,
  duration: null,
  end_date: null,
  last_scheduling_decision: null,
  logical_date: null,
  note: null,
  partition_date: null,
  partition_key: null,
  queued_at: null,
  run_after: "2025-01-01T00:00:00Z",
  run_type: "manual",
  start_date: null,
  state: "success",
  triggered_by: null,
  triggering_user_name: null,
};

const requests = vi.fn();
const server = setupServer(
  ...handlers,
  http.get("/api/v2/dags/test_dag/details", () => HttpResponse.json({})),
  http.post("/api/v2/dags/test_dag/dagRuns/run_1/clear", async ({ request }) => {
    const body = (await request.json()) as { dry_run: boolean; keep_task_state?: boolean };

    if (body.dry_run) {
      return HttpResponse.json({ task_instances: [], total_entries: 1 });
    }
    requests(body);

    return HttpResponse.json(dagRun);
  }),
);

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => {
  server.resetHandlers();
  requests.mockClear();
  localStorage.clear();
});
afterAll(() => server.close());

describe("ClearRunDialog", () => {
  it.each([
    { expected: false, storedDefault: false, toggle: false },
    { expected: true, storedDefault: false, toggle: true },
    { expected: true, storedDefault: true, toggle: false },
    { expected: false, storedDefault: true, toggle: true },
  ])(
    "sends keep_task_state=$expected with saved default=$storedDefault and toggle=$toggle",
    async ({ expected, storedDefault, toggle }) => {
      if (storedDefault) {
        localStorage.setItem(CLEAR_KEEP_TASK_STATE_KEY, JSON.stringify(true));
      }
      render(<ClearRunDialog dagRun={dagRun} onClose={vi.fn()} open />, { wrapper: Wrapper });

      const checkbox = await screen.findByRole("checkbox", { name: /keepTaskState/iu });

      expect(checkbox).toHaveProperty("checked", storedDefault);
      if (toggle) {
        fireEvent.click(checkbox);
      }
      const confirm = screen.getByRole("button", { name: /modal\.confirm/iu });

      await waitFor(() => expect(confirm).toBeEnabled());
      fireEvent.click(confirm);
      await waitFor(() =>
        expect(requests).toHaveBeenCalledWith(
          expect.objectContaining({
            dry_run: false,
            keep_task_state: expected,
          }),
        ),
      );
    },
  );
});
