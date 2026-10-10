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
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeAll, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { TimezoneProvider } from "src/context/timezone";
import i18n from "src/i18n/config";
import { BaseWrapper } from "src/utils/Wrapper";

import { CallbackLogs } from "./CallbackLogs";

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return { ...actual, useDeadlinesServiceGetCallbackLogs: vi.fn() };
});

vi.mock("src/queries/useConfig", () => ({ useConfig: () => undefined }));

const { useDeadlinesServiceGetCallbackLogs } = await import("openapi/queries");

const CALLBACK_ID = "0199a8e0-0000-7000-8000-000000000001";

beforeAll(() => {
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", { value: 20 });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", { value: 800 });
});

describe("CallbackLogs", () => {
  it("renders the callback's logs with the task log viewer", async () => {
    vi.mocked(useDeadlinesServiceGetCallbackLogs).mockReturnValue({
      data: {
        content: [
          { event: "Deadline miss callback started", level: "info", timestamp: "2026-10-01T00:00:06Z" },
          { event: "Deadline miss callback finished", level: "info", timestamp: "2026-10-01T00:00:07Z" },
        ],
      },
      error: null,
      isLoading: false,
    } as unknown as ReturnType<typeof useDeadlinesServiceGetCallbackLogs>);

    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={[`/dags/my_dag/runs/run_1/callbacks/${CALLBACK_ID}/logs`]}>
          <TimezoneProvider>
            <Routes>
              <Route element={<CallbackLogs />} path="/dags/:dagId/runs/:runId/callbacks/:callbackId/logs" />
            </Routes>
          </TimezoneProvider>
        </MemoryRouter>
      </BaseWrapper>,
    );

    expect(vi.mocked(useDeadlinesServiceGetCallbackLogs).mock.lastCall?.[0]).toEqual({
      accept: "application/x-ndjson",
      callbackId: CALLBACK_ID,
      dagId: "my_dag",
      dagRunId: "run_1",
    });
    await waitFor(() => expect(screen.getByText(/Deadline miss callback started/u)).toBeInTheDocument());
    expect(screen.getByText(/Deadline miss callback finished/u)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: i18n.t("dag:callbacks.allCallbacks") })).toHaveAttribute(
      "href",
      "/dags/my_dag/runs/run_1/callbacks",
    );
  });
});
