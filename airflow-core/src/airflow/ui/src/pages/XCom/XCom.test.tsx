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
import { setupServer, type SetupServer } from "msw/node";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import type * as ColorMode from "src/context/colorMode";
import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";

vi.mock("src/components/MonacoEditor", () => ({
  default: ({ value }: { readonly value?: string }) => <div data-testid="monaco-editor">{value}</div>,
}));

vi.mock("src/context/colorMode", async (importOriginal) => ({
  ...(await importOriginal<typeof ColorMode>()),
  useMonacoTheme: () => ({ beforeMount: vi.fn(), theme: "airflow-light" }),
}));

const xcomEntry = {
  dag_display_name: "example_dag",
  dag_id: "example_dag",
  key: "return_value",
  map_index: -1,
  run_after: "2025-01-01T00:00:00Z",
  run_id: "manual_run",
  task_display_name: "push",
  task_id: "push",
  timestamp: "2025-01-01T00:00:00Z",
};

let server: SetupServer;

beforeAll(() => {
  server = setupServer(
    ...handlers,
    http.get("/api/v2/dags/~/dagRuns/~/taskInstances/~/xcomEntries", () =>
      HttpResponse.json({ total_entries: 1, xcom_entries: [xcomEntry] }),
    ),
    http.get("/api/v2/dags/:dagId/dagRuns/:runId/taskInstances/:taskId/xcomEntries/:key", () =>
      HttpResponse.json({ ...xcomEntry, value: { nested: { answer: 42 } } }),
    ),
  );
  server.listen({ onUnhandledFrame: "bypass" });
});
afterAll(() => server.close());

describe("XCom list expand/collapse", () => {
  it("expands and collapses XCom values via the expand/collapse all buttons", async () => {
    render(<AppWrapper initialEntries={["/xcoms"]} />);

    await waitFor(() => expect(screen.getByTestId("expand-all-button")).toBeInTheDocument());
    await waitFor(() => expect(screen.getByText(/nested/u)).toBeInTheDocument());
    expect(screen.queryByTestId("monaco-editor")).toBeNull();

    fireEvent.click(screen.getByTestId("expand-all-button"));
    expect(await screen.findByTestId("monaco-editor")).toHaveTextContent(/"answer": 42/u);

    fireEvent.click(screen.getByTestId("collapse-all-button"));
    await waitFor(() => expect(screen.queryByTestId("monaco-editor")).toBeNull());
  });
});
