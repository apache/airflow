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
import { setupServer, type SetupServerApi } from "msw/node";
import { afterEach, describe, it, expect, beforeAll, afterAll } from "vitest";

import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";

let server: SetupServerApi;
const ITEM_HEIGHT = 20;

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledRequest: "bypass" });
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    value: ITEM_HEIGHT,
  });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", {
    value: 800,
  });
});

afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("Task log grouping", () => {
  it("Display task log content on click", async () => {
    render(
      <AppWrapper initialEntries={["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/generate"]} />,
    );
    await waitFor(() => expect(screen.queryByTestId("virtualized-list")).toBeInTheDocument());
    await waitFor(() => expect(screen.queryByTestId("virtualized-item-0")).toBeInTheDocument());
    await waitFor(() => expect(screen.queryByTestId("virtualized-item-10")).toBeInTheDocument());

    fireEvent.scroll(screen.getByTestId("virtualized-list"), { target: { scrollTop: ITEM_HEIGHT * 6 } });
    await waitFor(() => expect(screen.queryByTestId("virtualized-item-16")).toBeInTheDocument());

    await waitFor(() => expect(screen.queryByTestId("summary-Pre task execution logs")).toBeInTheDocument());
    await waitFor(() => expect(screen.getByTestId("summary-Pre task execution logs")).toBeVisible());
    await waitFor(() => expect(screen.queryByText(/Task instance is in running state/iu)).not.toBeVisible());

    await waitFor(() => screen.getByTestId("summary-Pre task execution logs").click());
    await waitFor(() => expect(screen.queryByText(/Task instance is in running state/iu)).toBeVisible());
  }, 10_000);
});
