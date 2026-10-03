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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { XComEntry } from "./XComEntry";

const server = setupServer();
const entryUrl = "/api/v2/dags/test_dag/dagRuns/test_run/taskInstances/test_task/xcomEntries/:xcomKey";
const entryProps = { dagId: "test_dag", mapIndex: -1, runId: "test_run", taskId: "test_task" };

beforeAll(() => server.listen({ onUnhandledRequest: "bypass" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("XComEntry", () => {
  it.each([
    { detail: "XCom entry was deleted", status: 404 },
    { detail: "Access to this XCom entry is denied", status: 403 },
    { detail: "Unable to deserialize the XCom value", status: 500 },
  ])("shows a $status error without removing another entry", async ({ detail, status }) => {
    server.use(
      http.get(entryUrl, ({ params }) =>
        params.xcomKey === "failed_entry"
          ? HttpResponse.json({ detail }, { status })
          : HttpResponse.json({ key: "healthy_entry", value: "Another XCom value" }),
      ),
    );

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <QueryClientProvider client={queryClient}>
        <XComEntry {...entryProps} xcomKey="failed_entry" />
        <XComEntry {...entryProps} xcomKey="healthy_entry" />
      </QueryClientProvider>,
      { wrapper: Wrapper },
    );

    const trigger = await screen.findByRole("button", { name: `${String(status)} ${detail}` });

    expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument();
    fireEvent.click(trigger);

    const alert = await screen.findByTestId("error-alert");

    expect(alert).toHaveTextContent(String(status));
    expect(alert).toHaveTextContent(detail);
    expect(await screen.findByText("Another XCom value")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: /copy/iu })).toHaveLength(1);
  });

  it("shows a refetch error instead of allowing a stale value to be copied", async () => {
    server.use(http.get(entryUrl, () => HttpResponse.json({ key: "return_value", value: "Stale value" })));

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <QueryClientProvider client={queryClient}>
        <XComEntry {...entryProps} xcomKey="return_value" />
      </QueryClientProvider>,
      { wrapper: Wrapper },
    );

    await screen.findByText("Stale value");

    server.use(
      http.get(entryUrl, () => HttpResponse.json({ detail: "XCom entry was deleted" }, { status: 404 })),
    );

    await act(() => queryClient.invalidateQueries());

    expect(await screen.findByTestId("xcom-entry-error")).toHaveTextContent("XCom entry was deleted");
    expect(screen.queryByText("Stale value")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /copy/iu })).not.toBeInTheDocument();
  });

  it("uses the error message for structured details and exposes the full validation error", async () => {
    server.use(
      http.get(entryUrl, () =>
        HttpResponse.json(
          { detail: [{ loc: ["query", "map_index"], msg: "Invalid map index", type: "value_error" }] },
          { status: 422 },
        ),
      ),
    );

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <QueryClientProvider client={queryClient}>
        <XComEntry {...entryProps} xcomKey="failed_entry" />
      </QueryClientProvider>,
      { wrapper: Wrapper },
    );

    fireEvent.click(await screen.findByRole("button", { name: "422 Validation Error" }));

    expect(await screen.findByTestId("error-alert")).toHaveTextContent("query.map_index Invalid map index");
  });

  it("shows a network error without an HTTP status or copy button", async () => {
    server.use(http.get(entryUrl, () => HttpResponse.error()));

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <QueryClientProvider client={queryClient}>
        <XComEntry {...entryProps} xcomKey="failed_entry" />
      </QueryClientProvider>,
      { wrapper: Wrapper },
    );

    const trigger = await screen.findByTestId("xcom-entry-error");

    expect(trigger).toHaveTextContent("Network Error");
    expect(trigger).not.toHaveTextContent("undefined");
    expect(screen.queryByRole("button", { name: /copy/iu })).not.toBeInTheDocument();
  });
});
