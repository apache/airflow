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
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { XcomService } from "openapi/requests";

import { Wrapper } from "src/utils/Wrapper";

import { XComEntry } from "./XComEntry";

const server = setupServer();
const entryUrl = "/api/v2/dags/test_dag/dagRuns/test_run/taskInstances/test_task/xcomEntries/:xcomKey";
const entryProps = { dagId: "test_dag", mapIndex: -1, runId: "test_run", taskId: "test_task" };

// i18n resources are not loaded in unit tests, so translated text renders as its key.
const errorTitle = "error.title";

beforeAll(() => server.listen({ onUnhandledFrame: "bypass" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());
afterEach(() => vi.restoreAllMocks());

const renderValue = (value: string) => {
  server.use(http.get(entryUrl, () => HttpResponse.json({ key: "return_value", value })));

  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

  render(
    <QueryClientProvider client={queryClient}>
      <XComEntry {...entryProps} xcomKey="return_value" />
    </QueryClientProvider>,
    { wrapper: Wrapper },
  );
};

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

    const trigger = await screen.findByRole("button", { name: `${errorTitle} ${String(status)}` });

    expect(trigger).toHaveTextContent(String(status));
    expect(trigger).not.toHaveTextContent(detail);
    expect(await screen.findByText("Another XCom value")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: /copy/iu })).toHaveLength(1);
    expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument();

    fireEvent.click(trigger);

    const dialog = await screen.findByRole("dialog", { name: errorTitle });
    const alert = within(dialog).getByTestId("error-alert");

    expect(alert).toHaveTextContent(String(status));
    expect(alert).toHaveTextContent(detail);
  });

  it("closes the full error details from the modal close button", async () => {
    server.use(
      http.get(entryUrl, () => HttpResponse.json({ detail: "XCom entry was deleted" }, { status: 404 })),
    );

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <QueryClientProvider client={queryClient}>
        <XComEntry {...entryProps} xcomKey="failed_entry" />
      </QueryClientProvider>,
      { wrapper: Wrapper },
    );

    fireEvent.click(await screen.findByRole("button", { name: `${errorTitle} 404` }));

    const dialog = await screen.findByRole("dialog", { name: errorTitle });

    fireEvent.click(within(dialog).getByRole("button", { name: "Close" }));

    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.getByRole("button", { name: `${errorTitle} 404` })).toBeInTheDocument();
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

    expect(await screen.findByTestId("xcom-entry-error")).toHaveTextContent("404");
    expect(screen.queryByText("Stale value")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /copy/iu })).not.toBeInTheDocument();
  });

  it("keeps new error details closed after a successful refetch", async () => {
    server.use(
      http.get(entryUrl, () => HttpResponse.json({ detail: "XCom entry was deleted" }, { status: 404 })),
    );

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <QueryClientProvider client={queryClient}>
        <XComEntry {...entryProps} xcomKey="return_value" />
      </QueryClientProvider>,
      { wrapper: Wrapper },
    );

    fireEvent.click(await screen.findByRole("button", { name: `${errorTitle} 404` }));
    await screen.findByRole("dialog", { name: errorTitle });

    server.use(
      http.get(entryUrl, () => HttpResponse.json({ key: "return_value", value: "Recovered value" })),
    );
    await act(() => queryClient.invalidateQueries());
    await screen.findByText("Recovered value");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

    server.use(
      http.get(entryUrl, () =>
        HttpResponse.json({ detail: "Unable to deserialize the XCom value" }, { status: 500 }),
      ),
    );
    await act(() => queryClient.invalidateQueries());

    expect(await screen.findByRole("button", { name: `${errorTitle} 500` })).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("exposes the full validation error for structured details", async () => {
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

    fireEvent.click(await screen.findByRole("button", { name: `${errorTitle} 422` }));

    const dialog = await screen.findByRole("dialog", { name: errorTitle });

    expect(within(dialog).getByTestId("error-alert")).toHaveTextContent("query.map_index Invalid map index");
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

    const trigger = await screen.findByRole("button", { name: errorTitle });

    expect(trigger).toHaveTextContent(errorTitle);
    expect(trigger).not.toHaveTextContent("undefined");
    expect(screen.queryByRole("button", { name: /copy/iu })).not.toBeInTheDocument();

    fireEvent.click(trigger);

    const dialog = await screen.findByRole("dialog", { name: errorTitle });

    expect(within(dialog).getByTestId("error-alert")).toHaveTextContent(/network error/iu);
  });

  it("keeps newlines in string values", async () => {
    renderValue("a\nb");

    expect((await screen.findByTestId("xcom-value")).textContent).toContain("a\nb");
  });

  it("keeps newlines and still renders links", async () => {
    renderValue("Line 1\nSee https://airflow.apache.org\nLine 3");

    expect(await screen.findByRole("link")).toHaveAttribute("href", "https://airflow.apache.org");
    expect(screen.getByTestId("xcom-value").textContent).toContain("Line 1\nSee ");
  });
});

it("fetches a retained value by its own region even when the public map index collides", async () => {
  const coordinates = {
    dag_id: "dag",
    map_index: -1,
    region_id: "11111111-1111-1111-1111-111111111111",
    region_index: 3,
    run_id: "run",
    task_id: "member",
  };
  const fetch = vi.spyOn(XcomService, "getXcomEntry").mockResolvedValue({
    ...coordinates,
    dag_display_name: "dag",
    key: "return_value",
    logical_date: null,
    run_after: "2026-01-01T00:00:00Z",
    task_display_name: "member",
    timestamp: "2026-01-01T00:00:00Z",
    value: "third pass",
  });

  render(
    <XComEntry
      dagId="dag"
      mapIndex={-1}
      regionId={coordinates.region_id}
      regionIndex={3}
      runId="run"
      taskId="member"
      xcomKey="return_value"
    />,
    { wrapper: Wrapper },
  );

  expect(await screen.findByText("third pass")).toBeVisible();
  await waitFor(() =>
    expect(fetch).toHaveBeenCalledWith(
      expect.objectContaining({ mapIndex: -1, regionId: coordinates.region_id, regionIndex: 3 }),
    ),
  );
});
