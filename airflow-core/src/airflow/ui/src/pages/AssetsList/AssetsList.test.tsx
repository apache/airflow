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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { delay, http, HttpResponse } from "msw";
import { setupServer, type SetupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";

let server: SetupServer;

beforeAll(() => {
  server = setupServer(...handlers);
  server.listen({ onUnhandledRequest: "bypass" });
});

afterEach(() => server.resetHandlers());
afterAll(() => server.close());

// The assets mock handler (see src/mocks/handlers/assets.ts) returns a single asset
// with one consuming task, one alias and one watcher.
describe("AssetsList columns", () => {
  it("shows the consuming tasks of an asset", async () => {
    render(<AppWrapper initialEntries={["/assets"]} />);

    await waitFor(() => expect(screen.getByText("asset_with_dependencies")).toBeInTheDocument());

    expect(screen.getByRole("button", { name: "1 task" })).toBeInTheDocument();
  });

  it("lists the aliases and watchers of an asset", async () => {
    render(<AppWrapper initialEntries={["/assets"]} />);

    await waitFor(() => expect(screen.getByRole("button", { name: "1 alias" })).toBeInTheDocument());

    expect(screen.getByRole("button", { name: "1 watcher" })).toBeInTheDocument();
  });
});

describe("AssetsList filtering", () => {
  it("keeps the listed assets on screen while a filter change is still loading", async () => {
    render(<AppWrapper initialEntries={["/assets"]} />);

    await waitFor(() => expect(screen.getByText("asset_with_dependencies")).toBeInTheDocument());

    server.use(
      http.get("/ui/assets", async () => {
        await delay("infinite");

        return HttpResponse.json({ assets: [], total_entries: 0 });
      }),
    );

    fireEvent.change(screen.getByTestId("search-dags"), { target: { value: "plain" } });

    await waitFor(() => expect(screen.getByRole("progressbar")).toBeVisible());

    expect(screen.getByText("asset_with_dependencies")).toBeInTheDocument();
    expect(screen.queryAllByTestId("skeleton")).toHaveLength(0);
  });

  it("offers Dag ID as an exact-match filter", async () => {
    render(<AppWrapper initialEntries={["/assets"]} />);

    fireEvent.click(await screen.findByTestId("add-filter-button"));
    fireEvent.click(await screen.findByTestId("add-filter-dag_id"));

    expect(await screen.findByTestId("filter-pill-input")).toBeInTheDocument();
    // The page search keeps its own advanced-search toggle. An exact-match Dag ID
    // filter must not add a second toggle inside its editor.
    expect(screen.getAllByTestId("advanced-search-toggle")).toHaveLength(1);
  });

  it("passes the selected Dag ID to the Assets API and omits it after clearing", async () => {
    const requestedDagIds: Array<Array<string>> = [];

    server.use(
      http.get("/ui/assets", ({ request }) => {
        requestedDagIds.push(new URL(request.url).searchParams.getAll("dag_ids"));

        return HttpResponse.json({ assets: [], total_entries: 0 });
      }),
    );

    render(<AppWrapper initialEntries={["/assets?dag_id=consumer_dag"]} />);

    await waitFor(() => expect(requestedDagIds.at(-1)).toEqual(["consumer_dag"]));

    const dagIdPill = await screen.findByTestId("dag_id-pill");

    fireEvent.click(within(dagIdPill).getByRole("button", { name: /Remove .* filter/u }));

    await waitFor(() => expect(requestedDagIds.at(-1)).toEqual([]));
    expect(screen.queryByTestId("dag_id-pill")).not.toBeInTheDocument();
  });

  it("ignores an empty Dag ID query parameter", async () => {
    const requestedDagIds: Array<Array<string>> = [];

    server.use(
      http.get("/ui/assets", ({ request }) => {
        requestedDagIds.push(new URL(request.url).searchParams.getAll("dag_ids"));

        return HttpResponse.json({ assets: [], total_entries: 0 });
      }),
    );

    render(<AppWrapper initialEntries={["/assets?dag_id="]} />);

    await waitFor(() => expect(requestedDagIds.at(-1)).toEqual([]));
    expect(screen.queryByTestId("dag_id-pill")).not.toBeInTheDocument();
  });
});
