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
import { beforeAll, describe, expect, it } from "vitest";

import { assetWithDependencies } from "src/mocks/handlers/assets";
import { server } from "src/mocks/server";
import { AppWrapper, preloadAppRoutes } from "src/utils/AppWrapper";

beforeAll(() => preloadAppRoutes(["/assets/1"]));

// The assets mock handler (see src/mocks/handlers/assets.ts) serves asset 1 with one
// alias and one watcher, and asset 2 with neither.
describe("Asset header", () => {
  it("lists the aliases and watchers of an asset", async () => {
    render(<AppWrapper initialEntries={["/assets/1"]} />);

    await waitFor(() => expect(screen.getByRole("button", { name: "1 alias" })).toBeInTheDocument());

    expect(screen.getByRole("button", { name: "1 watcher" })).toBeInTheDocument();
  });

  it("links each watcher to its trigger and shows the trigger's status", async () => {
    render(<AppWrapper initialEntries={["/assets/1"]} />);

    fireEvent.click(await screen.findByRole("button", { name: "1 watcher" }));

    expect(await screen.findByRole("link", { name: "my_watcher" })).toHaveAttribute("href", "/triggers/5");
    expect(screen.getByTestId("trigger-status-badge")).toHaveTextContent("states.running");
  });

  it.each([
    { button: "2 watcherFailing", statuses: ["failing", "orphaned", "failing", "running"] },
    { button: "1 watcherOrphaned", statuses: ["orphaned", "running"] },
  ])("says on the watchers button that $button", async ({ button, statuses }) => {
    const watchers = statuses.map((status, index) => ({
      created_date: "2025-01-01T00:00:00Z",
      name: `watcher_${index}`,
      status,
      trigger_id: index,
    }));

    server.use(
      http.get("/api/v2/assets/:assetId", () => HttpResponse.json({ ...assetWithDependencies, watchers })),
    );
    render(<AppWrapper initialEntries={["/assets/1"]} />);

    fireEvent.click(await screen.findByRole("button", { name: button }));

    expect(await screen.findAllByTestId("trigger-status-badge")).toHaveLength(statuses.length);
  });

  it("omits the aliases and watchers stats for an asset that has none", async () => {
    render(<AppWrapper initialEntries={["/assets/2"]} />);

    await waitFor(() => expect(screen.getAllByText("plain_asset").length).toBeGreaterThan(0));

    expect(screen.queryByRole("button", { name: /alias/iu })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /watcher/iu })).not.toBeInTheDocument();
  });
});
