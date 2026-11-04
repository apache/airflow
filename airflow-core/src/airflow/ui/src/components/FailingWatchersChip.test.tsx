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
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type { TriggerDetailResponse } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";
import { BaseWrapper } from "src/utils/Wrapper";

import { DagFailingWatchersChip } from "./FailingWatchersChip";

// A failing watcher's trigger, with one watcher per list of the Dags scheduled on its asset.
const trigger = (id: number, ...scheduledDagIds: Array<Array<string>>) =>
  ({
    asset_watchers: scheduledDagIds.map((dagIds, index) => ({
      asset_id: index,
      asset_name: `asset_${index}`,
      asset_uri: `s3://bucket/asset_${index}`,
      scheduled_dag_ids: dagIds,
      watcher_name: `watcher_${id}_${index}`,
    })),
    id,
    status: "failing",
  }) as unknown as TriggerDetailResponse;

/** Serve the Dag's details and the triggers; record each request for triggers, after a "details" for the Dag's. */
const serve = (triggers: Array<TriggerDetailResponse>, assetExpression: unknown = { any: [] }) => {
  const requested: Array<string> = [];

  server.use(
    http.get("/api/v2/dags/:dagId/details", ({ params }) => {
      requested.push("details");

      return HttpResponse.json({ asset_expression: assetExpression, dag_id: params.dagId });
    }),
    http.get("/api/v2/triggers", ({ request }) => {
      requested.push(new URL(request.url).search);

      return HttpResponse.json({ total_entries: triggers.length, triggers });
    }),
  );

  return requested;
};

const Location = () => {
  const { pathname, search } = useLocation();

  return <div data-testid="location">{`${pathname}${search}`}</div>;
};

const renderChip = () =>
  render(
    <BaseWrapper>
      <MemoryRouter>
        <Routes>
          <Route element={<DagFailingWatchersChip dagId="my_dag" />} path="/" />
          <Route element={<Location />} path="/triggers/*" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("DagFailingWatchersChip", () => {
  it("opens the trigger of a failing watcher on an asset the Dag is scheduled on", async () => {
    const requested = serve([
      trigger(7, ["my_dag"]),
      // A watcher on an asset the Dag only produces: it does not hold the Dag up.
      trigger(9, ["other_dag"]),
    ]);

    renderChip();
    fireEvent.click(await screen.findByRole("button", { name: "failingWatchers" }));

    expect(await screen.findByTestId("location")).toHaveTextContent("/triggers/7");
    expect(requested).toHaveLength(2);
    expect(Object.fromEntries(new URLSearchParams(requested[1]))).toEqual({
      kind: "asset_watcher",
      limit: "100",
      scheduled_dag_id: "my_dag",
      status: "failing",
    });
  });

  it("opens the failing watchers in the Triggers list when there are several", async () => {
    serve([trigger(7, ["my_dag"]), trigger(9, ["my_dag", "other_dag"])]);

    renderChip();
    fireEvent.click(await screen.findByRole("button", { name: "failingWatchers" }));

    expect(await screen.findByTestId("location")).toHaveTextContent(
      "/triggers?kind=asset_watcher&status=failing",
    );
  });

  it("shows nothing while no failing watcher runs the Dag", async () => {
    const requested = serve([trigger(9, ["other_dag"], [])]);

    renderChip();

    await waitFor(() => expect(requested).toHaveLength(2));
    expect(screen.queryByRole("button", { name: "failingWatchers" })).not.toBeInTheDocument();
  });

  it("does not look for failing watchers for a Dag not scheduled on assets", async () => {
    const requested = serve([trigger(7, ["my_dag"])], null);

    renderChip();

    await waitFor(() => expect(requested).toEqual(["details"]));
    // Give a request for triggers the time to go out, if one were made.
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(requested).toEqual(["details"]);
    expect(screen.queryByRole("button", { name: "failingWatchers" })).not.toBeInTheDocument();
  });
});
