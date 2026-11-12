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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";

import type { TriggerDetailResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import dagLocale from "../../public/i18n/locales/en/dag.json";
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
    first_failed_at: "2026-10-01T00:01:00Z",
    id,
    last_error: `QueueDoesNotExist: queue_${id}`,
    last_failed_at: "2026-10-01T00:04:00Z",
    status: "failing",
  }) as unknown as TriggerDetailResponse;

const PAGE_SIZE = 2;

/**
 * Serve the Dag's details, a page size, and the triggers a page at a time, the later pages once `nextPages`
 * resolves; record each request for triggers, after a "details" for the Dag's.
 */
const serve = (
  triggers: Array<TriggerDetailResponse>,
  assetExpression: unknown = { any: [] },
  nextPages?: Promise<void>,
) => {
  const requested: Array<string> = [];

  server.use(
    http.get("/ui/config", () => HttpResponse.json({ fallback_page_limit: PAGE_SIZE })),
    http.get("/api/v2/dags/:dagId/details", ({ params }) => {
      requested.push("details");

      return HttpResponse.json({ asset_expression: assetExpression, dag_id: params.dagId });
    }),
    http.get("/api/v2/triggers", async ({ request }) => {
      const { search, searchParams } = new URL(request.url);
      const offset = Number(searchParams.get("offset") ?? 0);

      requested.push(search);
      if (offset > 0) {
        await nextPages;
      }

      return HttpResponse.json({
        total_entries: triggers.length,
        triggers: triggers.slice(offset, offset + Number(searchParams.get("limit"))),
      });
    }),
  );

  return requested;
};

const renderChip = () => render(<DagFailingWatchersChip dagId="my_dag" />, { wrapper: Wrapper });

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("DagFailingWatchersChip", () => {
  it("opens a modal with a failing watcher's last error and a link to its trigger", async () => {
    // Its trigger also runs a watcher on an asset the Dag only produces, which does not hold the Dag up.
    const requested = serve([trigger(7, ["my_dag"], ["other_dag"])]);

    renderChip();
    fireEvent.click(await screen.findByRole("button", { name: "failingWatcherTriggers" }));

    const modal = await screen.findByRole("dialog");

    expect(await within(modal).findByText("QueueDoesNotExist: queue_7")).toBeInTheDocument();
    expect(within(modal).getAllByText("dagWarnings.failingWatcher")).toHaveLength(1);
    const params = requested.slice(1).map((search) => Object.fromEntries(new URLSearchParams(search)));
    const failing = { kind: "asset_watcher", scheduled_dag_id: "my_dag", status: "failing" };

    // The chip asks only for the count; the modal for its first page, sized by the config.
    expect(params).toEqual([
      { ...failing, limit: "0" },
      { ...failing, limit: String(PAGE_SIZE), offset: "0" },
    ]);

    expect(within(modal).getByRole("link", { name: "dagWarnings.viewTrigger" })).toHaveAttribute(
      "href",
      "/triggers/7",
    );
  });

  it("pages through more failing watchers than a page holds, keeping a page until the next loads", async () => {
    let showNextPage: () => void = () => undefined;
    const nextPage = new Promise<void>((resolve) => {
      showNextPage = resolve;
    });
    const triggers = [trigger(1, ["my_dag"]), trigger(2, ["my_dag"]), trigger(3, ["my_dag"])];
    const requested = serve(triggers, undefined, nextPage);

    renderChip();
    fireEvent.click(await screen.findByRole("button", { name: "failingWatcherTriggers" }));

    const modal = await screen.findByRole("dialog");

    expect(await within(modal).findByText("QueueDoesNotExist: queue_2")).toBeInTheDocument();
    expect(within(modal).queryByText("QueueDoesNotExist: queue_3")).not.toBeInTheDocument();

    fireEvent.click(within(modal).getByTestId("next"));

    await waitFor(() => expect(new URLSearchParams(requested.at(-1)).get("offset")).toBe(String(PAGE_SIZE)));
    expect(within(modal).getByText("QueueDoesNotExist: queue_2")).toBeInTheDocument();
    showNextPage();
    expect(await within(modal).findByText("QueueDoesNotExist: queue_3")).toBeInTheDocument();
    expect(within(modal).queryByText("QueueDoesNotExist: queue_1")).not.toBeInTheDocument();
  });

  it("counts a trigger shared by two of the Dag's watchers once, as a trigger", async () => {
    i18n.addResourceBundle("en", "dag", dagLocale, true, true);
    serve([trigger(7, ["my_dag"], ["my_dag"])]);

    try {
      renderChip();
      fireEvent.click(await screen.findByRole("button", { name: "1 failing watcher trigger" }));

      const modal = await screen.findByRole("dialog");

      expect(
        await within(modal).findByRole("heading", { name: "1 failing watcher trigger" }),
      ).toBeInTheDocument();
      expect(await within(modal).findAllByText("dagWarnings.failingWatcher")).toHaveLength(2);
    } finally {
      i18n.removeResourceBundle("en", "dag");
    }
  });

  it("shows nothing while no failing watcher runs the Dag", async () => {
    const requested = serve([]);

    renderChip();

    await waitFor(() => expect(requested).toHaveLength(2));
    expect(screen.queryByRole("button", { name: "failingWatcherTriggers" })).not.toBeInTheDocument();
  });

  it("does not look for failing watchers for a Dag not scheduled on assets", async () => {
    const requested = serve([trigger(7, ["my_dag"])], null);

    renderChip();

    await waitFor(() => expect(requested).toEqual(["details"]));
    // Give a request for triggers the time to go out, if one were made.
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(requested).toEqual(["details"]);
    expect(screen.queryByRole("button", { name: "failingWatcherTriggers" })).not.toBeInTheDocument();
  });
});
