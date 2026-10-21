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
import { render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";

import type { TaskInstanceState, WebhookWaitResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import i18n from "src/i18n/config";
import { server } from "src/mocks/server";
import { BaseWrapper } from "src/utils/Wrapper";

import browseLocale from "../../../public/i18n/locales/en/browse.json";
import commonLocale from "../../../public/i18n/locales/en/common.json";
import { WebhookWait } from "./WebhookWait";

const waitsUrl = "/api/v2/dags/:dagId/dagRuns/:dagRunId/taskInstances/:taskId/webhookWaits";
const deliveryRecordId = "0199f8a0-0000-7000-8000-0000000000aa";

const registeredWait: WebhookWaitResponse = {
  created_at: "2026-10-20T12:00:00Z",
  deadline: "2026-10-23T12:00:00Z",
  ended_at: null,
  events: ["pull_request"],
  id: "0199f8a0-0000-7000-8000-000000000001",
  inbound_delivery_id: null,
  match: {
    action: "closed",
    labels: ["deploy"],
    number: 4242,
    repository: ["my-org/configs", "my-org/web"],
  },
  status: "registered",
  webhook: "github",
};

/** Answer with `waits`, recording the URL of each request. */
const serveWaits = (waits: Array<WebhookWaitResponse>) => {
  const requests: Array<URL> = [];

  server.use(
    http.get(waitsUrl, ({ request }) => {
      requests.push(new URL(request.url));

      return HttpResponse.json({ total_entries: waits.length, webhook_waits: waits });
    }),
  );

  return requests;
};

const renderPanel = (path: string, tryState: TaskInstanceState | null | undefined = "awaiting_input") => {
  const panel = <WebhookWait refetchInterval={false} tryNumber={2} tryState={tryState} />;

  return render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>
        <TimezoneProvider>
          <Routes>
            <Route element={panel} path="dags/:dagId/runs/:runId/tasks/:taskId" />
            <Route element={panel} path="dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex" />
          </Routes>
        </TimezoneProvider>
      </MemoryRouter>
    </BaseWrapper>,
  );
};

const findRow = (panel: HTMLElement, label: string) =>
  within(panel).getByRole("row", { name: new RegExp(`^${i18n.t(label)}`, "u") });

describe("WebhookWait", () => {
  // Without the bundle i18n.t() echoes the key, so the label assertions below would pass blindly.
  beforeEach(() => {
    i18n.addResourceBundle("en", "browse", browseLocale, true, true);
    i18n.addResourceBundle("en", "common", commonLocale, true, true);
  });

  it("asks for the newest wait of the selected try of the task instance", async () => {
    const requests = serveWaits([registeredWait]);

    renderPanel("/dags/merge_gate/runs/run_1/tasks/wait_for_merge/mapped/3?region_id=r-1&region_index=3");

    await screen.findByTestId("webhook-wait");
    expect(requests.at(-1)?.pathname).toBe(
      "/api/v2/dags/merge_gate/dagRuns/run_1/taskInstances/wait_for_merge/webhookWaits",
    );
    expect(Object.fromEntries(requests.at(-1)?.searchParams ?? [])).toEqual({
      limit: "1",
      map_index: "3",
      region_id: "r-1",
      region_index: "3",
      try_number: "2",
    });
  });

  it("shows an open wait's webhook, status, events, match and deadline, without a delivery", async () => {
    serveWaits([registeredWait]);

    renderPanel("/dags/merge_gate/runs/run_1/tasks/wait_for_merge");

    const panel = await screen.findByTestId("webhook-wait");

    expect(
      within(findRow(panel, "browse:webhooks.webhook_one")).getByRole("link", { name: "github" }),
    ).toHaveAttribute("href", "/webhook_deliveries/github");
    expect(
      within(findRow(panel, "common:taskInstance.webhookWait.status")).getByText(
        i18n.t("common:taskInstance.webhookWait.statusExplanations.registered"),
      ),
    ).toBeInTheDocument();
    expect(
      within(panel).getByText(i18n.t("common:taskInstance.webhookWait.statuses.registered")),
    ).toBeInTheDocument();
    expect(
      within(findRow(panel, "common:taskInstance.webhookWait.events")).getByText("pull_request"),
    ).toBeInTheDocument();

    const match = within(panel).getByTestId("webhook-wait-match");

    expect(within(match).getByRole("row", { name: "action closed" })).toBeInTheDocument();
    expect(within(match).getByRole("row", { name: "labels deploy" })).toBeInTheDocument();
    expect(within(match).getByRole("row", { name: "number 4242" })).toBeInTheDocument();
    expect(
      within(match).getByRole("row", { name: "repository One of my-org/configs, my-org/web" }),
    ).toBeInTheDocument();
    expect(
      within(findRow(panel, "common:taskInstance.webhookWait.deadline")).getByTestId("time-display"),
    ).toHaveAttribute("datetime", "2026-10-23T12:00:00Z");
    expect(
      within(panel).queryByText(i18n.t("common:taskInstance.webhookWait.endedAt")),
    ).not.toBeInTheDocument();
    expect(
      within(panel).queryByText(i18n.t("browse:webhookDeliveries.delivery_one")),
    ).not.toBeInTheDocument();
  });

  it("says that a wait without a match accepts any attributes", async () => {
    serveWaits([{ ...registeredWait, match: {} }]);

    renderPanel("/dags/merge_gate/runs/run_1/tasks/wait_for_merge");

    const panel = await screen.findByTestId("webhook-wait");

    expect(
      within(findRow(panel, "common:taskInstance.webhookWait.match")).getByText(
        i18n.t("common:taskInstance.webhookWait.anyMatch"),
      ),
    ).toBeInTheDocument();
    expect(within(panel).queryByTestId("webhook-wait-match")).not.toBeInTheDocument();
  });

  it.each([
    { expected: "tryStopped", status: "registered", tryState: "failed" },
    { expected: "tryStopped", status: "received", tryState: "success" },
    { expected: "tryStopped", status: "registered", tryState: null },
    { expected: "statusExplanations.received", status: "received", tryState: "running" },
    { expected: "statusExplanations.registered", status: "registered", tryState: undefined },
    { expected: "statusExplanations.ended", status: "ended", tryState: "failed" },
  ] as const)(
    "explains a $status wait of a $tryState try with $expected",
    async ({ expected, status, tryState }) => {
      serveWaits([{ ...registeredWait, status }]);

      renderPanel("/dags/merge_gate/runs/run_1/tasks/wait_for_merge", tryState);

      const panel = await screen.findByTestId("webhook-wait");
      const statusRow = findRow(panel, "common:taskInstance.webhookWait.status");

      expect(
        within(statusRow).getByText(i18n.t(`common:taskInstance.webhookWait.statuses.${status}`)),
      ).toBeInTheDocument();
      expect(
        within(statusRow).getByText(i18n.t(`common:taskInstance.webhookWait.${expected}`)),
      ).toBeInTheDocument();
    },
  );

  it("links a resumed wait to the delivery that resumed it, and accepts any event without events", async () => {
    serveWaits([
      {
        ...registeredWait,
        ended_at: "2026-10-20T13:00:00Z",
        events: null,
        inbound_delivery_id: deliveryRecordId,
        status: "resumed",
      },
    ]);

    renderPanel("/dags/merge_gate/runs/run_1/tasks/wait_for_merge");

    const panel = await screen.findByTestId("webhook-wait");

    expect(
      within(findRow(panel, "browse:webhookDeliveries.delivery_one")).getByRole("link", {
        name: i18n.t("common:taskInstance.webhookWait.viewDelivery"),
      }),
    ).toHaveAttribute("href", `/webhook_deliveries/github?id=${deliveryRecordId}`);
    expect(
      within(panel).getByText(i18n.t("common:taskInstance.webhookWait.statuses.resumed")),
    ).toBeInTheDocument();
    expect(
      within(findRow(panel, "common:taskInstance.webhookWait.events")).getByText(
        i18n.t("common:taskInstance.webhookWait.anyEvent"),
      ),
    ).toBeInTheDocument();
    expect(
      within(findRow(panel, "common:taskInstance.webhookWait.endedAt")).getByTestId("time-display"),
    ).toHaveAttribute("datetime", "2026-10-20T13:00:00Z");
  });

  it("shows nothing for a try that registered no wait", async () => {
    const requests = serveWaits([]);

    renderPanel("/dags/merge_gate/runs/run_1/tasks/wait_for_merge");

    await waitFor(() => expect(requests).toHaveLength(1));
    expect(screen.queryByTestId("webhook-wait")).not.toBeInTheDocument();
  });
});
