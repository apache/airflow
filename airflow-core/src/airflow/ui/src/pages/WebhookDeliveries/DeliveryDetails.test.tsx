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
import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { WebhookCancelRequestResponse, WebhookDeliveryResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { DeliveryDetails } from "./DeliveryDetails";

vi.mock("src/components/MonacoEditor", () => ({
  default: ({ value }: { readonly value?: string }) => <div data-testid="monaco-editor">{value}</div>,
}));

const wait = {
  dag_id: "deploy",
  map_index: -1,
  run_id: "manual__2026-10-20",
  task_id: "wait_for_merge",
  ti_id: "0199f8a0-0000-7000-8000-0000000000b1",
  wait_id: "0199f8a0-0000-7000-8000-0000000000c1",
};

const buildDelivery = (
  actions: WebhookDeliveryResponse["actions"],
  cancelRequests?: Array<WebhookCancelRequestResponse>,
): WebhookDeliveryResponse => ({
  actions,
  attributes: { action: "closed", event: "pull_request", number: 4242 },
  cancel_requests: cancelRequests,
  candidates: null,
  correlation_key: "my-org/configs#4242",
  delivery_id: "gh-1",
  id: "0199f8a0-0000-7000-8000-000000000001",
  outcome: "resumed_wait",
  received_at: "2026-10-20T12:00:00Z",
  source_type: "github",
  team_name: null,
  webhook: "github",
});

const runRequestId = "0199f8a0-0000-7000-8000-0000000000e1";
const taskRequestId = "0199f8a0-0000-7000-8000-0000000000e2";
const regionId = "0199f8a0-0000-7000-8000-0000000000d2";

const recordedRunRequest = { dag_id: "preview_environment", request_id: runRequestId, scope: "run" };
const recordedTaskRequest = { dag_id: "ship_when_labeled", request_id: taskRequestId, scope: "task" };

const runRequest: WebhookCancelRequestResponse = {
  applied_at: "2026-10-20T12:00:01Z",
  dag_id: "preview_environment",
  id: runRequestId,
  result: {
    dag_runs: [{ run_id: "asset_triggered__2026-10-20", state: "failed" }],
    error: null,
    skipped_runs: [],
    task_instances: [],
  },
  status: "applied",
  targets: { run: { correlation_key_confs: ["pr"] }, tasks: [] },
};

const taskRequest: WebhookCancelRequestResponse = {
  applied_at: "2026-10-20T12:00:01Z",
  dag_id: "ship_when_labeled",
  id: taskRequestId,
  result: {
    dag_runs: [],
    error: null,
    skipped_runs: [{ reason: "dag_has_team", run_id: "manual_2" }],
    task_instances: [
      { map_index: -1, run_id: "manual_1", state: "skipped", task_id: "run_e2e_tests" },
      {
        map_index: 2,
        region_id: regionId,
        region_index: 2,
        run_id: "manual_1",
        state: "skipped",
        task_id: "run_e2e_tests",
      },
    ],
  },
  status: "applied",
  targets: {
    run: null,
    tasks: [
      { cancel_state: "failed", correlation_key_confs: [], task_id: "deploy" },
      { cancel_state: "skipped", correlation_key_confs: [], task_id: "run_e2e_tests" },
    ],
  },
};

describe("DeliveryDetails", () => {
  it("links each webhook wait to the task instance that registered it", () => {
    const mappedWait = {
      ...wait,
      map_index: 2,
      region_id: "0199f8a0-0000-7000-8000-0000000000d2",
      region_index: 2,
      ti_id: "0199f8a0-0000-7000-8000-0000000000b2",
      wait_id: "0199f8a0-0000-7000-8000-0000000000c2",
    };
    const loopWait = {
      ...wait,
      region_id: "0199f8a0-0000-7000-8000-0000000000d3",
      region_index: 2,
      ti_id: "0199f8a0-0000-7000-8000-0000000000b3",
      wait_id: "0199f8a0-0000-7000-8000-0000000000c3",
    };

    render(
      <DeliveryDetails
        delivery={buildDelivery({
          asset_events: [{ asset: "github/my-org/configs/pull_request", asset_id: 3, event_id: 7 }],
          waits: [wait, mappedWait, loopWait],
        })}
      />,
      { wrapper: Wrapper },
    );

    const waits = screen.getByTestId("delivery-waits");
    const [unmapped, mapped] = within(waits).getAllByRole("listitem") as [
      HTMLElement,
      HTMLElement,
      HTMLElement,
    ];

    expect(
      within(waits)
        .getAllByRole("link")
        .map((link) => [link.textContent, link.getAttribute("href")]),
    ).toEqual([
      ["wait_for_merge", "/dags/deploy/runs/manual__2026-10-20/tasks/wait_for_merge"],
      [
        "wait_for_merge [2]",
        `/dags/deploy/runs/manual__2026-10-20/tasks/wait_for_merge/mapped/2?region_id=${mappedWait.region_id}&region_index=2`,
      ],
      [
        "wait_for_merge",
        `/dags/deploy/runs/manual__2026-10-20/tasks/wait_for_merge?region_id=${loopWait.region_id}&region_index=2`,
      ],
    ]);
    expect(within(unmapped).getByText("deploy")).toBeInTheDocument();
    expect(within(unmapped).getByText("manual__2026-10-20")).toBeInTheDocument();
    expect(within(unmapped).getByText(wait.wait_id)).toBeInTheDocument();
    expect(within(mapped).getByText(mappedWait.wait_id)).toBeInTheDocument();
    expect(screen.getByTestId("delivery-asset-events")).toBeInTheDocument();
    expect(screen.getByText("webhookDeliveries.outcomeExplanations.resumed_wait")).toBeInTheDocument();
    expect(screen.queryByTestId("monaco-editor")).not.toBeInTheDocument();
  });

  it.each([
    { recorded: { ...wait, map_index: null }, shape: "without a map index" },
    {
      recorded: { ...wait, region_id: "0199f8a0-0000-7000-8000-0000000000d1" },
      shape: "with a region but no index",
    },
  ])("shows waits $shape as recorded", async ({ recorded }) => {
    const actions = { waits: [recorded] };

    render(<DeliveryDetails delivery={buildDelivery(actions)} />, { wrapper: Wrapper });

    expect(await screen.findByTestId("monaco-editor")).toHaveTextContent(
      JSON.stringify(actions, undefined, 2),
      {
        normalizeWhitespace: false,
      },
    );
    expect(screen.queryByTestId("delivery-waits")).not.toBeInTheDocument();
  });

  it("lists each cancel request with its Dag, scope, status and what it cancelled", () => {
    render(
      <DeliveryDetails
        delivery={buildDelivery({ cancel_requests: [recordedRunRequest, recordedTaskRequest] }, [
          runRequest,
          taskRequest,
        ])}
      />,
      { wrapper: Wrapper },
    );

    const [runItem, taskItem] = [...screen.getByTestId("delivery-cancel-requests").children] as [
      HTMLElement,
      HTMLElement,
    ];

    expect(
      within(runItem)
        .getAllByRole("link")
        .map((link) => [link.textContent, link.getAttribute("href")]),
    ).toEqual([
      ["preview_environment", "/dags/preview_environment"],
      ["asset_triggered__2026-10-20", "/dags/preview_environment/runs/asset_triggered__2026-10-20"],
    ]);
    expect(within(runItem).getByText("webhookDeliveries.details.cancelScopes.run")).toBeInTheDocument();
    expect(
      within(runItem).getByText("webhookDeliveries.details.cancelRequestStatuses.applied"),
    ).toBeInTheDocument();
    expect(
      within(runItem).getByText("webhookDeliveries.details.cancelRequestStatusExplanations.applied"),
    ).toBeInTheDocument();
    expect(within(runItem).getByText("webhookDeliveries.details.appliedAt")).toBeInTheDocument();
    expect(within(runItem).queryByText("webhookDeliveries.details.cancelTasks")).not.toBeInTheDocument();

    expect(
      within(taskItem)
        .getAllByRole("link")
        .map((link) => [link.textContent, link.getAttribute("href")]),
    ).toEqual([
      ["ship_when_labeled", "/dags/ship_when_labeled"],
      ["run_e2e_tests", "/dags/ship_when_labeled/runs/manual_1/tasks/run_e2e_tests"],
      [
        "run_e2e_tests [2]",
        `/dags/ship_when_labeled/runs/manual_1/tasks/run_e2e_tests/mapped/2?region_id=${regionId}&region_index=2`,
      ],
      ["manual_2", "/dags/ship_when_labeled/runs/manual_2"],
    ]);
    expect(within(taskItem).getByText("webhookDeliveries.details.cancelScopes.task")).toBeInTheDocument();
    expect(
      within(taskItem).getByText("deploy (states.failed), run_e2e_tests (states.skipped)"),
    ).toBeInTheDocument();
    expect(
      within(taskItem).getByText("webhookDeliveries.details.runsNotCancelledReasons.dag_has_team"),
    ).toBeInTheDocument();
    expect(screen.queryByText("webhookDeliveries.details.noRunCancelled")).not.toBeInTheDocument();
    expect(screen.queryByTestId("monaco-editor")).not.toBeInTheDocument();
  });

  it.each([
    {
      absent: ["webhookDeliveries.details.cancelError"],
      expected: [
        "webhookDeliveries.details.cancelRequestStatuses.applied",
        "webhookDeliveries.details.noRunCancelled",
      ],
      request: {
        ...runRequest,
        result: { dag_runs: [], error: null, skipped_runs: [], task_instances: [] },
      },
      shape: "an applied request that found no run",
    },
    {
      absent: [
        "webhookDeliveries.details.noRunCancelled",
        "webhookDeliveries.details.appliedAt",
        "webhookDeliveries.details.failedAt",
      ],
      expected: [
        "webhookDeliveries.details.cancelRequestStatuses.pending",
        "webhookDeliveries.details.cancelRequestStatusExplanations.pending",
      ],
      request: { ...runRequest, applied_at: null, result: null, status: "pending" as const },
      shape: "a pending request",
    },
    {
      absent: ["webhookDeliveries.details.noRunCancelled", "webhookDeliveries.details.appliedAt"],
      expected: [
        "webhookDeliveries.details.cancelRequestStatuses.failed",
        "webhookDeliveries.details.cancelRequestStatusExplanations.failed",
        "webhookDeliveries.details.failedAt",
        "RuntimeError",
      ],
      request: {
        ...runRequest,
        result: { dag_runs: [], error: "RuntimeError", skipped_runs: [], task_instances: [] },
        status: "failed" as const,
      },
      shape: "a failed request",
    },
    {
      absent: [],
      expected: ["superseded", "webhookDeliveries.details.cancelRequestStatusExplanations.unknown"],
      request: { ...runRequest, status: "superseded" as WebhookCancelRequestResponse["status"] },
      shape: "a request with a status this UI does not know",
    },
    {
      absent: ["webhookDeliveries.details.noRunCancelled"],
      expected: [
        "webhookDeliveries.details.runsNotCancelledReasons.dag_not_found",
        "webhookDeliveries.details.runsNotCancelledReasons.unknown",
      ],
      request: {
        ...runRequest,
        result: {
          dag_runs: [],
          error: null,
          skipped_runs: [
            { reason: "dag_not_found", run_id: "manual_3" },
            { reason: "something_new", run_id: "manual_4" },
          ],
          task_instances: [],
        },
      },
      shape: "a request that skipped runs for a known and an unknown reason",
    },
    {
      absent: ["webhookDeliveries.details.noRunCancelled", "webhookDeliveries.details.appliedAt"],
      expected: ["webhookDeliveries.details.cancelRequestStatusExplanations.gone"],
      request: undefined,
      shape: "a request whose record is gone",
    },
  ])("says what became of $shape", ({ absent, expected, request }) => {
    render(
      <DeliveryDetails
        delivery={buildDelivery(
          { cancel_requests: [recordedRunRequest] },
          request === undefined ? [] : [request],
        )}
      />,
      { wrapper: Wrapper },
    );

    const [item] = [...screen.getByTestId("delivery-cancel-requests").children] as [HTMLElement];

    for (const text of expected) {
      expect(within(item).getByText(text)).toBeInTheDocument();
    }
    for (const text of absent) {
      expect(within(item).queryByText(text)).not.toBeInTheDocument();
    }
    expect(within(item).getByRole("link", { name: "preview_environment" })).toBeInTheDocument();
    expect(within(item).getByText("webhookDeliveries.details.cancelScopes.run")).toBeInTheDocument();
  });

  it("lists a cancel request that the delivery's actions miss", () => {
    render(
      <DeliveryDetails
        delivery={buildDelivery({ cancel_requests: [recordedRunRequest] }, [runRequest, taskRequest])}
      />,
      { wrapper: Wrapper },
    );

    const items = [...screen.getByTestId("delivery-cancel-requests").children] as Array<HTMLElement>;

    expect(items).toHaveLength(2);

    const [, taskItem] = items as [HTMLElement, HTMLElement];

    expect(within(taskItem).getByRole("link", { name: "ship_when_labeled" })).toHaveAttribute(
      "href",
      "/dags/ship_when_labeled",
    );
    expect(within(taskItem).getByText("webhookDeliveries.details.cancelScopes.task")).toBeInTheDocument();
    expect(
      within(taskItem).getByText("webhookDeliveries.details.cancelRequestStatuses.applied"),
    ).toBeInTheDocument();
    expect(
      within(taskItem).queryByText("webhookDeliveries.details.cancelRequestStatusExplanations.gone"),
    ).not.toBeInTheDocument();
  });

  it.each([
    { recorded: { ...recordedRunRequest, scope: "tasks" }, shape: "with another scope" },
    { recorded: { dag_id: "preview_environment", scope: "run" }, shape: "without a request id" },
  ])("shows cancel requests $shape as recorded", async ({ recorded }) => {
    const actions = { cancel_requests: [recorded] };

    render(<DeliveryDetails delivery={buildDelivery(actions, [runRequest])} />, { wrapper: Wrapper });

    expect(await screen.findByTestId("monaco-editor")).toHaveTextContent(
      JSON.stringify(actions, undefined, 2),
      {
        normalizeWhitespace: false,
      },
    );
    const items = [...screen.getByTestId("delivery-cancel-requests").children] as Array<HTMLElement>;

    expect(items).toHaveLength(1);

    const [item] = items as [HTMLElement];

    expect(within(item).getByRole("link", { name: "preview_environment" })).toBeInTheDocument();
    expect(within(item).getByText("webhookDeliveries.details.cancelScopes.run")).toBeInTheDocument();
  });
});
