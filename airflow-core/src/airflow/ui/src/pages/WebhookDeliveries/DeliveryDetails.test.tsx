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

import type { WebhookDeliveryResponse } from "openapi/requests/types.gen";

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

const buildDelivery = (actions: WebhookDeliveryResponse["actions"]): WebhookDeliveryResponse => ({
  actions,
  attributes: { action: "closed", event: "pull_request", number: 4242 },
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
});
