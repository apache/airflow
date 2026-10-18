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
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { AssetEventResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { AssetEvent } from "./AssetEvent";

const baseEvent = {
  asset_id: 1,
  created_dagruns: [],
  id: 1,
  source_map_index: -1,
  timestamp: "2025-01-01T00:00:00Z",
} satisfies Partial<AssetEventResponse> as AssetEventResponse;

const deliveryRecordId = "0199f8a0-0000-7000-8000-000000000001";
const webhookExtra = { from_webhook: true, inbound_delivery_id: deliveryRecordId, webhook: "demo" };
const taskSource = { source_dag_id: "producer", source_run_id: "run_1", source_task_id: "emit" };
const taskPath = "/dags/producer/runs/run_1/tasks/emit";

describe("AssetEvent", () => {
  it("does not render a partition key line when partition_key is null", () => {
    render(<AssetEvent assetId={1} event={{ ...baseEvent, partition_key: null }} />, { wrapper: Wrapper });

    expect(screen.queryByText(/dagRun\.partitionKey/u)).not.toBeInTheDocument();
  });

  it("renders the partition key line when partition_key is set", () => {
    render(<AssetEvent assetId={1} event={{ ...baseEvent, partition_key: "foo" }} />, { wrapper: Wrapper });

    expect(screen.getByText("dagRun.partitionKey: foo")).toBeInTheDocument();
  });

  it("does not render a source when the event has no source task or source flag", () => {
    render(
      <AssetEvent
        assetId={1}
        event={{ ...baseEvent, source_dag_id: null, source_run_id: null, source_task_id: null }}
      />,
      { wrapper: Wrapper },
    );

    expect(screen.queryByRole("link")).not.toBeInTheDocument();
    expect(screen.queryByText(/source/u)).not.toBeInTheDocument();
  });

  it.each([
    { extra: { from_rest_api: true }, label: "sources.api", name: "an API event" },
    { extra: { from_trigger: true }, label: "sources.trigger", name: "a trigger event" },
    // The API sets from_rest_api itself, so a caller's extra cannot make its event a delivery's.
    {
      extra: { ...webhookExtra, from_rest_api: true },
      label: "sources.api",
      name: "an API event that claims a delivery",
    },
    {
      extra: { from_webhook: true, webhook: "demo" },
      label: "sources.webhook",
      name: "a webhook event without a delivery",
    },
    {
      extra: { ...webhookExtra, inbound_delivery_id: 7 },
      label: "sources.webhook",
      name: "a webhook event with a numeric delivery",
    },
    {
      extra: { ...webhookExtra, webhook: ".." },
      label: "sources.webhook",
      name: "a webhook event with an invalid name",
    },
  ])("labels $name without a link", ({ extra, label }) => {
    render(<AssetEvent assetId={1} event={{ ...baseEvent, extra, source_dag_id: null }} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText(label)).toBeInTheDocument();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });

  it.each([
    {
      description: "a webhook event to its delivery",
      event: { extra: webhookExtra, source_dag_id: null },
      href: `/webhook_deliveries/demo?id=${deliveryRecordId}`,
      name: "sources.webhook",
    },
    {
      description: "a webhook event to its delivery, escaping the delivery id",
      event: { extra: { ...webhookExtra, inbound_delivery_id: "x&outcome=accepted" }, source_dag_id: null },
      href: "/webhook_deliveries/demo?id=x%26outcome%3Daccepted",
      name: "sources.webhook",
    },
    {
      description: "a task event that sets from_webhook to its task",
      event: { ...taskSource, extra: webhookExtra },
      href: taskPath,
      name: "producer",
    },
    {
      description: "a task event that sets from_rest_api to its task",
      event: { ...taskSource, extra: { from_rest_api: true } },
      href: taskPath,
      name: "producer",
    },
  ])("links $description", ({ event, href, name }) => {
    render(<AssetEvent assetId={1} event={{ ...baseEvent, ...event }} />, { wrapper: Wrapper });

    expect(screen.getByRole("link", { name })).toHaveAttribute("href", href);
  });

  it.each([
    {
      description: "a webhook event",
      event: { extra: webhookExtra, source_dag_id: null },
      flag: "from_webhook",
      verb: "hides",
    },
    {
      description: "a task event that sets from_webhook",
      event: { ...taskSource, extra: webhookExtra },
      flag: "from_webhook",
      verb: "shows",
    },
    {
      description: "a task event that sets from_rest_api",
      event: { ...taskSource, extra: { from_rest_api: true } },
      flag: "from_rest_api",
      verb: "shows",
    },
  ])("$verb the $flag flag in the extra of $description", ({ event, flag, verb }) => {
    render(<AssetEvent assetId={1} event={{ ...baseEvent, ...event }} />, { wrapper: Wrapper });

    expect(screen.queryAllByText(`${flag}:`)).toHaveLength(verb === "shows" ? 1 : 0);
  });
});
