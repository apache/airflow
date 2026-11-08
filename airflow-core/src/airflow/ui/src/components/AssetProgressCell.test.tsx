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
import { expect, it, vi } from "vitest";

import { PartitionedDagRunService } from "openapi/requests/services.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { AssetProgressCell } from "./AssetProgressCell";

it("fetches separate details for pending runs sharing a partition key", async () => {
  const request = vi.spyOn(PartitionedDagRunService, "getPendingPartitionedDagRun").mockResolvedValue({
    assets: [],
    dag_id: "consumer",
    id: 1,
    partition_key: "same-key",
    total_received: 1,
    total_required: 1,
  });

  render(
    <>
      <AssetProgressCell
        dagId="consumer"
        partitionedDagRunId={1}
        partitionKey="same-key"
        totalReceived={1}
        totalRequired={1}
      />
      <AssetProgressCell
        dagId="consumer"
        partitionedDagRunId={2}
        partitionKey="same-key"
        totalReceived={1}
        totalRequired={1}
      />
    </>,
    { wrapper: Wrapper },
  );

  await waitFor(() => {
    expect(request).toHaveBeenCalledWith({
      dagId: "consumer",
      partitionedDagRunId: 1,
      partitionKey: "same-key",
    });
    expect(request).toHaveBeenCalledWith({
      dagId: "consumer",
      partitionedDagRunId: 2,
      partitionKey: "same-key",
    });
  });
});

it("labels a hidden asset instead of linking to it, leaving readable assets linked", async () => {
  // The shape the pending-detail endpoint serves for an asset the caller may not read.
  vi.spyOn(PartitionedDagRunService, "getPendingPartitionedDagRun").mockResolvedValue({
    assets: [
      {
        asset_id: 1,
        asset_name: "sales_daily",
        asset_uri: "s3://bucket/sales",
        hidden: false,
        is_rollup: true,
        received: false,
        received_count: 0,
        received_keys: [],
        required_count: 1,
        required_keys: ["2024-07-01"],
      },
      {
        asset_id: null,
        asset_name: null,
        asset_uri: null,
        hidden: true,
        is_rollup: true,
        received: false,
        received_count: 0,
        received_keys: [],
        required_count: 1,
        required_keys: ["2024-07-01"],
      },
    ],
    dag_id: "consumer",
    id: 1,
    partition_key: "2024-07-01",
    total_received: 0,
    total_required: 2,
  });

  render(
    <Wrapper>
      <AssetProgressCell
        dagId="consumer"
        partitionedDagRunId={1}
        partitionKey="2024-07-01"
        totalReceived={0}
        totalRequired={2}
      />
    </Wrapper>,
  );

  const trigger = screen.getByRole("button");

  // The trigger is disabled while the detail query is in flight, which swallows the click.
  await waitFor(() => expect(trigger).not.toBeDisabled());
  fireEvent.click(trigger);

  await waitFor(() => expect(screen.getByText(i18n.t("common:expression.hiddenAsset"))).toBeInTheDocument());
  expect(screen.getByRole("link", { name: "sales_daily" })).toHaveAttribute("href", "/assets/1");
  expect(screen.getAllByRole("link")).toHaveLength(1);
});
