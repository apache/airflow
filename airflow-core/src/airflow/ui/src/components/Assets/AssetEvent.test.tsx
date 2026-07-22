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

describe("AssetEvent", () => {
  it("does not render a partition key line when partition_key is null", () => {
    render(<AssetEvent assetId={1} event={{ ...baseEvent, partition_key: null }} />, { wrapper: Wrapper });

    expect(screen.queryByText(/dagRun\.partitionKey/u)).not.toBeInTheDocument();
  });

  it("renders the partition key line when partition_key is set", () => {
    render(<AssetEvent assetId={1} event={{ ...baseEvent, partition_key: "foo" }} />, { wrapper: Wrapper });

    expect(screen.getByText("dagRun.partitionKey: foo")).toBeInTheDocument();
  });
});
