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

import type { AssetExpressionAsset } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { AssetNode } from "./AssetNode";

const readableAsset: AssetExpressionAsset = {
  asset: { group: "asset", id: 1, name: "sales_daily", uri: "s3://bucket/sales" },
};

// The shape `next_run_assets` serves for an asset the caller may not read.
const hiddenAsset: AssetExpressionAsset = {
  asset: { group: "asset", hidden: true, id: null, name: null, uri: null },
};

describe("AssetNode", () => {
  it("labels a hidden asset instead of linking to it, leaving readable assets linked", () => {
    render(
      <>
        <AssetNode asset={readableAsset} />
        <AssetNode asset={hiddenAsset} />
      </>,
      { wrapper: Wrapper },
    );

    expect(screen.getByText("expression.hiddenAsset")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "sales_daily" })).toHaveAttribute("href", "/assets/1");
    expect(screen.getAllByRole("link")).toHaveLength(1);
  });
});
