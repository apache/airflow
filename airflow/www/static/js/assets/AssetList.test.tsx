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

/* global describe, test, expect */

import React from "react";
import { render } from "@testing-library/react";

import * as useAssetsModule from "src/api/useAssetsSummary";
import { Wrapper } from "src/utils/testUtils";

import type { UseQueryResult } from "react-query";
import type { AssetListItem } from "src/types";
import AssetsList from "./AssetsList";

const assets = [
  {
    id: 0,
    uri: "this_asset",
    extra: null,
    lastAssetUpdate: null,
    totalUpdates: 0,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  },
  {
    id: 1,
    uri: "that_asset",
    extra: null,
    lastAssetUpdate: new Date().toISOString(),
    totalUpdates: 10,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  },
  {
    id: 1,
    uri: "extra_asset",
    extra: null,
    lastAssetUpdate: new Date().toISOString(),
    totalUpdates: 1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  },
];

type UseAssetsReturn = UseQueryResult<useAssetsModule.AssetsData> & {
  data: useAssetsModule.AssetsData;
};

const returnValue = {
  data: {
    assets,
    totalEntries: assets.length,
  },
  isSuccess: true,
} as UseAssetsReturn;

const emptyReturnValue = {
  data: {
    assets: [] as AssetListItem[],
    totalEntries: 0,
  },
  isSuccess: true,
  isLoading: false,
} as UseAssetsReturn;

describe("Test Assets List", () => {
  test("Displays a list of assets", () => {
    jest
      .spyOn(useAssetsModule, "default")
      .mockImplementation(() => returnValue);

    const { getByText, queryAllByTestId } = render(
      <AssetsList onSelect={() => {}} />,
      { wrapper: Wrapper }
    );

    const listItems = queryAllByTestId("asset-list-item");

    expect(listItems).toHaveLength(3);

    expect(getByText(assets[0].uri)).toBeDefined();
    expect(getByText("Total Updates: 0")).toBeDefined();

    expect(getByText(assets[1].uri)).toBeDefined();
    expect(getByText("Total Updates: 10")).toBeDefined();

    expect(getByText(assets[2].uri)).toBeDefined();
    expect(getByText("Total Updates: 1")).toBeDefined();
  });

  test("Empty state displays when there are no assets", () => {
    jest
      .spyOn(useAssetsModule, "default")
      .mockImplementation(() => emptyReturnValue);

    const { getByText, queryAllByTestId, getByTestId } = render(
      <AssetsList onSelect={() => {}} />,
      { wrapper: Wrapper }
    );

    const listItems = queryAllByTestId("asset-list-item");

    expect(listItems).toHaveLength(0);

    expect(getByTestId("no-assets-msg")).toBeInTheDocument();
    expect(getByText("No Data found.")).toBeInTheDocument();
  });
});
