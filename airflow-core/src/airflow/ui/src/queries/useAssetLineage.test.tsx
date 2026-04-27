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
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useAssetLineage } from "./useAssetLineage";

const { mockGetAssetLineage, mockGetAssetOnlyLineage } = vi.hoisted(() => ({
  mockGetAssetLineage: vi.fn(),
  mockGetAssetOnlyLineage: vi.fn(),
}));

vi.mock("openapi/requests/services.gen", () => ({
  AssetService: {
    getAssetLineage: mockGetAssetLineage,
    getAssetOnlyLineage: mockGetAssetOnlyLineage,
  },
}));

const TestWrapper = ({ children }: PropsWithChildren) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        staleTime: Infinity,
      },
    },
  });

  return (
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    </ChakraProvider>
  );
};

describe("useAssetLineage", () => {
  beforeEach(() => {
    mockGetAssetLineage.mockReset();
    mockGetAssetOnlyLineage.mockReset();
    mockGetAssetLineage.mockResolvedValue({ edges: [], nodes: [] });
    mockGetAssetOnlyLineage.mockResolvedValue({ edges: [], nodes: [] });
    window.history.replaceState({}, "", "/");
  });

  afterEach(() => {
    window.history.replaceState({}, "", "/");
  });

  it("calls the full lineage endpoint by default", async () => {
    const { result } = renderHook(() => useAssetLineage("33"), { wrapper: TestWrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(mockGetAssetLineage).toHaveBeenCalledWith({ assetId: 33, depth: undefined });
    expect(mockGetAssetOnlyLineage).not.toHaveBeenCalled();
  });

  it("calls the asset-only lineage endpoint when requested", async () => {
    const { result } = renderHook(() => useAssetLineage("33", { depth: 5, mode: "asset_only" }), {
      wrapper: TestWrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(mockGetAssetOnlyLineage).toHaveBeenCalledWith({ assetId: 33, depth: 5 });
    expect(mockGetAssetLineage).not.toHaveBeenCalled();
  });

  it("returns mock full lineage data when mockAssets is enabled", async () => {
    window.history.replaceState({}, "", "/assets/33?mockAssets=true");

    const { result } = renderHook(() => useAssetLineage("33"), { wrapper: TestWrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data?.nodes.map((node) => node.id)).toEqual([
      "dag:upstream_dag",
      "task:upstream_dag:producer_task",
      "asset:1",
      "task:downstream_dag:consumer_task",
      "asset:2",
    ]);
    expect(mockGetAssetLineage).not.toHaveBeenCalled();
    expect(mockGetAssetOnlyLineage).not.toHaveBeenCalled();
  });

  it("returns mock asset-only lineage data when mockAssets is enabled", async () => {
    window.history.replaceState({}, "", "/assets/33?mockAssets=true");

    const { result } = renderHook(() => useAssetLineage("33", { mode: "asset_only" }), {
      wrapper: TestWrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data?.edges[0]?.column_lineage).toEqual({
      player_name: [
        {
          source_asset_uri: "file://incoming/player-stats/team_b_raw.csv",
          source_column: "player_name",
        },
      ],
    });
    expect(mockGetAssetLineage).not.toHaveBeenCalled();
    expect(mockGetAssetOnlyLineage).not.toHaveBeenCalled();
  });

  it("surfaces mock errors when requested", async () => {
    window.history.replaceState({}, "", "/assets/33?mockAssets=true&mockAssetsError=true");

    const { result } = renderHook(() => useAssetLineage("33", { mode: "asset_only" }), {
      wrapper: TestWrapper,
    });

    await waitFor(() => expect(result.current.isError).toBe(true));

    expect(result.current.error).toEqual(new Error("Mock asset lineage request failed."));
  });
});
