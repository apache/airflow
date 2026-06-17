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
import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useConfig } from "src/queries/useConfig";

import { useRerunWithLatestVersion } from "./useRerunWithLatestVersion";

// Mock useConfig to control the global config value
vi.mock("src/queries/useConfig", () => ({
  useConfig: vi.fn(),
}));

const mockUseConfig = vi.mocked(useConfig);

describe("useRerunWithLatestVersion — precedence", () => {
  it("falls back to default when config is not yet resolved", () => {
    mockUseConfig.mockReturnValue(undefined);

    const { result } = renderHook(() => useRerunWithLatestVersion({ dagLevelConfig: undefined }));

    expect(result.current.value).toBe(false);
  });

  it("DAG-level true takes precedence over global false", () => {
    mockUseConfig.mockReturnValue(false);

    const { result } = renderHook(() => useRerunWithLatestVersion({ dagLevelConfig: true }));

    expect(result.current.value).toBe(true);
  });

  it("DAG-level false takes precedence over global true", () => {
    mockUseConfig.mockReturnValue(true);

    const { result } = renderHook(() => useRerunWithLatestVersion({ dagLevelConfig: false }));

    expect(result.current.value).toBe(false);
  });

  it("falls back to global config when DAG-level is null", () => {
    mockUseConfig.mockReturnValue(true);

    const { result } = renderHook(() => useRerunWithLatestVersion({ dagLevelConfig: null }));

    expect(result.current.value).toBe(true);
  });

  it("falls back to false when DAG-level is null and global is false", () => {
    mockUseConfig.mockReturnValue(false);

    const { result } = renderHook(() => useRerunWithLatestVersion({ dagLevelConfig: null }));

    expect(result.current.value).toBe(false);
  });

  it("defaults to false when no config is set", () => {
    mockUseConfig.mockReturnValue(undefined);

    const { result } = renderHook(() => useRerunWithLatestVersion({ dagLevelConfig: null }));

    expect(result.current.value).toBe(false);
  });

  it("applies global config when DAG details are still loading", () => {
    // Global config loads first, DAG details still undefined
    mockUseConfig.mockReturnValue(true);

    const { rerender, result } = renderHook(
      ({ dagLevelConfig }: { dagLevelConfig?: boolean | null }) =>
        useRerunWithLatestVersion({ dagLevelConfig }),
      { initialProps: { dagLevelConfig: undefined as boolean | null | undefined } },
    );

    // Global config is true, DAG details not yet loaded -> should apply global default
    expect(result.current.value).toBe(true);

    // DAG details load with explicit false -> should override to false
    rerender({ dagLevelConfig: false });

    expect(result.current.value).toBe(false);
  });

  it("uses fallback=true for backfills when no DAG or global config", () => {
    mockUseConfig.mockReturnValue(undefined);

    const { result } = renderHook(() => useRerunWithLatestVersion({ dagLevelConfig: null, fallback: true }));

    expect(result.current.value).toBe(true);
  });

  it("DAG-level false overrides fallback=true", () => {
    mockUseConfig.mockReturnValue(undefined);

    const { result } = renderHook(() => useRerunWithLatestVersion({ dagLevelConfig: false, fallback: true }));

    expect(result.current.value).toBe(false);
  });
});

describe("useRerunWithLatestVersion — user toggle", () => {
  it("user toggle overrides the resolved default", () => {
    mockUseConfig.mockReturnValue(true);

    const { result } = renderHook(() => useRerunWithLatestVersion({ dagLevelConfig: true }));

    expect(result.current.value).toBe(true);

    act(() => {
      result.current.setValue(false);
    });

    expect(result.current.value).toBe(false);
  });

  it("user toggle is preserved when config changes", () => {
    mockUseConfig.mockReturnValue(false);

    const { rerender, result } = renderHook(
      ({ dagLevelConfig }: { dagLevelConfig?: boolean | null }) =>
        useRerunWithLatestVersion({ dagLevelConfig }),
      { initialProps: { dagLevelConfig: false as boolean | null } },
    );

    // User toggles to true
    act(() => {
      result.current.setValue(true);
    });

    expect(result.current.value).toBe(true);

    // Config changes, but user toggle should be preserved
    rerender({ dagLevelConfig: false });

    expect(result.current.value).toBe(true);
  });
});
