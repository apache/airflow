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
import type { Monaco } from "@monaco-editor/react";
import { renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// `useColorMode` is the only dependency of the hook we want to test. We mock
// it with a mutable return so individual tests can drive light/dark behaviour.
const colorModeMock = vi.fn<() => { colorMode: "dark" | "light" | undefined }>();

vi.mock("./useColorMode", () => ({
  useColorMode: () => colorModeMock(),
}));

// The hook registers Monaco themes exactly once via a module-level flag. We
// reset modules between tests so each test starts with a fresh flag state.
const loadHook = async () => {
  const module = await import("./useMonacoTheme");

  return module.useMonacoTheme;
};

const createFakeMonaco = () => {
  const defineTheme = vi.fn();

  return { defineTheme, monaco: { editor: { defineTheme } } as unknown as Monaco };
};

// happy-dom does not resolve Chakra's CSS custom properties, so the hook's
// `getPropertyValue` calls would return empty strings. Stub it to return a
// parseable value — culori accepts plain hex, so the exact string doesn't
// matter as long as it's a valid CSS color the parser recognizes.
const stubComputedStyle = () => {
  vi.spyOn(globalThis, "getComputedStyle").mockReturnValue({
    getPropertyValue: () => "#abcdef",
  } as unknown as CSSStyleDeclaration);
};

describe("useMonacoTheme", () => {
  beforeEach(() => {
    vi.resetModules();
    colorModeMock.mockReturnValue({ colorMode: "light" });
    stubComputedStyle();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns the airflow-light theme name when color mode is light", async () => {
    const useMonacoTheme = await loadHook();
    const { result } = renderHook(() => useMonacoTheme());

    expect(result.current.theme).toBe("airflow-light");
  });

  it("returns the airflow-dark theme name when color mode is dark", async () => {
    colorModeMock.mockReturnValue({ colorMode: "dark" });
    const useMonacoTheme = await loadHook();
    const { result } = renderHook(() => useMonacoTheme());

    expect(result.current.theme).toBe("airflow-dark");
  });

  it("falls back to airflow-light when color mode is undefined", async () => {
    colorModeMock.mockReturnValue({ colorMode: undefined });
    const useMonacoTheme = await loadHook();
    const { result } = renderHook(() => useMonacoTheme());

    expect(result.current.theme).toBe("airflow-light");
  });

  it("registers both airflow themes when beforeMount runs for the first time", async () => {
    const useMonacoTheme = await loadHook();
    const { result } = renderHook(() => useMonacoTheme());
    const { defineTheme, monaco } = createFakeMonaco();

    result.current.beforeMount(monaco);

    expect(defineTheme).toHaveBeenCalledTimes(2);
    expect(defineTheme).toHaveBeenCalledWith("airflow-light", expect.objectContaining({ base: "vs" }));
    expect(defineTheme).toHaveBeenCalledWith("airflow-dark", expect.objectContaining({ base: "vs-dark" }));
  });

  it("does not re-register themes on subsequent beforeMount calls", async () => {
    const useMonacoTheme = await loadHook();
    const { result } = renderHook(() => useMonacoTheme());
    const first = createFakeMonaco();
    const second = createFakeMonaco();

    result.current.beforeMount(first.monaco);
    result.current.beforeMount(second.monaco);

    expect(first.defineTheme).toHaveBeenCalledTimes(2);
    expect(second.defineTheme).not.toHaveBeenCalled();
  });

  it("inherits from the base theme and adds no syntax token rules", async () => {
    const useMonacoTheme = await loadHook();
    const { result } = renderHook(() => useMonacoTheme());
    const { defineTheme, monaco } = createFakeMonaco();

    result.current.beforeMount(monaco);

    for (const call of defineTheme.mock.calls) {
      const [, themeData] = call as [string, { inherit: boolean; rules: Array<unknown> }];

      expect(themeData.inherit).toBe(true);
      expect(themeData.rules).toEqual([]);
    }
  });
});
