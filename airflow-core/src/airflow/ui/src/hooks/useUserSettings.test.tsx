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
import { afterEach, describe, expect, it } from "vitest";

import {
  CLEAR_PREVENT_RUNNING_TASK_KEY,
  CLEAR_RUN_DEFAULT_OPTIONS_KEY,
  CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
  DEFAULT_GRAPH_DIRECTION_KEY,
  MARK_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
} from "src/constants/localStorage";

import {
  useClearPreventRunningTaskDefault,
  useClearRunDefaultOptions,
  useClearTaskInstanceDefaultOptions,
  useDefaultGraphDirection,
  useMarkTaskInstanceDefaultOptions,
} from "./useUserSettings";

afterEach(() => {
  localStorage.clear();
});

describe("useDefaultGraphDirection", () => {
  it("defaults to RIGHT when nothing is stored", () => {
    const { result } = renderHook(() => useDefaultGraphDirection());

    expect(result.current[0]).toBe("RIGHT");
  });

  it("reads an existing stored direction", () => {
    localStorage.setItem(DEFAULT_GRAPH_DIRECTION_KEY, JSON.stringify("LEFT"));

    const { result } = renderHook(() => useDefaultGraphDirection());

    expect(result.current[0]).toBe("LEFT");
  });

  it("persists a new direction to localStorage", () => {
    const { result } = renderHook(() => useDefaultGraphDirection());

    act(() => {
      result.current[1]("DOWN");
    });

    expect(result.current[0]).toBe("DOWN");
    expect(JSON.parse(localStorage.getItem(DEFAULT_GRAPH_DIRECTION_KEY) ?? '""')).toBe("DOWN");
  });
});

describe("useClearRunDefaultOptions", () => {
  it("defaults to ['existingTasks']", () => {
    const { result } = renderHook(() => useClearRunDefaultOptions());

    expect(result.current[0]).toEqual(["existingTasks"]);
  });

  it("persists a new selection", () => {
    const { result } = renderHook(() => useClearRunDefaultOptions());

    act(() => {
      result.current[1](["onlyFailed"]);
    });

    expect(result.current[0]).toEqual(["onlyFailed"]);
    expect(JSON.parse(localStorage.getItem(CLEAR_RUN_DEFAULT_OPTIONS_KEY) ?? "[]")).toEqual(["onlyFailed"]);
  });
});

describe("useClearTaskInstanceDefaultOptions", () => {
  it("defaults to ['downstream']", () => {
    const { result } = renderHook(() => useClearTaskInstanceDefaultOptions());

    expect(result.current[0]).toEqual(["downstream"]);
  });

  it("persists a new selection", () => {
    const { result } = renderHook(() => useClearTaskInstanceDefaultOptions());

    act(() => {
      result.current[1](["past", "future", "downstream"]);
    });

    expect(result.current[0]).toEqual(["past", "future", "downstream"]);
    expect(JSON.parse(localStorage.getItem(CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY) ?? "[]")).toEqual([
      "past",
      "future",
      "downstream",
    ]);
  });
});

describe("useClearPreventRunningTaskDefault", () => {
  it("defaults to true", () => {
    const { result } = renderHook(() => useClearPreventRunningTaskDefault());

    expect(result.current[0]).toBe(true);
  });

  it("persists a new value", () => {
    const { result } = renderHook(() => useClearPreventRunningTaskDefault());

    act(() => {
      result.current[1](false);
    });

    expect(result.current[0]).toBe(false);
    expect(JSON.parse(localStorage.getItem(CLEAR_PREVENT_RUNNING_TASK_KEY) ?? "true")).toBe(false);
  });
});

describe("useMarkTaskInstanceDefaultOptions", () => {
  it("defaults to an empty selection", () => {
    const { result } = renderHook(() => useMarkTaskInstanceDefaultOptions());

    expect(result.current[0]).toEqual([]);
  });

  it("persists a new selection", () => {
    const { result } = renderHook(() => useMarkTaskInstanceDefaultOptions());

    act(() => {
      result.current[1](["downstream"]);
    });

    expect(result.current[0]).toEqual(["downstream"]);
    expect(JSON.parse(localStorage.getItem(MARK_TASK_INSTANCE_DEFAULT_OPTIONS_KEY) ?? "[]")).toEqual([
      "downstream",
    ]);
  });
});
