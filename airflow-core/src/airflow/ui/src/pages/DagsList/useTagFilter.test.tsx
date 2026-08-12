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
import type { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { useTagFilter } from "./useTagFilter";

const createWrapper =
  (initialEntries: Array<string> = ["/"]) =>
  ({ children }: PropsWithChildren) => (
    <BaseWrapper>
      <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
    </BaseWrapper>
  );

afterEach(() => {
  localStorage.clear();
});

describe("useTagFilter — initial state", () => {
  it("returns empty tags and 'any' mode by default", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(),
    });

    expect(result.current.selectedTags).toEqual([]);
    expect(result.current.tagFilterMode).toBe("any");
  });

  it("reads tags from URL params", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(["/?tags=production&tags=ml"]),
    });

    expect(result.current.selectedTags).toEqual(["production", "ml"]);
  });

  it("reads match mode from URL params", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(["/?tags=production&tags=ml&tags_match_mode=all"]),
    });

    expect(result.current.tagFilterMode).toBe("all");
  });

  it("ignores legacy tag values left in localStorage (URL is the only source)", () => {
    localStorage.setItem("tags", JSON.stringify(["saved-tag-1", "saved-tag-2"]));
    localStorage.setItem("tags_match_mode", JSON.stringify("all"));

    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(),
    });

    expect(result.current.selectedTags).toEqual([]);
    expect(result.current.tagFilterMode).toBe("any");
  });
});

describe("useTagFilter — setSelectedTags", () => {
  it("sets tags", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(),
    });

    act(() => {
      result.current.setSelectedTags(["new-tag-1", "new-tag-2"]);
    });

    expect(result.current.selectedTags).toEqual(["new-tag-1", "new-tag-2"]);
  });

  it("does not persist tags to localStorage", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(),
    });

    act(() => {
      result.current.setSelectedTags(["some-tag"]);
    });

    expect(localStorage.getItem("tags")).toBeNull();
  });

  it("clears tags when given empty array", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(["/?tags=old-tag"]),
    });

    act(() => {
      result.current.setSelectedTags([]);
    });

    expect(result.current.selectedTags).toEqual([]);
  });

  it("does not reset match mode when tags drop below 2", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(["/?tags=a&tags=b&tags_match_mode=all"]),
    });

    expect(result.current.tagFilterMode).toBe("all");

    act(() => {
      result.current.setSelectedTags(["only-one"]);
    });

    expect(result.current.tagFilterMode).toBe("all");
  });

  it("resets offset when tags change", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(["/?tags=a&offset=20"]),
    });

    act(() => {
      result.current.setSelectedTags(["b"]);
    });

    expect(result.current.selectedTags).toEqual(["b"]);
  });
});

describe("useTagFilter — setTagFilterMode", () => {
  it("toggles mode from 'any' to 'all'", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(["/?tags=a&tags=b"]),
    });

    expect(result.current.tagFilterMode).toBe("any");

    act(() => {
      result.current.setTagFilterMode("all");
    });

    expect(result.current.tagFilterMode).toBe("all");
  });

  it("toggles mode from 'all' to 'any'", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(["/?tags=a&tags=b&tags_match_mode=all"]),
    });

    expect(result.current.tagFilterMode).toBe("all");

    act(() => {
      result.current.setTagFilterMode("any");
    });

    expect(result.current.tagFilterMode).toBe("any");
  });

  it("resets offset when mode changes", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(["/?tags=a&tags=b&tags_match_mode=any&offset=20"]),
    });

    act(() => {
      result.current.setTagFilterMode("all");
    });

    expect(result.current.tagFilterMode).toBe("all");
  });

  it("does not persist mode to localStorage", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(["/?tags=a&tags=b"]),
    });

    act(() => {
      result.current.setTagFilterMode("all");
    });

    expect(localStorage.getItem("tags_match_mode")).toBeNull();
  });
});

describe("useTagFilter — tag count transitions preserve match mode", () => {
  it("match mode is preserved when replacing tags", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(),
    });

    act(() => {
      result.current.setSelectedTags(["first", "second"]);
    });

    act(() => {
      result.current.setTagFilterMode("all");
    });

    expect(result.current.tagFilterMode).toBe("all");

    act(() => {
      result.current.setSelectedTags(["only-one"]);
    });

    expect(result.current.tagFilterMode).toBe("all");

    act(() => {
      result.current.setSelectedTags(["only-one", "new-second"]);
    });

    expect(result.current.tagFilterMode).toBe("all");
  });

  it("match mode is preserved when clearing all tags", () => {
    const { result } = renderHook(() => useTagFilter(), {
      wrapper: createWrapper(),
    });

    act(() => {
      result.current.setSelectedTags(["a", "b"]);
    });

    act(() => {
      result.current.setTagFilterMode("all");
    });

    act(() => {
      result.current.setSelectedTags([]);
    });

    expect(result.current.tagFilterMode).toBe("all");
  });
});
