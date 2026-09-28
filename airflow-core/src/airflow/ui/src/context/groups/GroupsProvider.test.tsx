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
import type { PropsWithChildren } from "react";

import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { NodeResponse } from "openapi/requests/types.gen";

import { DEFAULT_TASK_GROUPS_EXPANDED_KEY, openGroupsKey } from "src/constants/localStorage";

import { GroupsProvider } from "./GroupsProvider";
import { useGroups } from "./useGroups";

const structure = vi.hoisted(() => ({ nodes: undefined as Array<NodeResponse> | undefined }));

vi.mock("openapi/queries", () => ({
  useStructureServiceStructureData: () => ({
    data: structure.nodes === undefined ? undefined : { edges: [], nodes: structure.nodes },
  }),
}));
vi.mock("src/hooks/useSelectedVersion", () => ({ default: () => 1 }));
const nodes: Array<NodeResponse> = [
  {
    children: [{ children: [], id: "group.nested", label: "Nested", type: "task" }],
    id: "group",
    label: "Group",
    type: "task",
  },
];
const wrapper = ({ children }: PropsWithChildren) => (
  <GroupsProvider dagId="example">{children}</GroupsProvider>
);

beforeEach(() => {
  structure.nodes = nodes;
});
afterEach(() => {
  localStorage.clear();
});

describe("task group defaults", () => {
  it("expands nested groups when the preference is enabled", () => {
    localStorage.setItem(DEFAULT_TASK_GROUPS_EXPANDED_KEY, "true");
    const { result } = renderHook(useGroups, { wrapper });

    expect(result.current.openGroupIds).toEqual(["group", "group.nested"]);
  });
  it.each([{ saved: [] }, { saved: ["group"] }])(
    "preserves a saved selection $saved over the default",
    ({ saved }) => {
      localStorage.setItem(DEFAULT_TASK_GROUPS_EXPANDED_KEY, "true");
      const initial = renderHook(useGroups, { wrapper });

      expect(initial.result.current.openGroupIds).toEqual(["group", "group.nested"]);
      initial.unmount();
      localStorage.setItem(openGroupsKey("example"), JSON.stringify(saved));
      const { result } = renderHook(useGroups, { wrapper });

      expect(result.current.openGroupIds).toEqual(saved);
    },
  );
  it("applies the default after structure loads without saving an empty selection", () => {
    localStorage.setItem(DEFAULT_TASK_GROUPS_EXPANDED_KEY, "true");
    structure.nodes = undefined;
    const { rerender, result } = renderHook(useGroups, { wrapper });

    expect(result.current.openGroupIds).toEqual([]);
    structure.nodes = nodes;
    rerender();
    expect(result.current.openGroupIds).toEqual(["group", "group.nested"]);
    expect(localStorage.getItem(openGroupsKey("example"))).toBeNull();
  });
  it("preserves a manual collapse after remounting", async () => {
    localStorage.setItem(DEFAULT_TASK_GROUPS_EXPANDED_KEY, "true");
    const { result, unmount } = renderHook(useGroups, { wrapper });

    act(() => result.current.toggleGroupId("group"));
    await waitFor(() => expect(result.current.openGroupIds).toEqual(["group.nested"]));
    unmount();
    const next = renderHook(useGroups, { wrapper });

    expect(next.result.current.openGroupIds).toEqual(["group.nested"]);
  });
  it("keeps collapse-all saved even with the expanded default", () => {
    localStorage.setItem(DEFAULT_TASK_GROUPS_EXPANDED_KEY, "true");
    const { result, unmount } = renderHook(useGroups, { wrapper });

    expect(result.current.openGroupIds).toEqual(["group", "group.nested"]);
    act(() => result.current.setOpenGroupIds([]));
    unmount();
    const next = renderHook(useGroups, { wrapper });

    expect(next.result.current.openGroupIds).toEqual([]);
  });
});
