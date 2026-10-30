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
import { describe, expect, it } from "vitest";

import { useRowSelection } from "./useRowSelection";

const firstItem = { id: "first", state: "queued" };
const secondItem = { id: "second", state: "success" };
const getKey = (item: typeof firstItem) => item.id;

describe("useRowSelection retained items", () => {
  it.each(["row", "page"] as const)("retains items selected by %s across pages", (selection) => {
    const { rerender, result } = renderHook(({ data }) => useRowSelection({ data, getKey }), {
      initialProps: { data: [firstItem] },
    });
    const select = (key: string, checked: boolean) => {
      if (selection === "row") {
        result.current.handleRowSelect(key, checked);
      } else {
        result.current.handleSelectAll(checked);
      }
    };

    act(() => select(firstItem.id, true));
    rerender({ data: [secondItem] });
    act(() => select(secondItem.id, true));
    expect(result.current.selectedItems).toEqual([firstItem, secondItem]);

    act(() => select(secondItem.id, false));
    expect(result.current.selectedItems).toEqual([firstItem]);
  });

  it.each(["clearSelections", "deselectKeys"] as const)("removes retained items with %s", (action) => {
    const { rerender, result } = renderHook(({ data }) => useRowSelection({ data, getKey }), {
      initialProps: { data: [firstItem] },
    });

    act(() => result.current.handleRowSelect(firstItem.id, true));
    rerender({ data: [secondItem] });
    act(() => {
      if (action === "clearSelections") {
        result.current.clearSelections();
      } else {
        result.current.deselectKeys([firstItem.id]);
      }
    });
    expect(result.current.selectedItems).toEqual([]);
  });

  it("uses refreshed data for selected items on the current page", () => {
    const { rerender, result } = renderHook(({ data }) => useRowSelection({ data, getKey }), {
      initialProps: { data: [firstItem] },
    });

    act(() => result.current.handleRowSelect(firstItem.id, true));
    const updatedItem = { ...firstItem, state: "success" };

    rerender({ data: [updatedItem] });
    expect(result.current.selectedItems).toEqual([updatedItem]);
  });

  it("does not retain an item that is absent from the current data", () => {
    const { result } = renderHook(() => useRowSelection({ data: [firstItem], getKey }));

    act(() => result.current.handleRowSelect("missing", true));
    expect(result.current.selectedItems).toEqual([]);
    expect(result.current.selectedRows.size).toBe(0);
  });
});
