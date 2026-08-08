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
import { MemoryRouter, useNavigate } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";

import {
  clearGridScrollOffsets,
  extractDagIdFromPath,
  readGridScrollOffset,
  saveGridScrollOffset,
  useGridScrollRestoration,
  useResetGridScrollOnLeave,
} from "./useGridScrollRestoration";

/** Minimal stand-in for the scroll container: a settable scrollTop plus event wiring. */
const makeScrollElement = () => {
  const listeners: Array<() => void> = [];

  return {
    addEventListener: (_type: string, callback: () => void) => listeners.push(callback),
    dispatchScroll() {
      listeners.forEach((callback) => callback());
    },
    get listenerCount() {
      return listeners.length;
    },
    removeEventListener: (_type: string, callback: () => void) => {
      const index = listeners.indexOf(callback);

      if (index !== -1) {
        listeners.splice(index, 1);
      }
    },
    scrollTop: 0,
  };
};

type FakeElement = ReturnType<typeof makeScrollElement>;

const renderRestoration = (dagId: string, element: FakeElement | null, rowCount: number) =>
  renderHook(
    (props: { count: number; id: string }) =>
      useGridScrollRestoration({
        dagId: props.id,
        getScrollElement: () => element as unknown as HTMLElement | null,
        rowCount: props.count,
      }),
    { initialProps: { count: rowCount, id: dagId } },
  );

const renderReset = (path: string) =>
  renderHook(
    () => {
      useResetGridScrollOnLeave();

      return useNavigate();
    },
    {
      wrapper: ({ children }: PropsWithChildren) => (
        <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>
      ),
    },
  );

// The offset store is module scope, so it outlives each test.
beforeEach(clearGridScrollOffsets);

describe("extractDagIdFromPath", () => {
  it.each<[string, string | undefined]>([
    ["/dags/my_dag", "my_dag"],
    ["/dags/my_dag/runs/run_1", "my_dag"],
    ["/dags/my_dag/runs/run_1/tasks/task_1", "my_dag"],
    ["/dags/other_dag", "other_dag"],
    ["/dags", undefined],
    ["/dags/", undefined],
    ["/", undefined],
    ["/assets", undefined],
  ])("returns %s → %s", (pathname, expected) => {
    expect(extractDagIdFromPath(pathname)).toBe(expected);
  });
});

describe("clearGridScrollOffsets", () => {
  it("drops every saved offset", () => {
    saveGridScrollOffset("dag_clear_a", 100);
    saveGridScrollOffset("dag_clear_b", 200);

    clearGridScrollOffsets();

    expect(readGridScrollOffset("dag_clear_a")).toBeUndefined();
    expect(readGridScrollOffset("dag_clear_b")).toBeUndefined();
  });
});

// BaseLayout is the root route element and never remounts, so a pathname change is the only
// thing that fires the reset in production.
describe("useResetGridScrollOnLeave", () => {
  it("keeps saved offsets when navigating between pages of the same Dag", async () => {
    const { result } = renderReset("/dags/dag_within");

    saveGridScrollOffset("dag_within", 300);
    await act(async () => result.current("/dags/dag_within/runs/run_1/tasks/task_1"));

    expect(readGridScrollOffset("dag_within")).toBe(300);
  });

  it("clears saved offsets when navigating out of the Dag detail area", async () => {
    const { result } = renderReset("/dags/dag_nav/tasks/task_1");

    saveGridScrollOffset("dag_nav", 300);
    await act(async () => result.current("/dags"));

    expect(readGridScrollOffset("dag_nav")).toBeUndefined();
  });

  it("clears saved offsets when switching to another Dag", async () => {
    const { result } = renderReset("/dags/dag_from");

    saveGridScrollOffset("dag_from", 300);
    await act(async () => result.current("/dags/dag_to"));

    expect(readGridScrollOffset("dag_from")).toBeUndefined();
  });

  it("does not remember the position after visiting another Dag and coming back", async () => {
    const { result } = renderReset("/dags/dag_round_trip");

    saveGridScrollOffset("dag_round_trip", 300);
    await act(async () => result.current("/dags/dag_elsewhere"));
    await act(async () => result.current("/dags/dag_round_trip"));

    expect(readGridScrollOffset("dag_round_trip")).toBeUndefined();
  });
});

describe("saveGridScrollOffset / readGridScrollOffset", () => {
  it("round-trips an offset for a dagId", () => {
    saveGridScrollOffset("dag_roundtrip", 250);

    expect(readGridScrollOffset("dag_roundtrip")).toBe(250);
  });

  it("ignores an empty dagId", () => {
    saveGridScrollOffset("", 999);

    expect(readGridScrollOffset("")).toBeUndefined();
  });

  it("returns undefined for a dagId that was never saved", () => {
    expect(readGridScrollOffset("dag_never_seen")).toBeUndefined();
  });
});

describe("useGridScrollRestoration", () => {
  it("saves scrollTop to the store on every scroll event", () => {
    const element = makeScrollElement();

    renderRestoration("dag_save", element, 30);

    element.scrollTop = 480;
    element.dispatchScroll();

    expect(readGridScrollOffset("dag_save")).toBe(480);
  });

  it("restores the saved offset once the grid has rows", () => {
    saveGridScrollOffset("dag_restore", 320);
    const element = makeScrollElement();

    renderRestoration("dag_restore", element, 30);

    expect(element.scrollTop).toBe(320);
  });

  it("waits for rows before restoring, then restores when they appear", () => {
    saveGridScrollOffset("dag_deferred", 300);
    const element = makeScrollElement();

    const { rerender } = renderRestoration("dag_deferred", element, 0);

    expect(element.scrollTop).toBe(0);

    rerender({ count: 30, id: "dag_deferred" });

    expect(element.scrollTop).toBe(300);
  });

  it("restores only once so later user scrolling is not clobbered", () => {
    saveGridScrollOffset("dag_once", 300);
    const element = makeScrollElement();

    const { rerender } = renderRestoration("dag_once", element, 30);

    expect(element.scrollTop).toBe(300);

    // No scroll event on purpose: the store must keep 300 so a second restore would show up as
    // 300. Dispatching here would save 50 and make the assertion pass either way.
    element.scrollTop = 50;
    rerender({ count: 40, id: "dag_once" });

    expect(element.scrollTop).toBe(50);
  });

  it("does not touch scrollTop when there is no saved offset", () => {
    const element = makeScrollElement();

    element.scrollTop = 17;
    renderRestoration("dag_no_saved", element, 30);

    expect(element.scrollTop).toBe(17);
  });

  it("does not restore a zero offset", () => {
    saveGridScrollOffset("dag_zero", 0);
    const element = makeScrollElement();

    element.scrollTop = 42;
    renderRestoration("dag_zero", element, 30);

    expect(element.scrollTop).toBe(42);
  });

  it("removes the scroll listener on unmount", () => {
    const element = makeScrollElement();

    const { unmount } = renderRestoration("dag_cleanup", element, 30);

    expect(element.listenerCount).toBe(1);

    unmount();

    expect(element.listenerCount).toBe(0);
  });

  it("neither saves nor restores when the scroll element is missing", () => {
    saveGridScrollOffset("dag_null_el", 300);

    expect(() => renderRestoration("dag_null_el", null, 30)).not.toThrow();
    expect(readGridScrollOffset("dag_null_el")).toBe(300);
  });

  it("does not attach a scroll listener when the dagId is empty", () => {
    const element = makeScrollElement();

    renderRestoration("", element, 30);

    expect(element.listenerCount).toBe(0);
  });
});
