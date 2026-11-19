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
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type {
  DAGRunResponse,
  DAGWithLatestDagRunsResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

import type { PluginActionsForDestination } from "src/hooks/usePluginActions";
import { Wrapper } from "src/utils/Wrapper";
import type { AppliesToContext } from "src/utils/pluginAppliesTo";

import { PluginActions } from "./PluginActions";
import { PluginActionsProvider } from "./PluginActionsContext";
import type { PluginActionEntity, PluginActionProps, PluginActionResponse } from "./types";

// Deferred so a test can inspect the row mid-load; resolved by `finishLoad` below.
let finishLoad: (() => void) | undefined;

// Stands in for the real bundle loader, including the side effect that matters: a successful
// load registers the component under the action's name on `globalThis`.
const loadPluginMock = vi.fn(
  async (reactApp: { bundle_url: string; name: string }) =>
    new Promise((resolve) => {
      finishLoad = () => {
        Reflect.set(globalThis, reactApp.name, () => null);
        resolve({ default: () => null });
      };
    }),
);

// Stands in for a bundle: the loader hands back a component, which the action renders
// directly, so what the test reads here is exactly what a real plugin would receive. The
// promise stays pending until `finishLoad`, which is what lets a test look at the row mid-load.
vi.mock("src/pages/ReactPlugin", () => ({
  loadPlugin: (reactApp: { bundle_url: string; name: string }) =>
    loadPluginMock(reactApp).then(() => ({
      default: ({ dagId, onClose, open, runId, taskId }: PluginActionProps) => {
        // A plugin component that throws while rendering.
        if (reactApp.name === "Image") {
          throw new Error("plugin component exploded");
        }

        return (
          <div
            data-dag-id={dagId}
            data-open={String(open)}
            data-run-id={runId}
            data-task-id={taskId}
            data-testid="plugin-surface"
          >
            <button onClick={onClose} type="button">
              dismiss
            </button>
          </div>
        );
      },
    })),
}));

vi.mock("src/context/colorMode", () => ({
  useColorMode: () => ({ colorMode: "light" }),
}));

const dagRow = {
  dag_id: "etl_sales",
  tags: [{ dag_display_name: "etl_sales", dag_id: "etl_sales", name: "prod" }],
} as unknown as DAGWithLatestDagRunsResponse;

const dagRun = {
  dag_id: "etl_sales",
  dag_run_id: "manual__2026-01-01",
  state: "success",
} as unknown as DAGRunResponse;

const taskInstance = {
  dag_id: "etl_sales",
  dag_run_id: "manual__2026-01-01",
  map_index: -1,
  task_id: "load",
} as unknown as TaskInstanceResponse;

const link = (overrides: Partial<PluginActionResponse> = {}): PluginActionResponse =>
  ({
    action_type: "link",
    destination: "dag_run",
    href: "https://example.com/runs/{DAG_ID}/{RUN_ID}",
    name: "Runbook",
    ...overrides,
  }) as PluginActionResponse;

const reactAction = (overrides: Partial<PluginActionResponse> = {}): PluginActionResponse =>
  ({
    action_type: "react",
    bundle_url: "https://example.com/action.js",
    destination: "dag_run",
    name: "Inspect",
    ...overrides,
  }) as PluginActionResponse;

const openSurface = async (name = "Inspect") => {
  fireEvent.click(screen.getByRole("button", { name }));
  finishLoad?.();
  await waitFor(() => expect(screen.getByTestId("plugin-surface")).toBeInTheDocument());
};

const expectSurfaceOpen = async (state: "false" | "true") =>
  waitFor(() => expect(screen.getByTestId("plugin-surface")).toHaveAttribute("data-open", state));

const renderActions = (
  actions: Array<PluginActionResponse>,
  entity: PluginActionEntity = { dagRun, destination: "dag_run" },
  routeContext: AppliesToContext = { isLoading: false },
) =>
  render(<PluginActions actions={actions} entity={entity} routeContext={routeContext} />, {
    wrapper: Wrapper,
  });

const noop = () => undefined;

const renderUnderProvider = (value: PluginActionsForDestination) =>
  render(
    <PluginActionsProvider value={value}>
      <PluginActions entity={{ dagRun, destination: "dag_run" }} />
    </PluginActionsProvider>,
    { wrapper: Wrapper },
  );

describe("PluginActions", () => {
  beforeEach(() => {
    loadPluginMock.mockClear();
    finishLoad = undefined;
    Reflect.deleteProperty(globalThis, "Inspect");
  });

  it("renders nothing when there are no actions", () => {
    const { container } = renderActions([]);

    expect(container).toBeEmptyDOMElement();
  });

  describe("link actions", () => {
    it("interpolates the entity into the href and opens a cross-origin target in a new tab", () => {
      renderActions([link()]);

      const anchor = screen.getByRole("link", { name: "Runbook" });

      expect(anchor).toHaveAttribute("href", "https://example.com/runs/etl_sales/manual__2026-01-01");
      expect(anchor).toHaveAttribute("target", "_blank");
      expect(anchor).toHaveAttribute("rel", "noopener noreferrer");
    });

    it("navigates in-app for a URL the router owns", () => {
      renderActions([link({ href: "/dags/{DAG_ID}/runs/{RUN_ID}" })]);

      const anchor = screen.getByRole("link", { name: "Runbook" });

      // A router link, not a document request: same tab, no rel needed.
      expect(anchor).toHaveAttribute("href", "/dags/etl_sales/runs/manual__2026-01-01");
      expect(anchor).not.toHaveAttribute("target", "_blank");
    });

    // Same origin, but not a page the router owns -- a plugin's own backend page. Following it
    // in place would drop the user out of the UI, so it counts as external.
    it("opens a same-origin path the router does not own in a new tab too", () => {
      renderActions([link({ href: "/plugin-api/catalog/{DAG_ID}" })]);

      const anchor = screen.getByRole("link", { name: "Runbook" });

      expect(anchor).toHaveAttribute("href", "/plugin-api/catalog/etl_sales");
      expect(anchor).toHaveAttribute("target", "_blank");
      expect(anchor).toHaveAttribute("rel", "noopener noreferrer");
    });

    it.each([
      ["a cross-origin URL", "https://example.com/x"],
      ["a same-origin page the router does not own", "/plugin-api/catalog"],
    ])("badges %s with a corner external indicator", (_label, href) => {
      renderActions([link({ href })]);

      expect(screen.getByTestId("plugin-action-external-indicator")).toBeInTheDocument();
    });

    it("leaves an in-app link unbadged, since nothing is left behind", () => {
      renderActions([link({ href: "/dags/{DAG_ID}/runs" })]);

      expect(screen.queryByTestId("plugin-action-external-indicator")).not.toBeInTheDocument();
    });

    it("keeps the indicator out of the accessible name", () => {
      renderActions([link()]);

      // The arrow is decorative; the action's name is what gets announced.
      expect(screen.getByRole("link", { name: "Runbook" })).toBeInTheDocument();
      expect(screen.getByTestId("plugin-action-external-indicator")).toHaveAttribute("aria-hidden", "true");
    });

    it("renders nothing for an href with an unsafe scheme", () => {
      const { container } = renderActions([link({ href: "javascript:alert(1)" })]);

      expect(container).toBeEmptyDOMElement();
    });
  });

  describe("React actions", () => {
    it("mounts nothing until the trigger is clicked", () => {
      renderActions([reactAction()]);

      expect(screen.queryByTestId("plugin-surface")).not.toBeInTheDocument();
      expect(loadPluginMock).not.toHaveBeenCalled();
    });

    // The bug this guards: a Suspense placeholder beside the button reflowed the action row
    // every time someone opened a surface.
    it("renders nothing beside the button while the bundle loads", () => {
      const { container } = renderActions([reactAction()]);
      const before = container.innerHTML;

      fireEvent.click(screen.getByRole("button", { name: "Inspect" }));

      expect(loadPluginMock).toHaveBeenCalledTimes(1);
      // Still loading: nothing has been added to the row, and no surface exists yet.
      expect(screen.queryByTestId("plugin-surface")).not.toBeInTheDocument();
      expect(container.querySelectorAll("button")).toHaveLength(before.split("<button").length - 1);
    });

    // Without a boundary this reaches the route's error element and replaces the whole page
    // the action was only meant to add a button to.
    it("keeps a component that throws inside its own slot", () => {
      // React logs the caught error itself; silenced so the run stays readable.
      const consoleError = vi.spyOn(console, "error").mockImplementation(noop);

      renderActions([reactAction({ name: "Image" }), reactAction({ name: "Fine" })]);

      fireEvent.click(screen.getByRole("button", { name: "Image" }));

      // The thrower rendered nothing, and its own button and its neighbour both survive.
      expect(screen.queryByTestId("plugin-surface")).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Image" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Fine" })).toBeInTheDocument();

      consoleError.mockRestore();
    });

    it("hands the plugin the clicked row's entity, not the surrounding route", async () => {
      renderActions([reactAction({ destination: "task_instance" })], {
        destination: "task_instance",
        taskInstance,
      });

      await openSurface();

      const surface = screen.getByTestId("plugin-surface");

      expect(surface).toHaveAttribute("data-dag-id", "etl_sales");
      expect(surface).toHaveAttribute("data-run-id", "manual__2026-01-01");
      expect(surface).toHaveAttribute("data-task-id", "load");
      expect(surface).toHaveAttribute("data-open", "true");
    });

    it("lets the plugin dismiss itself, and keeps it mounted afterwards", async () => {
      renderActions([reactAction()]);
      await openSurface();

      fireEvent.click(screen.getByRole("button", { name: "dismiss" }));

      // Still mounted, so it can animate out and keep its state -- just no longer open.
      expect(screen.getByTestId("plugin-surface")).toHaveAttribute("data-open", "false");
    });

    it("closes again when the trigger is clicked a second time", async () => {
      renderActions([reactAction()]);
      await openSurface();

      fireEvent.click(screen.getByRole("button", { name: "Inspect" }));

      await expectSurfaceOpen("false");
    });

    it("does not re-fetch the bundle once it is registered", async () => {
      renderActions([reactAction()]);
      await openSurface();

      fireEvent.click(screen.getByRole("button", { name: "Inspect" }));

      // The component is on `globalThis` now, so reopening is synchronous.
      expect(loadPluginMock).toHaveBeenCalledTimes(1);
    });
  });

  // The negative control for the remount this PR's context exists to prevent. When any action
  // is scoped, `routeContext` carries the page's polled queries, so a new object arrives on
  // every refresh tick. Reaching the cell through context means the component type is
  // unchanged and React updates in place; threading it through a column factory instead
  // produced a new type each tick, remounting the cell and closing any open surface.
  describe("stability across refreshes", () => {
    const scoped = [reactAction({ applies_to: { state: ["success"] } })];

    it("keeps the same button node when a new route context arrives", () => {
      const { rerender } = renderUnderProvider({
        actions: scoped,
        routeContext: { dagRun, isLoading: false },
      });
      const before = screen.getByRole("button", { name: "Inspect" });

      // A fresh object, as each poll produces.
      rerender(
        <PluginActionsProvider value={{ actions: [...scoped], routeContext: { dagRun, isLoading: false } }}>
          <PluginActions entity={{ dagRun, destination: "dag_run" }} />
        </PluginActionsProvider>,
      );

      expect(screen.getByRole("button", { name: "Inspect" })).toBe(before);
    });

    it("keeps an opened surface mounted across that refresh", async () => {
      const { rerender } = renderUnderProvider({
        actions: scoped,
        routeContext: { dagRun, isLoading: false },
      });

      await openSurface();
      const surface = screen.getByTestId("plugin-surface");

      rerender(
        <PluginActionsProvider value={{ actions: [...scoped], routeContext: { dagRun, isLoading: false } }}>
          <PluginActions entity={{ dagRun, destination: "dag_run" }} />
        </PluginActionsProvider>,
      );

      // Remounting would have dropped the loaded component and closed the surface.
      expect(screen.getByTestId("plugin-surface")).toBe(surface);
    });
  });

  describe("scoping", () => {
    it("shows a tag-scoped action on a Dags-list row, whose record has tags", () => {
      renderActions([link({ applies_to: { "dag.tags.name": ["prod"] }, destination: "dag" })], {
        dag: dagRow,
        destination: "dag",
      });

      expect(screen.getByRole("link", { name: "Runbook" })).toBeInTheDocument();
    });

    it("hides a tag-scoped action on a row whose Dag lacks the tag", () => {
      renderActions([link({ applies_to: { "dag.tags.name": ["finance"] }, destination: "dag" })], {
        dag: dagRow,
        destination: "dag",
      });

      expect(screen.queryByRole("link", { name: "Runbook" })).not.toBeInTheDocument();
    });

    // A Dag Run row carries no tags, so a path rooted at the Dag stays unevaluable there and
    // the action shows. Pinned so the documented leak on the global Dag Runs list changes
    // knowingly rather than by accident.
    it("shows a tag-scoped Dag Run action when no Dag record is available to judge it", () => {
      renderActions([link({ applies_to: { "dag.tags.name": ["finance"] } })]);

      expect(screen.getByRole("link", { name: "Runbook" })).toBeInTheDocument();
    });

    // The case the open field matcher exists for: the row record carries `state`, so this is
    // decided locally with no Dag lookup at all.
    it("matches a Dag Run row on its own state", () => {
      renderActions([link({ applies_to: { state: ["failed"] } })]);

      expect(screen.queryByRole("link", { name: "Runbook" })).not.toBeInTheDocument();

      cleanup();
      renderActions([link({ applies_to: { state: ["success"] } })]);

      expect(screen.getByRole("link", { name: "Runbook" })).toBeInTheDocument();
    });

    it("hides that same action once the route supplies the Dag and its tags disagree", () => {
      renderActions([link({ applies_to: { "dag.tags.name": ["finance"] } })], undefined, {
        dag: dagRow,
        isLoading: false,
      });

      expect(screen.queryByRole("link", { name: "Runbook" })).not.toBeInTheDocument();
    });

    it("keeps an action scoped to a different destination out", () => {
      renderActions([link({ destination: "dag" })]);

      expect(screen.queryByRole("link", { name: "Runbook" })).not.toBeInTheDocument();
    });

    it("withholds a scoped action while the route context is still loading", () => {
      renderActions([link({ applies_to: { "dag.dag_id": ["etl_sales"] } })], undefined, {
        isLoading: true,
      });

      expect(screen.queryByRole("link", { name: "Runbook" })).not.toBeInTheDocument();
    });
  });

  it("renders several actions of both kinds together", async () => {
    renderActions([link(), reactAction()]);

    expect(screen.getByRole("link", { name: "Runbook" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Inspect" }));
    finishLoad?.();

    await waitFor(() => expect(screen.getByTestId("plugin-surface")).toBeInTheDocument());
  });
});
