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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Link, MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { DagRunService, DagService, TaskInstanceService } from "openapi/requests";
import type {
  DAGDetailsResponse,
  DAGRunResponse,
  ExecutionCollectionResponse,
  ExecutionRegionResponse,
  ExecutionTaskResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import type * as Utils from "src/utils";
import { BaseWrapper } from "src/utils/Wrapper";

import commonTranslations from "../../../public/i18n/locales/en/common.json";
import dagTranslations from "../../../public/i18n/locales/en/dag.json";
import { Execution } from "./Execution";

vi.mock("src/router", () => ({ taskInstanceRoutes: [] }));

// The running-task gate opens a second dialog that jsdom dismisses before its dry run settles.
vi.mock("src/components/Clear/TaskInstance/ClearTaskInstanceConfirmationDialog", async () => {
  const { useEffect } = await import("react");

  const ConfirmImmediately = ({
    onClose,
    onConfirm,
  }: {
    readonly onClose: () => void;
    readonly onConfirm?: () => void;
  }) => {
    useEffect(() => {
      onConfirm?.();
      onClose();
    }, [onClose, onConfirm]);

    return undefined;
  };

  return { default: ConfirmImmediately };
});

beforeAll(() => {
  i18n.addResourceBundle("en", "dag", dagTranslations, true, true);
  i18n.addResourceBundle("en", "common", commonTranslations, true, true);
});

const refresh = vi.hoisted<{ interval: number | false }>(() => ({ interval: false }));

vi.mock("src/utils", async (importOriginal) => ({
  ...(await importOriginal<typeof Utils>()),
  useAutoRefresh: () => refresh.interval,
}));

const ROOT = "11111111-1111-4111-8111-111111111111";
const FORK = "22222222-2222-4222-8222-222222222222";
const MAPPING = "33333333-3333-4333-8333-333333333333";

const region = (id: string, changes: Partial<ExecutionRegionResponse> = {}): ExecutionRegionResponse => ({
  forked_from_region_id: null,
  id,
  node_id: "body",
  parent_region_id: null,
  parent_region_index: null,
  resumes_from_index: 0,
  ...changes,
});

const task = (id: string, changes: Partial<ExecutionTaskResponse> = {}): ExecutionTaskResponse => ({
  dag_id: "dag",
  dag_run_id: "run",
  dag_version_id: null,
  duration: null,
  end_date: null,
  id,
  map_index: -1,
  operator: "EmptyOperator",
  region_id: ROOT,
  region_index: 0,
  start_date: null,
  state: "success",
  task_display_name: id,
  task_id: id,
  try_number: 1,
  ...changes,
});

const affectedTask = {
  dag_id: "dag",
  dag_run_id: "run",
  id: "work",
  map_index: -1,
  region_id: ROOT,
  region_index: 0,
  state: "success",
  task_id: "work",
} as TaskInstanceResponse;

const renderExecution = (search = "") =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[`/dags/dag/runs/run/execution${search}`]}>
        <Routes>
          <Route element={<Execution />} path="/dags/:dagId/runs/:runId/execution" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

afterEach(() => {
  vi.restoreAllMocks();
  refresh.interval = false;
});

const mockRun = (state: DAGRunResponse["state"]) =>
  vi.spyOn(DagRunService, "getDagRun").mockResolvedValue({
    dag_id: "dag",
    dag_versions: [{ id: "version", version_number: 1 }],
    state,
  } as DAGRunResponse);

const sentRealClear = (clear: { mock: { calls: Array<Array<unknown>> } }) =>
  (clear.mock.calls as Array<[{ requestBody: { dry_run?: boolean } }]>).some(
    ([request]) => request.requestBody.dry_run === false,
  );

describe("Run Execution", () => {
  it("resets selected UUIDs when navigating to another run", async () => {
    vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [region(ROOT)],
      task_instances: [task("work")],
      total_entries: 1,
    });
    render(
      <BaseWrapper>
        <MemoryRouter initialEntries={["/dags/dag/runs/first/execution"]}>
          <Link to="/dags/dag/runs/second/execution">Second run</Link>
          <Routes>
            <Route element={<Execution />} path="/dags/:dagId/runs/:runId/execution" />
          </Routes>
        </MemoryRouter>
      </BaseWrapper>,
    );
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select work" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("link", { name: "Second run" }));
    expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeDisabled();
  });
  it("clears selected UUIDs separately when public task coordinates collide", async () => {
    vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [region(ROOT), region(FORK)],
      task_instances: [
        task("first", { task_id: "work" }),
        task("second", { region_id: FORK, region_index: 1, task_id: "work" }),
      ],
      total_entries: 2,
    });
    const clear = vi
      .spyOn(TaskInstanceService, "postClearTaskInstances")
      .mockResolvedValue({ task_instances: [], total_entries: 0 });

    renderExecution();
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select first" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Select second" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));
    await waitFor(() =>
      expect(clear.mock.lastCall?.[0].requestBody).toMatchObject({
        dry_run: true,
        task_instance_ids: ["first", "second"],
      }),
    );
  });
  it("keeps running branches and retained regions visible beside newer loop passes", async () => {
    const response: ExecutionCollectionResponse = {
      regions: [region(ROOT), region(FORK, { forked_from_region_id: ROOT, resumes_from_index: 2 })],
      task_instances: [
        task("early", { state: "running" }),
        task("retained", { region_index: 3 }),
        task("gate", { operator: "LoopGateOperator", region_id: FORK, region_index: 2 }),
      ],
      total_entries: 3,
    };

    vi.spyOn(DagRunService, "getExecution").mockResolvedValue(response);
    renderExecution();

    expect(await screen.findByRole("heading", { name: "body · Iteration 0" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "body · Iteration 2" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "body · Iteration 3" })).toBeVisible();
    const gate = screen.getByRole("link", { name: "gate" });

    expect(gate).toHaveAttribute("href", expect.stringContaining(`region_id=${FORK}`));
    expect(gate).toHaveAttribute("href", expect.stringContaining("region_index=2"));
    expect(gate).toHaveAttribute("href", expect.stringContaining("try_number=1"));
    expect(screen.getByRole("link", { name: "early" })).toBeVisible();
    expect(screen.queryByRole("heading", { name: "body · Iteration 4" })).not.toBeInTheDocument();
  });

  it.each(["body.map", "body.mapped.member"])(
    "expands %s positions within loop iteration 3 and preserves skipped empty mapping",
    async (taskId) => {
      vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
        regions: [
          region(ROOT),
          region(MAPPING, { node_id: taskId, parent_region_id: ROOT, parent_region_index: 3 }),
          region("empty-mapping", { node_id: "empty", parent_region_id: ROOT, parent_region_index: 3 }),
        ],
        task_instances: [
          task("map-zero", {
            map_index: 0,
            region_id: MAPPING,
            region_index: 0,
            task_display_name: "map",
            task_id: taskId,
          }),
          task("map-one", {
            map_index: 1,
            region_id: MAPPING,
            region_index: 1,
            task_display_name: "map",
            task_id: taskId,
          }),
          task("empty", { region_id: "empty-mapping", region_index: -1, state: "skipped" }),
        ],
        total_entries: 3,
      });
      renderExecution();

      fireEvent.click(await screen.findByRole("button", { name: "map (2 mapped tasks on this page)" }));
      expect(screen.getByRole("link", { name: "map [0]" })).toHaveAttribute(
        "href",
        expect.stringContaining("/mapped/0/logs"),
      );
      expect(screen.getByRole("link", { name: "map [1]" })).toHaveAttribute(
        "href",
        expect.stringContaining("region_index=1"),
      );
      expect(screen.getByRole("link", { name: "empty" })).toBeVisible();
      expect(screen.getByRole("heading", { name: "body · Iteration 3" })).toBeVisible();
    },
  );

  it("pages using the API total without presenting a page as the complete loop", async () => {
    const fetch = vi
      .spyOn(DagRunService, "getExecution")
      .mockResolvedValueOnce({
        regions: [region(ROOT)],
        task_instances: [task("first")],
        total_entries: 101,
      })
      .mockResolvedValueOnce({
        regions: [region(ROOT)],
        task_instances: [task("second")],
        total_entries: 101,
      });

    renderExecution();
    expect(await screen.findByText("Showing 1–1 of 101 executions")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: /next page/iu }));
    await waitFor(() =>
      expect(fetch).toHaveBeenLastCalledWith(expect.objectContaining({ limit: 100, offset: 100 })),
    );
    expect(await screen.findByRole("link", { name: "second" })).toBeVisible();
    expect(screen.queryByRole("link", { name: "first" })).not.toBeInTheDocument();
    expect(screen.getByText("Showing 101–101 of 101 executions")).toBeVisible();
  });

  it("translates the state badge, including the no-state fallback", async () => {
    vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [region(ROOT)],
      task_instances: [task("done"), task("unstarted", { state: null })],
      total_entries: 2,
    });
    renderExecution();

    expect(await screen.findByText("Success")).toBeVisible();
    expect(screen.getByText("No Status")).toBeVisible();
  });

  it("shows an empty state instead of a zero range for an empty run", async () => {
    vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [],
      task_instances: [],
      total_entries: 0,
    });
    renderExecution();

    expect(await screen.findByText("No executions in this run.")).toBeVisible();
    expect(screen.queryByText(/Showing/u)).not.toBeInTheDocument();
  });

  it("clamps an offset past the end of the live set to the last page", async () => {
    const fetch = vi
      .spyOn(DagRunService, "getExecution")
      .mockResolvedValueOnce({ regions: [region(ROOT)], task_instances: [], total_entries: 101 })
      .mockResolvedValue({ regions: [region(ROOT)], task_instances: [task("last")], total_entries: 101 });

    renderExecution("?execution_offset=500");

    expect(await screen.findByRole("link", { name: "last" })).toBeVisible();
    expect(fetch).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 100 }));
  });

  it("keeps polling a pending run but not a finished one", async () => {
    refresh.interval = 20;
    const fetch = vi
      .spyOn(DagRunService, "getExecution")
      .mockResolvedValue({ regions: [region(ROOT)], task_instances: [task("work")], total_entries: 1 });

    mockRun("success");
    const finished = renderExecution();

    await screen.findByRole("link", { name: "work" });
    await new Promise((resolve) => {
      setTimeout(resolve, 200);
    });
    expect(fetch).toHaveBeenCalledTimes(1);
    finished.unmount();
    fetch.mockClear();

    mockRun("running");
    renderExecution();
    await waitFor(() => expect(fetch.mock.calls.length).toBeGreaterThan(2));
  });

  it("keeps polling a finished run until its last pending task has settled", async () => {
    refresh.interval = 20;
    const fetch = vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [region(ROOT)],
      task_instances: [task("work", { state: "running" })],
      total_entries: 1,
    });

    mockRun("success");
    renderExecution();

    await screen.findByRole("link", { name: "work" });
    await waitFor(() => expect(fetch.mock.calls.length).toBeGreaterThan(2));
  });

  it("keeps the selection when the clear dialog is cancelled and drops it once the clear succeeds", async () => {
    vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [region(ROOT)],
      task_instances: [task("work")],
      total_entries: 1,
    });
    const clear = vi
      .spyOn(TaskInstanceService, "postClearTaskInstances")
      .mockResolvedValue({ task_instances: [affectedTask], total_entries: 1 });

    vi.spyOn(DagService, "getDagDetails").mockResolvedValue({ dag_id: "dag" } as DAGDetailsResponse);
    mockRun("success");
    renderExecution();
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select work" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));
    fireEvent.click(await screen.findByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Cancel" })).not.toBeInTheDocument());
    expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled();

    fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));
    await screen.findByRole("button", { name: "Cancel" });
    const confirm = () => screen.getByRole("button", { name: "Confirm" });

    await waitFor(() => expect(confirm()).toBeEnabled());
    fireEvent.click(confirm());
    await waitFor(() => expect(sentRealClear(clear)).toBe(true));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeDisabled(),
    );
  });

  it("offers the loop options for a selected loop execution and addresses it by id", async () => {
    vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [region(ROOT)],
      task_instances: [task("work")],
      total_entries: 1,
    });
    const clear = vi
      .spyOn(TaskInstanceService, "postClearTaskInstances")
      .mockResolvedValue({ task_instances: [affectedTask], total_entries: 1 });

    renderExecution();
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select work" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));

    expect(await screen.findByRole("checkbox", { name: "Clear later loop iterations" })).toBeChecked();
    await waitFor(() =>
      expect(clear.mock.lastCall?.[0].requestBody).toMatchObject({
        include_later_loop_iterations: true,
        task_instance_ids: ["work"],
      }),
    );
  });

  it("clears a plain mapped execution by task and map index without loop options", async () => {
    vi.spyOn(DagRunService, "getExecution").mockResolvedValue({
      regions: [region(MAPPING, { node_id: "plain" })],
      task_instances: [
        task("plain-0", { map_index: 0, region_id: MAPPING, region_index: 0, task_id: "plain" }),
      ],
      total_entries: 1,
    });
    const clear = vi
      .spyOn(TaskInstanceService, "postClearTaskInstances")
      .mockResolvedValue({ task_instances: [affectedTask], total_entries: 1 });

    renderExecution();
    fireEvent.click(await screen.findByRole("button", { name: "plain-0 (1 mapped task on this page)" }));
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select plain-0" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
    );
    fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));

    await waitFor(() =>
      expect(clear.mock.lastCall?.[0].requestBody).toMatchObject({ task_ids: [["plain", 0]] }),
    );
    expect(clear.mock.lastCall?.[0].requestBody).not.toHaveProperty("task_instance_ids");
    expect(screen.queryByRole("checkbox", { name: "Clear later loop iterations" })).not.toBeInTheDocument();
  });

  it("selects the current execution once a selected one has been retried", async () => {
    refresh.interval = 20;
    vi.spyOn(DagRunService, "getExecution")
      .mockResolvedValueOnce({
        regions: [region(ROOT)],
        task_instances: [task("old-id", { state: "running", task_id: "work" })],
        total_entries: 1,
      })
      .mockResolvedValue({
        regions: [region(ROOT)],
        task_instances: [task("new-id", { state: "running", task_id: "work" })],
        total_entries: 1,
      });
    const clear = vi
      .spyOn(TaskInstanceService, "postClearTaskInstances")
      .mockResolvedValue({ task_instances: [], total_entries: 1 });

    mockRun("running");
    renderExecution();
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select old-id" }));
    await waitFor(() => expect(screen.getByRole("checkbox", { name: "Select new-id" })).toBeChecked());
    fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));

    await waitFor(() =>
      expect(clear.mock.lastCall?.[0].requestBody).toMatchObject({ task_instance_ids: ["new-id"] }),
    );
  });

  it("keeps a selection made on another page when this page does not list it", async () => {
    vi.spyOn(DagRunService, "getExecution")
      .mockResolvedValueOnce({ regions: [region(ROOT)], task_instances: [task("first")], total_entries: 101 })
      .mockResolvedValue({ regions: [region(ROOT)], task_instances: [task("second")], total_entries: 101 });

    renderExecution();
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select first" }));
    fireEvent.click(screen.getByRole("button", { name: /next page/iu }));
    expect(await screen.findByRole("checkbox", { name: "Select second" })).not.toBeChecked();

    expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled();
  });
});
