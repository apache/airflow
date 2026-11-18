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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { DagVersionResponse, TaskInstanceResponse } from "openapi/requests/types.gen";

import BulkClearTaskInstancesButton from "src/pages/TaskInstances/BulkClearTaskInstancesButton";

import {
  CLEAR_KEEP_TASK_STATE_KEY,
  CLEAR_PREVENT_RUNNING_TASK_KEY,
  CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
} from "src/constants/localStorage";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { ClearTaskInstanceButton } from "./ClearTaskInstanceButton";
import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

const DAG_ID = "test_dag";
const DAG_RUN_ID = "run_1";
const TASK_ID = "task_1";

const taskInstance: TaskInstanceResponse = {
  dag_display_name: "Test DAG",
  dag_id: DAG_ID,
  dag_run_id: DAG_RUN_ID,
  dag_version: null,
  duration: null,
  end_date: null,
  executor: null,
  executor_config: "{}",
  hostname: null,
  id: "test_task_instance",
  in_loop: false,
  logical_date: "2025-01-01T00:00:00Z",
  map_index: -1,
  max_tries: 0,
  note: null,
  operator: "EmptyOperator",
  operator_name: "EmptyOperator",
  pid: null,
  pool: "default_pool",
  pool_slots: 1,
  priority_weight: null,
  queue: null,
  queued_when: null,
  rendered_fields: undefined,
  rendered_map_index: null,
  run_after: "2025-01-01T00:00:00Z",
  scheduled_when: null,
  start_date: null,
  state: "success",
  task_display_name: "task_1",
  task_id: TASK_ID,
  trigger: null,
  triggerer_job: null,
  try_number: 1,
  unixname: null,
};

const affectedTasks = { task_instances: [taskInstance], total_entries: 1 };

const loopPass = (id: string, regionIndex: number): TaskInstanceResponse => ({
  ...taskInstance,
  id,
  region_id: "11111111-1111-4111-8111-111111111111",
  region_index: regionIndex,
  task_id: "body.work",
});

const loopMember: TaskInstanceResponse = {
  ...loopPass("loop-member", 1),
  in_loop: true,
};

// A code-only change advanced the Dag version's bundle in place; the run stays pinned to its bundle.
const latestVersion: DagVersionResponse = {
  bundle_name: "git-bundle",
  bundle_url: null,
  bundle_version: "latest-sha",
  created_at: "2025-01-02T00:00:00Z",
  dag_display_name: "Test DAG",
  dag_id: DAG_ID,
  id: "version-21",
  version_number: 21,
};

const mutateMock = vi.fn();
const dryRunsMock = vi.fn();
const confirmationDryRun = { isFetching: false };

vi.mock("src/queries/useClearTaskInstances", () => ({
  useClearTaskInstances: () => ({ isPending: false, mutate: mutateMock }),
}));

// Mocked directly (rather than via MSW) because it backs both the affected-tasks
// list in the main dialog and the confirmation dialog's running-task gate; a
// same-tick response for both keeps the gate from blocking on a real fetch.
vi.mock("src/queries/useClearTaskInstancesDryRun", () => ({
  useClearTaskInstancesDryRun: () => ({
    data: affectedTasks,
    isFetching: confirmationDryRun.isFetching,
    isPending: false,
  }),
  useClearTaskInstancesDryRuns: (args: unknown) => {
    dryRunsMock(args);

    return { data: affectedTasks, error: null, isFetching: false };
  },
}));

beforeEach(() => {
  server.use(
    http.get(`/api/v2/dags/${DAG_ID}/details`, () => HttpResponse.json({})),
    http.get(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}`, () => HttpResponse.json({ dag_versions: [] })),
  );
});
afterEach(() => {
  affectedTasks.task_instances = [taskInstance];
  confirmationDryRun.isFetching = false;
  mutateMock.mockClear();
  dryRunsMock.mockClear();
  localStorage.clear();
});

const serveBundleOnlyUpdate = (runLoaded: Promise<void> = Promise.resolve()) =>
  server.use(
    http.get(`/api/v2/dags/${DAG_ID}/details`, () =>
      HttpResponse.json({
        bundle_version: "latest-sha",
        latest_dag_version: latestVersion,
        rerun_with_latest_version: null,
      }),
    ),
    http.get(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}`, async () => {
      await runLoaded;

      return HttpResponse.json({ bundle_version: "historical-sha", dag_versions: [latestVersion] });
    }),
  );

const clickConfirm = async () => {
  const confirmButton = await screen.findByRole("button", { name: /modal\.confirm/iu });

  await waitFor(() => expect(confirmButton).toBeEnabled());
  fireEvent.click(confirmButton);
};

describe("ClearTaskInstanceDialog", () => {
  it("seeds the keep-task-state checkbox from the stored default and sends it on confirm", async () => {
    localStorage.setItem(CLEAR_KEEP_TASK_STATE_KEY, JSON.stringify(true));

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    const keepTaskStateCheckbox = screen.getByRole("checkbox", { name: /keepTaskState/iu });

    expect(keepTaskStateCheckbox).toBeChecked();

    await clickConfirm();

    await waitFor(() => expect(mutateMock).toHaveBeenCalled());

    const [{ requestBody }] = mutateMock.mock.calls[0] as [{ requestBody: { keep_task_state?: boolean } }];

    expect(requestBody.keep_task_state).toBe(true);
  });

  it("sends the clear once the running-task check resolves", async () => {
    const onClose = vi.fn();

    confirmationDryRun.isFetching = true;
    const { rerender } = render(
      <ClearTaskInstanceDialog onClose={onClose} open taskInstance={taskInstance} />,
      {
        wrapper: Wrapper,
      },
    );

    await clickConfirm();
    // Let the clear dialog finish closing while the running-task check is still in flight.
    await new Promise((resolve) => {
      setTimeout(resolve, 0);
    });
    confirmationDryRun.isFetching = false;
    rerender(<ClearTaskInstanceDialog onClose={onClose} open taskInstance={taskInstance} />);

    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    const [{ requestBody }] = mutateMock.mock.calls[0] as [{ requestBody: { dry_run?: boolean } }];

    expect(requestBody.dry_run).toBe(false);
  });

  it("warns instead of clearing a queued task when running tasks are protected", async () => {
    localStorage.setItem(CLEAR_PREVENT_RUNNING_TASK_KEY, JSON.stringify(true));
    affectedTasks.task_instances = [{ ...taskInstance, state: "queued" }];
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    await clickConfirm();

    expect(await screen.findByText(/confirmationDialog\.title/iu)).toBeVisible();
    expect(mutateMock).not.toHaveBeenCalled();
  });

  it("clears the kept loop passes by execution id when another pass is unticked", async () => {
    affectedTasks.task_instances = [loopPass("pass-0", 0), loopPass("pass-2", 2)];

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    const rows = await screen.findAllByRole("row");

    fireEvent.click(within(rows[1] as HTMLElement).getByRole("checkbox"));
    await clickConfirm();

    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    const [{ requestBody }] = mutateMock.mock.calls[0] as [
      { requestBody: { task_ids?: unknown; task_instance_ids?: Array<string> } },
    ];

    expect(requestBody.task_instance_ids).toEqual(["pass-2"]);
    expect(requestBody.task_ids).toBeUndefined();
  });

  it("keeps an unticked loop pass excluded when its retry comes back with a new execution id", async () => {
    affectedTasks.task_instances = [loopPass("pass-0", 0), loopPass("pass-2", 2)];

    const dialog = <ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />;
    const { rerender } = render(dialog, { wrapper: Wrapper });

    const rows = await screen.findAllByRole("row");

    fireEvent.click(within(rows[1] as HTMLElement).getByRole("checkbox"));

    affectedTasks.task_instances = [loopPass("pass-0-retried", 0), loopPass("pass-2", 2)];
    rerender(dialog);

    await clickConfirm();

    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    const [{ requestBody }] = mutateMock.mock.calls[0] as [
      { requestBody: { task_instance_ids?: Array<string> } },
    ];

    expect(requestBody.task_instance_ids).toEqual(["pass-2"]);
  });

  it.each([
    { inLoop: true, shown: true },
    { inLoop: false, shown: false },
  ])(
    "shows the later-iterations option only for a loop member (in_loop=$inLoop)",
    async ({ inLoop, shown }) => {
      render(
        <ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={{ ...loopMember, in_loop: inLoop }} />,
        {
          wrapper: Wrapper,
        },
      );

      expect(await screen.findByRole("checkbox", { name: /keepTaskState/iu })).toBeVisible();
      expect(screen.queryByRole("button", { name: /options\.laterIterations/iu }) !== null).toBe(shown);
    },
  );

  it("includes later iterations in the first dry run and leaves the saved defaults untouched", async () => {
    localStorage.setItem(CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY, JSON.stringify(["downstream"]));
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={loopMember} />, {
      wrapper: Wrapper,
    });

    fireEvent.click(await screen.findByRole("button", { name: /options\.laterIterations/iu }));

    expect(dryRunsMock.mock.calls[0]?.[0]).toMatchObject({
      requests: [{ requestBody: { include_later_loop_iterations: true } }],
    });
    expect(localStorage.getItem(CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY)).toBe(
      JSON.stringify(["downstream"]),
    );
  });

  it("clears a loop member by execution id with the later-iterations choice and never sends past or future", async () => {
    localStorage.setItem(
      CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
      JSON.stringify(["past", "future", "downstream"]),
    );
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={loopMember} />, {
      wrapper: Wrapper,
    });

    fireEvent.click(await screen.findByRole("button", { name: /options\.laterIterations/iu }));
    await clickConfirm();

    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    const [{ requestBody }] = mutateMock.mock.calls[0] as [{ requestBody: Record<string, unknown> }];

    expect(requestBody).toMatchObject({
      include_downstream: true,
      include_later_loop_iterations: false,
      task_instance_ids: ["loop-member"],
      whole_expansion_ids: [],
    });
    expect(requestBody).not.toHaveProperty("task_ids");
    expect(requestBody).not.toHaveProperty("include_past");
    expect(requestBody).not.toHaveProperty("include_future");
  });

  it("offers whole-expansion clearing for a mapped loop member and sends the member's id", async () => {
    const mappedMember = { ...loopMember, map_index: 2 };

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={mappedMember} />, {
      wrapper: Wrapper,
    });

    fireEvent.click(await screen.findByRole("button", { name: /options\.wholeExpansion/iu }));
    await clickConfirm();

    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    const [{ requestBody }] = mutateMock.mock.calls[0] as [{ requestBody: Record<string, unknown> }];

    expect(requestBody).toMatchObject({
      task_instance_ids: ["loop-member"],
      whole_expansion_ids: ["loop-member"],
    });
  });

  it("offers no whole-expansion option when the loop member is not mapped", async () => {
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={loopMember} />, {
      wrapper: Wrapper,
    });

    expect(await screen.findByRole("button", { name: /options\.laterIterations/iu })).toBeVisible();
    expect(screen.queryByRole("button", { name: /options\.wholeExpansion/iu })).not.toBeInTheDocument();
  });

  it("does not offer the exclusion checkboxes for a loop clear", async () => {
    affectedTasks.task_instances = [loopPass("pass-0", 0), loopPass("pass-2", 2)];

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={loopMember} />, {
      wrapper: Wrapper,
    });

    const rows = await screen.findAllByRole("row");

    expect(rows).toHaveLength(3);
    expect(within(rows[1] as HTMLElement).queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it("clears a selection by task and map index when none of it is in a loop", async () => {
    const other = { ...taskInstance, id: "other", map_index: 3, task_id: "task_2" };

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstances={[taskInstance, other]} />, {
      wrapper: Wrapper,
    });

    await clickConfirm();

    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    const [{ requestBody }] = mutateMock.mock.calls[0] as [{ requestBody: Record<string, unknown> }];

    expect(requestBody).toMatchObject({
      dag_run_id: DAG_RUN_ID,
      task_ids: [
        [TASK_ID, -1],
        ["task_2", 3],
      ],
    });
    expect(requestBody).not.toHaveProperty("task_instance_ids");
  });

  it("sends one run-scoped request per run when a selection spans runs", async () => {
    const otherRun = { ...loopMember, dag_run_id: "run_2", id: "other-run-member" };

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstances={[loopMember, otherRun]} />, {
      wrapper: Wrapper,
    });

    await clickConfirm();

    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(2));
    expect(
      mutateMock.mock.calls.map(([request]) => (request as { requestBody: unknown }).requestBody),
    ).toMatchObject([
      { dag_run_id: DAG_RUN_ID, task_instance_ids: ["loop-member"] },
      { dag_run_id: "run_2", task_instance_ids: ["other-run-member"] },
    ]);
    expect(dryRunsMock.mock.lastCall?.[0]).toMatchObject({
      requests: [
        { dagId: DAG_ID, requestBody: { dag_run_id: DAG_RUN_ID, task_instance_ids: ["loop-member"] } },
        { dagId: DAG_ID, requestBody: { dag_run_id: "run_2", task_instance_ids: ["other-run-member"] } },
      ],
    });
  });

  it("sends the note only when it was edited", async () => {
    const withNote = { ...loopMember, note: "existing note" };

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={withNote} />, {
      wrapper: Wrapper,
    });

    const note = await screen.findByRole("textbox", { hidden: true });

    expect(note).toHaveValue("existing note");
    await clickConfirm();
    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    expect((mutateMock.mock.calls[0] as [{ requestBody: { note?: string } }])[0].requestBody.note).toBe(
      undefined,
    );
  });

  it("sends the note once the user edits it", async () => {
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={loopMember} />, {
      wrapper: Wrapper,
    });

    fireEvent.change(await screen.findByRole("textbox", { hidden: true }), {
      target: { value: "new reason" },
    });
    await clickConfirm();
    await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
    expect((mutateMock.mock.calls[0] as [{ requestBody: { note?: string } }])[0].requestBody.note).toBe(
      "new reason",
    );
  });

  it("lists every affected loop pass, including later ones", async () => {
    affectedTasks.task_instances = [loopPass("pass-1", 1), loopPass("pass-2", 2)];

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={loopMember} />, {
      wrapper: Wrapper,
    });

    expect(await screen.findAllByText("body.work")).toHaveLength(2);
  });

  it.each([
    { loopIteration: { iteration: 3, loop_id: "body" }, shown: true },
    { loopIteration: undefined, shown: false },
  ])(
    "shows the iteration column in the affected tasks only for loop passes: $shown",
    async ({ loopIteration, shown }) => {
      affectedTasks.task_instances = [
        { ...loopPass("pass-3", 3), loop_iteration: loopIteration },
        { ...taskInstance, id: "outside-loop", task_id: "outside" },
      ];

      render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={loopMember} />, {
        wrapper: Wrapper,
      });

      expect(await screen.findByText("body.work")).toBeVisible();
      expect(screen.queryByRole("columnheader", { name: "taskInstance.iteration" }) !== null).toBe(shown);
      expect(screen.queryByText("3") !== null).toBe(shown);
    },
  );

  it.each([
    { expectedShown: true, version: { bundle_version: "bundle-1", id: "version-1", version_number: 1 } },
    { expectedShown: false, version: { bundle_version: "bundle-2", id: "version-2", version_number: 2 } },
  ])(
    "shows the run-on-latest option for an execution row on an outdated version: $expectedShown",
    async ({ expectedShown, version }) => {
      const answered = new Set<string>();

      server.use(
        http.get(`/api/v2/dags/${DAG_ID}/details`, () => {
          answered.add("details");

          return HttpResponse.json({
            bundle_version: "bundle-2",
            latest_dag_version: { version_number: 2 },
            rerun_with_latest_version: null,
          });
        }),
        http.get(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}`, () => {
          answered.add("run");

          return HttpResponse.json({ dag_versions: [version] });
        }),
      );
      const execution = {
        dag_id: DAG_ID,
        dag_run_id: DAG_RUN_ID,
        dag_version_id: version.id,
        id: "execution",
        in_loop: true,
        map_index: -1,
        region_index: 1,
        task_id: "body.work",
      };

      render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstances={[execution]} />, {
        wrapper: Wrapper,
      });

      expect(await screen.findByRole("checkbox", { name: /keepTaskState/iu })).toBeVisible();
      await waitFor(() => expect(answered.size).toBe(2));
      await new Promise((resolve) => {
        setTimeout(resolve, 50);
      });
      expect(screen.queryByRole("checkbox", { name: /runOnLatestVersion/iu }) !== null).toBe(expectedShown);
    },
  );

  it.each([false, true])(
    "offers the run-on-latest option when only the run's bundle is outdated and sends the choice: %s",
    async (runOnLatest) => {
      serveBundleOnlyUpdate();
      render(
        <ClearTaskInstanceDialog
          onClose={vi.fn()}
          open
          taskInstance={{ ...taskInstance, dag_version: latestVersion }}
        />,
        { wrapper: Wrapper },
      );

      const checkbox = await screen.findByRole("checkbox", { name: /runOnLatestVersion/iu });

      expect(checkbox).not.toBeChecked();
      if (runOnLatest) {
        fireEvent.click(checkbox);
        await waitFor(() => expect(checkbox).toBeChecked());
      }
      await clickConfirm();

      await waitFor(() => expect(mutateMock).toHaveBeenCalledTimes(1));
      expect((mutateMock.mock.calls[0] as [{ requestBody: unknown }])[0].requestBody).toMatchObject({
        dag_run_id: DAG_RUN_ID,
        dry_run: false,
        run_on_latest_version: runOnLatest,
      });
    },
  );

  it("offers the run-on-latest option when clearing all mapped instances on an outdated bundle", async () => {
    serveBundleOnlyUpdate();
    render(
      <ClearTaskInstanceDialog
        allMapped
        dagId={DAG_ID}
        dagRunId={DAG_RUN_ID}
        onClose={vi.fn()}
        open
        taskId={TASK_ID}
      />,
      { wrapper: Wrapper },
    );

    expect(await screen.findByRole("checkbox", { name: /runOnLatestVersion/iu })).toBeVisible();
  });

  it("keeps confirm disabled until the run's bundle has loaded", async () => {
    let loadRun: (() => void) | undefined;

    serveBundleOnlyUpdate(
      new Promise<void>((resolve) => {
        loadRun = resolve;
      }),
    );
    render(
      <ClearTaskInstanceDialog
        onClose={vi.fn()}
        open
        taskInstance={{ ...taskInstance, dag_version: latestVersion }}
      />,
      { wrapper: Wrapper },
    );

    const confirmButton = await screen.findByRole("button", { name: /modal\.confirm/iu });

    expect(confirmButton).toBeDisabled();
    expect(screen.queryByRole("checkbox", { name: /runOnLatestVersion/iu })).not.toBeInTheDocument();

    loadRun?.();

    expect(await screen.findByRole("checkbox", { name: /runOnLatestVersion/iu })).toBeVisible();
    await waitFor(() => expect(confirmButton).toBeEnabled());
  });

  it.each([
    {
      entry: "the task instance page",
      ui: (member: TaskInstanceResponse) => <ClearTaskInstanceButton taskInstance={member} />,
    },
    {
      entry: "a bulk selection",
      ui: (member: TaskInstanceResponse) => (
        <BulkClearTaskInstancesButton
          clearSelections={vi.fn()}
          selectedTaskInstances={[taskInstance, member]}
        />
      ),
    },
  ])("offers the loop options for a loop member cleared from $entry", async ({ ui }) => {
    render(ui(loopMember), { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: /clear\.button/iu }));

    expect(await screen.findByRole("button", { name: /options\.laterIterations/iu })).toBeVisible();
  });
});
