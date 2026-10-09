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
import { setupServer, type SetupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import BulkClearTaskInstancesButton from "src/pages/TaskInstances/BulkClearTaskInstancesButton";

import {
  CLEAR_KEEP_TASK_STATE_KEY,
  CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
} from "src/constants/localStorage";
import { handlers } from "src/mocks/handlers";
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

const mutateMock = vi.fn();
const dryRunsMock = vi.fn();

vi.mock("src/queries/useClearTaskInstances", () => ({
  useClearTaskInstances: () => ({ isPending: false, mutate: mutateMock }),
}));

// Mocked directly (rather than via MSW) because it backs both the affected-tasks
// list in the main dialog and the confirmation dialog's running-task gate; a
// same-tick response for both keeps the gate from blocking on a real fetch.
vi.mock("src/queries/useClearTaskInstancesDryRun", () => ({
  useClearTaskInstancesDryRun: () => ({ data: affectedTasks, isFetching: false, isPending: false }),
  useClearTaskInstancesDryRuns: (args: unknown) => {
    dryRunsMock(args);

    return { data: affectedTasks, error: null, isFetching: false };
  },
}));

let server: SetupServer;

beforeAll(() => {
  server = setupServer(
    ...handlers,
    http.get(`/api/v2/dags/${DAG_ID}/details`, () => HttpResponse.json({})),
    http.get(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}`, () => HttpResponse.json({ dag_versions: [] })),
  );
  server.listen({ onUnhandledFrame: "bypass" });
});
afterEach(() => {
  affectedTasks.task_instances = [taskInstance];
  mutateMock.mockClear();
  dryRunsMock.mockClear();
  server.resetHandlers();
  localStorage.clear();
});
afterAll(() => server.close());

describe("ClearTaskInstanceDialog", () => {
  it("seeds the keep-task-state checkbox from the stored default and sends it on confirm", async () => {
    localStorage.setItem(CLEAR_KEEP_TASK_STATE_KEY, JSON.stringify(true));

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    const keepTaskStateCheckbox = screen.getByRole("checkbox", { name: /keepTaskState/iu });

    expect(keepTaskStateCheckbox).toBeChecked();

    const confirmButton = await screen.findByRole("button", { name: /modal\.confirm/iu });

    fireEvent.click(confirmButton);

    await waitFor(() => expect(mutateMock).toHaveBeenCalled());

    const [{ requestBody }] = mutateMock.mock.calls[0] as [{ requestBody: { keep_task_state?: boolean } }];

    expect(requestBody.keep_task_state).toBe(true);
  });

  it("clears the kept loop passes by execution id when another pass is unticked", async () => {
    affectedTasks.task_instances = [loopPass("pass-0", 0), loopPass("pass-2", 2)];

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    const rows = await screen.findAllByRole("row");

    fireEvent.click(within(rows[1] as HTMLElement).getByRole("checkbox"));
    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));

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

    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));

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
      expect(screen.queryByRole("checkbox", { name: /execution\.clearLater/iu }) !== null).toBe(shown);
    },
  );

  it("clears a loop member by execution id with the later-iterations choice and never sends past or future", async () => {
    localStorage.setItem(
      CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
      JSON.stringify(["past", "future", "downstream"]),
    );
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={loopMember} />, {
      wrapper: Wrapper,
    });

    fireEvent.click(await screen.findByRole("checkbox", { name: /execution\.clearLater/iu }));
    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));

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

    fireEvent.click(await screen.findByRole("checkbox", { name: /execution\.clearWhole/iu }));
    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));

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

    expect(await screen.findByRole("checkbox", { name: /execution\.clearLater/iu })).toBeVisible();
    expect(screen.queryByRole("checkbox", { name: /execution\.clearWhole/iu })).not.toBeInTheDocument();
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

    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));

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

    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));

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
    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));
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
    fireEvent.click(await screen.findByRole("button", { name: /modal\.confirm/iu }));
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

    expect(await screen.findByRole("checkbox", { name: /execution\.clearLater/iu })).toBeVisible();
  });
});
