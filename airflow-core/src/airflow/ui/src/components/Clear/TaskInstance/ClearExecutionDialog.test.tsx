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
import { afterEach, beforeAll, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests";

import {
  CLEAR_KEEP_TASK_STATE_KEY,
  CLEAR_PREVENT_RUNNING_TASK_KEY,
  CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
} from "src/constants/localStorage";
import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import dagTranslations from "../../../../public/i18n/locales/en/dag.json";
import dagsTranslations from "../../../../public/i18n/locales/en/dags.json";
import { ClearExecutionDialog } from "./ClearExecutionDialog";

beforeAll(() => {
  i18n.addResourceBundle("en", "dag", dagTranslations, true, true);
  i18n.addResourceBundle("en", "dags", dagsTranslations, true, true);
});
afterEach(() => {
  vi.restoreAllMocks();
  localStorage.clear();
});

const execution = {
  id: "selected-uuid",
  map_index: -1,
  note: "existing note",
  region_id: "11111111-1111-1111-1111-111111111111",
  region_index: 1,
  task_display_name: "work",
  task_id: "body.work",
};

const findClearRequest = (clear: { mock: { calls: Array<Array<unknown>> } }) =>
  (clear.mock.calls as Array<[{ requestBody: { dry_run?: boolean; note?: string | null } }]>)
    .map(([request]) => request)
    .find((request) => request.requestBody.dry_run === false);

it("keeps stale UUID scope visible when the clear preview rejects an archived execution", async () => {
  const clear = vi
    .spyOn(TaskInstanceService, "postClearTaskInstances")
    .mockRejectedValue({ body: { detail: "Selected execution was archived" }, status: 409 });
  const onClose = vi.fn();

  render(
    <ClearExecutionDialog
      dagId="dag"
      executions={[
        {
          id: "archived-uuid",
          map_index: -1,
          region_id: "11111111-1111-1111-1111-111111111111",
          region_index: 3,
          task_display_name: "work",
          task_id: "body.work",
        },
      ]}
      onClose={onClose}
      open
      runId="run"
    />,
    { wrapper: Wrapper },
  );
  expect(await screen.findByText("Selected execution was archived")).toBeVisible();
  expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeDisabled();
  expect(clear).toHaveBeenCalledTimes(1);
  expect(clear.mock.lastCall?.[0].requestBody).toMatchObject({
    dry_run: true,
    task_instance_ids: ["archived-uuid"],
  });
  expect(onClose).not.toHaveBeenCalled();
});

it("previews and clears UUID seeds with separate whole-expansion and later-loop intent", async () => {
  const clear = vi
    .spyOn(TaskInstanceService, "postClearTaskInstances")
    .mockResolvedValue({ task_instances: [], total_entries: 0 });

  render(
    <ClearExecutionDialog
      dagId="dag"
      executions={[
        {
          id: "selected-uuid",
          map_index: 1,
          region_id: "11111111-1111-1111-1111-111111111111",
          region_index: 1,
          task_display_name: "mapped",
          task_id: "body.mapped",
        },
      ]}
      onClose={vi.fn()}
      open
      runId="run"
    />,
    { wrapper: Wrapper },
  );

  await waitFor(() =>
    expect(clear.mock.lastCall?.[0]).toMatchObject({
      dagId: "dag",
      requestBody: {
        dag_run_id: "run",
        dry_run: true,
        include_downstream: true,
        include_later_loop_iterations: true,
        only_failed: false,
        task_instance_ids: ["selected-uuid"],
        whole_expansion_ids: [],
      },
    }),
  );
  fireEvent.click(screen.getByLabelText("Clear whole mapped expansions"));
  fireEvent.click(screen.getByLabelText("Clear later loop iterations"));
  await waitFor(() =>
    expect(clear.mock.lastCall?.[0]).toMatchObject({
      dagId: "dag",
      requestBody: {
        dry_run: true,
        include_later_loop_iterations: false,
        whole_expansion_ids: ["selected-uuid"],
      },
    }),
  );
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
  );
  fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));
  await waitFor(() =>
    expect(
      clear.mock.calls.map(([request]) => request).find((request) => request.requestBody.dry_run === false),
    ).toMatchObject({
      dagId: "dag",
      requestBody: {
        dry_run: false,
        include_downstream: true,
        include_later_loop_iterations: false,
        task_instance_ids: ["selected-uuid"],
        whole_expansion_ids: ["selected-uuid"],
      },
    }),
  );
});

it("sends the saved clear defaults in the dry run and the real request", async () => {
  localStorage.setItem(CLEAR_PREVENT_RUNNING_TASK_KEY, JSON.stringify(true));
  localStorage.setItem(CLEAR_KEEP_TASK_STATE_KEY, JSON.stringify(true));
  localStorage.setItem(CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY, JSON.stringify([]));
  const clear = vi
    .spyOn(TaskInstanceService, "postClearTaskInstances")
    .mockResolvedValue({ task_instances: [], total_entries: 1 });

  render(<ClearExecutionDialog dagId="dag" executions={[execution]} onClose={vi.fn()} open runId="run" />, {
    wrapper: Wrapper,
  });

  await waitFor(() =>
    expect(clear.mock.lastCall?.[0].requestBody).toMatchObject({
      dry_run: true,
      include_downstream: false,
      keep_task_state: true,
      prevent_running_task: true,
    }),
  );
  expect(await screen.findByText("1 execution will be cleared.")).toBeVisible();
  expect(screen.getByLabelText("Prevent rerun if task is running")).toBeChecked();
  expect(screen.getByLabelText("Keep task state and resume")).toBeChecked();
  fireEvent.click(screen.getByLabelText("Keep task state and resume"));
  fireEvent.click(screen.getByLabelText("Prevent rerun if task is running"));
  await waitFor(() =>
    expect(clear.mock.lastCall?.[0].requestBody).toMatchObject({
      dry_run: true,
      keep_task_state: false,
      prevent_running_task: false,
    }),
  );
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
  );
  fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));
  await waitFor(() =>
    expect(findClearRequest(clear)?.requestBody).toMatchObject({
      keep_task_state: false,
      prevent_running_task: false,
    }),
  );
});

it("hides the affected count while the dry run is pending", async () => {
  vi.spyOn(TaskInstanceService, "postClearTaskInstances").mockReturnValue(
    new Promise(() => undefined) as ReturnType<typeof TaskInstanceService.postClearTaskInstances>,
  );

  render(<ClearExecutionDialog dagId="dag" executions={[execution]} onClose={vi.fn()} open runId="run" />, {
    wrapper: Wrapper,
  });

  expect(await screen.findByLabelText("Clear downstream tasks")).toBeVisible();
  expect(screen.queryByText(/executions? will be cleared/u)).not.toBeInTheDocument();
});

it("seeds the note from the execution and only sends it when edited", async () => {
  const clear = vi
    .spyOn(TaskInstanceService, "postClearTaskInstances")
    .mockResolvedValue({ task_instances: [], total_entries: 1 });

  render(<ClearExecutionDialog dagId="dag" executions={[execution]} onClose={vi.fn()} open runId="run" />, {
    wrapper: Wrapper,
  });

  const note = await screen.findByLabelText("Reason for clearing (optional)");

  expect(note).toHaveValue("existing note");
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
  );
  fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));
  await waitFor(() => expect(findClearRequest(clear)).toBeDefined());
  expect(findClearRequest(clear)?.requestBody.note).toBeUndefined();
});

it("does not send an empty note when the user erases the existing one", async () => {
  const clear = vi
    .spyOn(TaskInstanceService, "postClearTaskInstances")
    .mockResolvedValue({ task_instances: [], total_entries: 1 });

  render(<ClearExecutionDialog dagId="dag" executions={[execution]} onClose={vi.fn()} open runId="run" />, {
    wrapper: Wrapper,
  });

  fireEvent.change(await screen.findByLabelText("Reason for clearing (optional)"), { target: { value: "" } });
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
  );
  fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));
  await waitFor(() => expect(findClearRequest(clear)).toBeDefined());
  expect(findClearRequest(clear)?.requestBody.note).toBeUndefined();
});

it("resets the dialog state once it closes", async () => {
  vi.spyOn(TaskInstanceService, "postClearTaskInstances").mockResolvedValue({
    task_instances: [],
    total_entries: 1,
  });
  const props = { dagId: "dag", executions: [execution], onClose: vi.fn(), runId: "run" };
  const { rerender } = render(<ClearExecutionDialog {...props} open />, { wrapper: Wrapper });

  fireEvent.click(await screen.findByLabelText("Clear later loop iterations"));
  fireEvent.change(screen.getByLabelText("Reason for clearing (optional)"), { target: { value: "typed" } });
  await waitFor(() => expect(screen.getByLabelText("Clear later loop iterations")).not.toBeChecked());
  await waitFor(() => expect(screen.getByLabelText("Reason for clearing (optional)")).toHaveValue("typed"));
  rerender(<ClearExecutionDialog {...props} open={false} />);
  await waitFor(() => expect(screen.queryByLabelText("Clear later loop iterations")).not.toBeInTheDocument());
  rerender(<ClearExecutionDialog {...props} open />);

  expect(await screen.findByLabelText("Clear later loop iterations")).toBeChecked();
  expect(screen.getByLabelText("Reason for clearing (optional)")).toHaveValue("existing note");
});
