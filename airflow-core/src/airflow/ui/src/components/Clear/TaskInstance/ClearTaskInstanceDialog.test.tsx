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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenApiQueries from "openapi/queries";
import type {
  ClearTaskInstancesBody,
  DagVersionResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

import { CLEAR_KEEP_TASK_STATE_KEY } from "src/constants/localStorage";
import type * as UserSettings from "src/hooks/useUserSettings";
import { Wrapper } from "src/utils/Wrapper";

import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

const latestVersion: DagVersionResponse = {
  bundle_name: "git-bundle",
  bundle_url: null,
  bundle_version: "latest",
  created_at: "2026-09-26T00:00:00Z",
  dag_display_name: "Test Dag",
  dag_id: "test_dag",
  id: "dag-version-21",
  version_number: 21,
};

const taskInstance: TaskInstanceResponse = {
  dag_display_name: "Test Dag",
  dag_id: "test_dag",
  dag_run_id: "test_run",
  dag_version: latestVersion,
  duration: null,
  end_date: null,
  executor: null,
  executor_config: "{}",
  hostname: null,
  id: "test_task_instance",
  logical_date: "2026-09-26T00:00:00Z",
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
  rendered_map_index: null,
  run_after: "2026-09-26T00:00:00Z",
  scheduled_when: null,
  start_date: null,
  state: "failed",
  task_display_name: "task",
  task_id: "task",
  trigger: null,
  triggerer_job: null,
  try_number: 1,
  unixname: null,
};

let dagRun: { bundle_version: string | null; dag_versions: Array<DagVersionResponse> } | undefined;
const mutate = vi.fn<(variables: { dagId: string; requestBody: ClearTaskInstancesBody }) => void>();

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenApiQueries>()),
  useDagRunServiceGetDagRun: () => ({ data: dagRun }),
  useDagServiceGetDagDetails: () => ({
    data: { bundle_version: "latest", latest_dag_version: latestVersion },
  }),
}));

vi.mock("src/components/ActionAccordion", () => ({ ActionAccordion: () => null }));
vi.mock("src/queries/useConfig", () => ({ useConfig: () => false }));
vi.mock("src/queries/useClearTaskInstances", () => ({
  useClearTaskInstances: () => ({ isPending: false, mutate }),
}));
vi.mock("src/queries/useClearTaskInstancesDryRun", () => ({
  useClearTaskInstancesDryRun: () => ({
    data: { task_instances: [taskInstance], total_entries: 1 },
    isFetching: false,
  }),
}));
vi.mock("src/hooks/useUserSettings", async (importOriginal) => ({
  ...(await importOriginal<typeof UserSettings>()),
  useClearPreventRunningTaskDefault: () => [false],
  useClearTaskInstanceDefaultOptions: () => [[]],
}));

describe("ClearTaskInstanceDialog", () => {
  beforeEach(() => {
    mutate.mockClear();
    dagRun = { bundle_version: "historical", dag_versions: [latestVersion] };
  });

  afterEach(() => {
    localStorage.clear();
  });

  it("seeds the keep-task-state checkbox from the stored default and sends it on confirm", async () => {
    localStorage.setItem(CLEAR_KEEP_TASK_STATE_KEY, JSON.stringify(true));

    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByRole("checkbox", { name: /keepTaskState/iu })).toBeChecked();

    fireEvent.click(screen.getByRole("button", { name: /modal\.confirm/iu }));

    await waitFor(() => expect(mutate).toHaveBeenCalledOnce());
    expect(mutate.mock.calls[0]?.[0].requestBody.keep_task_state).toBe(true);
  });

  it.each([false, true])("offers a bundle-only update and submits the user's choice: %s", async (latest) => {
    render(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />, {
      wrapper: Wrapper,
    });

    const checkbox = screen.getByRole("checkbox", { name: /run.*latest/iu });

    expect(checkbox).not.toBeChecked();
    if (latest) {
      fireEvent.click(checkbox);
      await waitFor(() => expect(checkbox).toBeChecked());
    }
    fireEvent.click(screen.getByRole("button", { name: /confirm/iu }));

    await waitFor(() => {
      expect(mutate).toHaveBeenCalledOnce();
      expect(mutate.mock.calls[0]?.[0].dagId).toBe("test_dag");
      expect(mutate.mock.calls[0]?.[0].requestBody).toMatchObject({
        dag_run_id: "test_run",
        dry_run: false,
        run_on_latest_version: latest,
      });
    });
  });

  it("waits for the run's bundle before allowing confirmation", () => {
    dagRun = undefined;
    const { rerender } = render(
      <ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />,
      { wrapper: Wrapper },
    );

    expect(screen.getByRole("button", { name: /confirm/iu })).toBeDisabled();
    expect(screen.queryByRole("checkbox", { name: /run.*latest/iu })).not.toBeInTheDocument();

    dagRun = { bundle_version: "historical", dag_versions: [latestVersion] };
    rerender(<ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />);

    expect(screen.getByRole("button", { name: /confirm/iu })).toBeEnabled();
    expect(screen.getByRole("checkbox", { name: /run.*latest/iu })).toBeInTheDocument();
  });

  it("offers a bundle-only update when clearing all mapped instances", () => {
    render(
      <ClearTaskInstanceDialog
        allMapped
        dagId="test_dag"
        dagRunId="test_run"
        onClose={vi.fn()}
        open
        taskId="task"
      />,
      { wrapper: Wrapper },
    );

    expect(screen.getByRole("checkbox", { name: /run.*latest/iu })).toBeInTheDocument();
  });
});
