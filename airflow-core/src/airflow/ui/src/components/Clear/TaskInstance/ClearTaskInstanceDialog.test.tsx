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
import "@testing-library/jest-dom";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import commonLocale from "../../../../public/i18n/locales/en/common.json";
import dagsLocale from "../../../../public/i18n/locales/en/dags.json";
import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

const { mockMutate, mockUseClearTaskInstances, mockUseClearTaskInstancesDryRun } = vi.hoisted(() => ({
  mockMutate: vi.fn(),
  mockUseClearTaskInstances: vi.fn(),
  mockUseClearTaskInstancesDryRun: vi.fn(),
}));

vi.mock("src/queries/useClearTaskInstances", () => ({
  useClearTaskInstances: mockUseClearTaskInstances,
}));

vi.mock("src/queries/useClearTaskInstancesDryRun", () => ({
  useClearTaskInstancesDryRun: mockUseClearTaskInstancesDryRun,
}));

vi.mock("src/hooks/useUserSettings", () => ({
  useClearPreventRunningTaskDefault: () => [false, vi.fn()],
  useClearTaskInstanceDefaultOptions: () => [[], vi.fn()],
}));

vi.mock("openapi/queries", async (importOriginal) => {
  // eslint-disable-next-line @typescript-eslint/consistent-type-imports -- `import()` type is the standard pattern for typing `importOriginal` in Vitest mocks.
  const actual = await importOriginal<typeof import("openapi/queries")>();

  return {
    ...actual,
    useDagRunServiceGetDagRun: () => ({ data: undefined }),
    useDagServiceGetDagDetails: () => ({ data: undefined }),
  };
});

const taskInstance: TaskInstanceResponse = {
  dag_display_name: "example_dag",
  dag_id: "example_dag",
  dag_run_id: "TEST_DAG_RUN_ID",
  dag_version: null,
  duration: null,
  end_date: null,
  executor: null,
  executor_config: "{}",
  hostname: null,
  id: "ti-1",
  ignore_upstream_deps: false,
  logical_date: "2025-01-01T00:00:00Z",
  map_index: -1,
  max_tries: 0,
  note: null,
  operator: "EmptyOperator",
  operator_name: "EmptyOperator",
  pid: null,
  pool: "default_pool",
  pool_slots: 1,
  priority_weight: 1,
  queue: null,
  queued_when: null,
  rendered_fields: {},
  rendered_map_index: null,
  run_after: "2025-01-01T00:00:00Z",
  scheduled_when: null,
  start_date: null,
  state: "success",
  task_display_name: "task",
  task_id: "task",
  team_name: null,
  trigger: null,
  triggerer_job: null,
  try_number: 1,
  unixname: "airflow",
};

const dryRunResult = {
  data: {
    task_instances: [taskInstance],
    total_entries: 1,
  },
};

describe("ClearTaskInstanceDialog", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "dags", dagsLocale, true, true);
    i18n.addResourceBundle("en", "common", commonLocale, true, true);
    mockMutate.mockReset();
    mockUseClearTaskInstances.mockReturnValue({ isPending: false, mutate: mockMutate });
    mockUseClearTaskInstancesDryRun.mockReturnValue(dryRunResult);
  });

  it("does not show the force run warning by default, and shows it once ticked", () => {
    render(
      <Wrapper>
        <ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />
      </Wrapper>,
    );

    const warning = i18n.t("dags:runAndTaskActions.forceRunWarning");

    expect(screen.queryByText(warning)).not.toBeInTheDocument();

    const forceRunCheckbox = screen.getByRole("checkbox", {
      name: i18n.t("dags:runAndTaskActions.options.forceRun"),
    });

    fireEvent.click(forceRunCheckbox);

    expect(screen.getByText(warning)).toBeInTheDocument();
  });

  it("disables the upstream/downstream segments while force run is ticked", () => {
    render(
      <Wrapper>
        <ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />
      </Wrapper>,
    );

    const upstreamOption = screen.getByRole("radio", {
      name: i18n.t("dags:runAndTaskActions.options.upstream"),
    });
    const downstreamOption = screen.getByRole("radio", {
      name: i18n.t("dags:runAndTaskActions.options.downstream"),
    });

    expect(upstreamOption).not.toBeDisabled();
    expect(downstreamOption).not.toBeDisabled();

    fireEvent.click(
      screen.getByRole("checkbox", { name: i18n.t("dags:runAndTaskActions.options.forceRun") }),
    );

    expect(upstreamOption).toBeDisabled();
    expect(downstreamOption).toBeDisabled();
  });

  it("sends ignore_upstream_deps, only_failed false and no relatives when force run is confirmed", async () => {
    render(
      <Wrapper>
        <ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />
      </Wrapper>,
    );

    fireEvent.click(
      screen.getByRole("checkbox", { name: i18n.t("dags:runAndTaskActions.options.forceRun") }),
    );

    fireEvent.click(screen.getByRole("button", { name: i18n.t("modal.confirm", { ns: "common" }) }));

    await waitFor(() => {
      expect(mockMutate).toHaveBeenCalled();
    });

    const [call] = mockMutate.mock.calls[0] as [{ requestBody: Record<string, unknown> }];

    expect(call.requestBody).toMatchObject({
      ignore_upstream_deps: true,
      include_downstream: false,
      include_upstream: false,
      only_failed: false,
    });
  });

  it("omits ignore_upstream_deps from the request body when force run is left unticked", async () => {
    render(
      <Wrapper>
        <ClearTaskInstanceDialog onClose={vi.fn()} open taskInstance={taskInstance} />
      </Wrapper>,
    );

    fireEvent.click(screen.getByRole("button", { name: i18n.t("modal.confirm", { ns: "common" }) }));

    await waitFor(() => {
      expect(mockMutate).toHaveBeenCalled();
    });

    const [call] = mockMutate.mock.calls[0] as [{ requestBody: Record<string, unknown> }];

    expect(call.requestBody).not.toHaveProperty("ignore_upstream_deps");
  });
});
