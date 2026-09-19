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
import { render, screen } from "@testing-library/react";
import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeAll, describe, expect, it, vi } from "vitest";

import type { TaskInstanceHistoryResponse, TaskInstanceResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import commonLocale from "../../../public/i18n/locales/en/common.json";
import { Details } from "./Details";

const { mockUseMappedTaskInstance, mockUseTaskInstanceTryDetails } = vi.hoisted(() => ({
  mockUseMappedTaskInstance: vi.fn(),
  mockUseTaskInstanceTryDetails: vi.fn(),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  // eslint-disable-next-line @typescript-eslint/consistent-type-imports -- `import()` type is the standard pattern for typing `importOriginal` in Vitest mocks.
  const actual = await importOriginal<typeof import("openapi/queries")>();

  return {
    ...actual,
    useTaskInstanceServiceGetMappedTaskInstance: mockUseMappedTaskInstance,
    useTaskInstanceServiceGetTaskInstanceTryDetails: mockUseTaskInstanceTryDetails,
  };
});

vi.mock("./ExtraLinks", () => ({ ExtraLinks: () => null }));
vi.mock("./BlockingDeps", () => ({ BlockingDeps: () => null }));
vi.mock("./TriggererInfo", () => ({ TriggererInfo: () => null }));

const baseTaskInstance: TaskInstanceResponse = {
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

const baseTryInstance: TaskInstanceHistoryResponse = { ...baseTaskInstance };

const renderDetails = (ignoreUpstreamDeps: boolean) => {
  mockUseMappedTaskInstance.mockReturnValue({
    data: { ...baseTaskInstance, ignore_upstream_deps: ignoreUpstreamDeps },
  });
  mockUseTaskInstanceTryDetails.mockReturnValue({
    data: { ...baseTryInstance, ignore_upstream_deps: ignoreUpstreamDeps },
  });

  return render(
    <BaseWrapper>
      <TimezoneProvider>
        <MemoryRouter initialEntries={["/dags/example_dag/runs/TEST_DAG_RUN_ID/tasks/task/-1"]}>
          <Routes>
            <Route element={<Details />} path="/dags/:dagId/runs/:runId/tasks/:taskId/:mapIndex" />
          </Routes>
        </MemoryRouter>
      </TimezoneProvider>
    </BaseWrapper>,
  );
};

describe("Details", () => {
  // src/i18n/config.ts kicks off VersionService.getVersion() at import time, which never
  // settles in network-isolated environments. Initialising a plain i18next instance here
  // avoids importing that module (and its network call) at all.
  beforeAll(async () => {
    await i18n.use(initReactI18next).init({
      defaultNS: "common",
      fallbackLng: "en",
      lng: "en",
      ns: ["common"],
      resources: { en: { common: commonLocale } },
    });
  });

  it("does not show the force run row when the task instance was not forced", () => {
    renderDetails(false);

    expect(screen.queryByText(i18n.t("taskInstance.forceRun", { ns: "common" }))).not.toBeInTheDocument();
  });

  it("shows the force run row when the task instance was forced", () => {
    renderDetails(true);

    expect(screen.getByText(i18n.t("taskInstance.forceRun", { ns: "common" }))).toBeInTheDocument();
  });
});
