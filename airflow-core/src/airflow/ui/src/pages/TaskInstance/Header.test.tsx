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
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { Header } from "./Header";

// Action buttons and note preview pull in mutation/permission wiring that is
// unrelated to the team stat under test; stub them out so the test only
// depends on the stats rendered by Header itself.
vi.mock("src/components/Clear", () => ({ ClearTaskInstanceButton: () => undefined }));
vi.mock("src/components/Clear/TaskInstance/ClearTaskInstanceDialog", () => ({ default: () => undefined }));
vi.mock("src/components/MarkAs", () => ({ MarkTaskInstanceAsButton: () => undefined }));
vi.mock("src/components/NotePreview", () => ({ default: () => undefined }));

const mockConfig: Record<string, unknown> = { multi_team: false };

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

const baseTaskInstance = {
  dag_id: "test_dag",
  dag_run_id: "run_1",
  dag_version: null,
  duration: null,
  end_date: null,
  map_index: -1,
  note: null,
  operator_name: "PythonOperator",
  rendered_map_index: null,
  start_date: null,
  state: "success",
  task_display_name: "test_task",
  task_id: "test_task",
  try_number: 1,
} satisfies Partial<TaskInstanceResponse> as unknown as TaskInstanceResponse;

describe("Header", () => {
  beforeEach(() => {
    mockConfig.multi_team = false;
  });

  it("shows the team stat when multi-team is enabled and a team is resolved", () => {
    mockConfig.multi_team = true;
    render(<Header taskInstance={{ ...baseTaskInstance, team_name: "team-a" }} />, { wrapper: Wrapper });

    expect(screen.getByText(i18n.t("common:dagDetails.team"))).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "team-a" })).toHaveAttribute("href", "/dags?teams=team-a");
  });

  it("hides the team stat when multi-team is disabled", () => {
    render(<Header taskInstance={{ ...baseTaskInstance, team_name: "team-a" }} />, { wrapper: Wrapper });

    expect(screen.queryByText(i18n.t("common:dagDetails.team"))).not.toBeInTheDocument();
  });
});
