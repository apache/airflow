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
import type { DagScheduleAssetReference, TaskOutletAssetReference } from "openapi-gen/requests/types.gen";
import { afterEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { DependencyPopover } from "./DependencyPopover";

const mockConfig: Record<string, unknown> = { multi_team: false };

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

const scheduledDags = [
  {
    created_at: "2024-01-01T00:00:00Z",
    dag_id: "dag_a",
    team_name: "team-a",
    updated_at: "2024-01-01T00:00:00Z",
  },
] as Array<DagScheduleAssetReference>;

const producingTasks = [
  {
    created_at: "2024-01-01T00:00:00Z",
    dag_id: "dag_b",
    task_id: "task_b",
    team_name: "team-b",
    updated_at: "2024-01-01T00:00:00Z",
  },
] as Array<TaskOutletAssetReference>;

describe("DependencyPopover", () => {
  afterEach(() => {
    mockConfig.multi_team = false;
  });

  it("shows the owning team next to each scheduled Dag when multi-team is enabled", async () => {
    mockConfig.multi_team = true;
    render(
      <Wrapper>
        <DependencyPopover dependencies={scheduledDags} type="Dag" />
      </Wrapper>,
    );

    fireEvent.click(screen.getByRole("button"));

    await waitFor(() => expect(screen.getByRole("link", { name: "dag_a" })).toBeInTheDocument());
    expect(screen.getByText(i18n.t("common:dagDetails.team"))).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "team-a" })).toHaveAttribute("href", "/dags?teams=team-a");
  });

  it("shows the owning team next to each producing task when multi-team is enabled", async () => {
    mockConfig.multi_team = true;
    render(
      <Wrapper>
        <DependencyPopover dependencies={producingTasks} type="Task" />
      </Wrapper>,
    );

    fireEvent.click(screen.getByRole("button"));

    await waitFor(() => expect(screen.getByRole("link", { name: "dag_b.task_b" })).toBeInTheDocument());
    expect(screen.getByText(i18n.t("common:dagDetails.team"))).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "team-b" })).toHaveAttribute("href", "/dags?teams=team-b");
  });

  it("does not show the team when multi-team is disabled", async () => {
    render(
      <Wrapper>
        <DependencyPopover dependencies={scheduledDags} type="Dag" />
      </Wrapper>,
    );

    fireEvent.click(screen.getByRole("button"));

    await waitFor(() => expect(screen.getByRole("link", { name: "dag_a" })).toBeInTheDocument());
    expect(screen.queryByText(i18n.t("common:dagDetails.team"))).not.toBeInTheDocument();
    expect(screen.queryByText("team-a")).not.toBeInTheDocument();
  });
});
