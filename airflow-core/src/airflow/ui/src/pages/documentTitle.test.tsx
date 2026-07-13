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
import { render, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { AppWrapper } from "src/utils/AppWrapper";

// The detail pages open a task-instance summaries stream that would otherwise stay active when the
// tiny title-only assertion resolves and the tree unmounts. Stub it so no stream is left open.
vi.mock("src/queries/useGridTISummaries.ts", () => ({
  useGridTiSummariesStream: () => ({ isLoading: false, summariesByRunId: new Map() }),
}));

// instance_name in the mocked config is "Airflow" (src/mocks/handlers/config.ts). The test i18n
// backend is not loaded, so translations resolve to their keys — asserting on the key still proves
// each page wires the intended translation key into the browser tab title.
const INSTANCE = "Airflow";

describe("document title", () => {
  beforeEach(() => {
    document.title = "";
  });

  it.each([
    ["/dags", "nav.dags"],
    ["/assets", "nav.assets"],
    ["/dag_runs", "dagRun_other"],
    ["/task_instances", "taskInstance_other"],
    ["/connections", "admin.Connections"],
    ["/variables", "admin.Variables"],
    ["/pools", "admin.Pools"],
    ["/providers", "admin.Providers"],
    ["/plugins", "admin.Plugins"],
    ["/configs", "admin.Config"],
    ["/deadlines", "browse.deadlines"],
    ["/jobs", "browse.jobs"],
    ["/xcoms", "browse.xcoms"],
    ["/events", "browse.auditLog"],
    ["/required_actions", "browse.requiredActions"],
    ["/security/users", "nav.security"],
  ])("shows the page name before the instance name on %s", async (route, pageTitle) => {
    render(<AppWrapper initialEntries={[route]} />);

    await waitFor(() => expect(document.title).toBe(`${pageTitle} - ${INSTANCE}`));
  });

  it.each([
    ["/dags/log_grouping/runs/manual__2025-02-18T12:19", "manual__2025-02-18T12:19"],
    ["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/log_source", "log_source"],
    // Nested list/audit tabs are gated, so the detail page keeps ownership of the title.
    ["/dags/log_grouping/runs/manual__2025-02-18T12:19/tasks/log_source/events", "log_source"],
  ])("uses the entity identifier for detail page %s", async (route, pageTitle) => {
    render(<AppWrapper initialEntries={[route]} />);

    await waitFor(() => expect(document.title).toBe(`${pageTitle} - ${INSTANCE}`));
  });
});
