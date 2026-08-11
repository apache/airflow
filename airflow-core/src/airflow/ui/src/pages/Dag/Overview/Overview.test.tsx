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
import { describe, expect, it, vi } from "vitest";

import type { ReactAppResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { Overview } from "./Overview";

vi.mock("openapi/queries", () => ({
  useAssetServiceGetAssetEvents: () => ({ data: undefined, isLoading: false }),
  useDagRunServiceGetDagRuns: () => ({ data: { dag_runs: [], total_entries: 0 }, isLoading: false }),
  usePluginServiceGetPlugins: () => ({
    data: {
      plugins: [
        {
          react_apps: [
            { bundle_url: "/dag.js", destination: "dag_overview", name: "Dag overview plugin" },
            { bundle_url: "/task.js", destination: "task_overview", name: "Task overview plugin" },
          ],
        },
      ],
    },
  }),
  useTaskInstanceServiceGetTaskInstances: () => ({
    data: { task_instances: [], total_entries: 0 },
    isLoading: false,
  }),
}));

vi.mock("src/components/Assets/AssetEvents", () => ({ AssetEvents: () => null }));
vi.mock("src/components/DurationChart", () => ({ DurationChart: () => null }));
vi.mock("src/components/TimeRangeSelector", () => ({ default: () => null }));
vi.mock("src/components/TrendCountButton", () => ({ TrendCountButton: () => null }));
vi.mock("src/pages/ReactPlugin", () => ({
  ReactPlugin: ({ reactApp }: { readonly reactApp: ReactAppResponse }) => <div>{reactApp.name}</div>,
}));
vi.mock("src/queries/useGridRuns.ts", () => ({ useGridRuns: () => ({ data: [], isLoading: false }) }));
vi.mock("src/utils", () => ({ isStatePending: () => false, useAutoRefresh: () => false }));
vi.mock("./DagDeadlines", () => ({ DagDeadlines: () => null }));
vi.mock("./FailedLogs", () => ({ default: () => null }));

describe("Dag overview plugins", () => {
  it("renders only React plugins registered for the Dag overview", () => {
    render(<Overview />, { wrapper: Wrapper });

    expect(screen.getByText("Dag overview plugin")).toBeInTheDocument();
    expect(screen.queryByText("Task overview plugin")).not.toBeInTheDocument();
  });
});
