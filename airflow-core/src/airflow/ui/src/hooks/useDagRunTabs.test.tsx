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
import { renderHook } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { DAGRunResponse } from "openapi/requests/types.gen";

import { DagRunTab } from "src/constants/tab";

import { useDagRunTabs } from "./useDagRunTabs";

const dagId = "etl_sales";
const runId = "manual__2026-01-01";

const TABS = [
  { icon: undefined, label: "Task Instances", value: "" },
  { icon: undefined, label: "Asset Events", value: DagRunTab.AssetEvents },
  { icon: undefined, label: "Code", value: "code" },
];

let mockPathname = `/dags/${dagId}/runs/${runId}`;

vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactRouterDom>()),
  useLocation: () => ({ pathname: mockPathname }),
}));

const runOfType = (runType: string) => ({ run_type: runType }) as DAGRunResponse;

const tabValues = (dagRun: DAGRunResponse | undefined) =>
  renderHook(() => useDagRunTabs(dagRun, TABS)).result.current.tabs.map((tab) => tab.value);

describe("useDagRunTabs", () => {
  beforeEach(() => {
    mockPathname = `/dags/${dagId}/runs/${runId}`;
  });

  it("keeps the asset events tab for an asset-triggered run", () => {
    expect(tabValues(runOfType("asset_triggered"))).toEqual(TABS.map((tab) => tab.value));
  });

  it("keeps every tab while the run is still unknown", () => {
    expect(tabValues(undefined)).toHaveLength(TABS.length);
  });

  it.each(["scheduled", "manual", "backfill", "asset_materialization"])(
    "drops asset events for a %s run, which has no consumed events",
    (runType) => {
      expect(tabValues(runOfType(runType))).not.toContain(DagRunTab.AssetEvents);
    },
  );

  it("leaves tabs it has no rule for alone", () => {
    expect(tabValues(runOfType("scheduled"))).toEqual(["", "code"]);
  });

  it("keeps a hidden tab the user is already on, so deep links still work", () => {
    mockPathname = `/dags/${dagId}/runs/${runId}/${DagRunTab.AssetEvents}`;

    expect(tabValues(runOfType("scheduled"))).toContain(DagRunTab.AssetEvents);
  });
});
