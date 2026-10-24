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
import { render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { DAGDetailsResponse, WebhookMatchResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import commonLocale from "../../../public/i18n/locales/en/common.json";
import { Details } from "./Details";

vi.mock("src/components/DagVersionDetails", () => ({ DagVersionDetails: () => undefined }));
vi.mock("src/components/TeamName", () => ({ TeamName: () => undefined }));
vi.mock("src/hooks/useShowTeam", () => ({ useShowTeam: () => false }));

const mockDagDetails = vi.fn<() => DAGDetailsResponse>();

vi.mock("openapi/queries", async () => {
  const actual = await vi.importActual("openapi/queries");

  return { ...actual, useDagServiceGetDagDetails: () => ({ data: mockDagDetails() }) };
});

const buildDagDetails = (cancelOn: Array<WebhookMatchResponse> | null | undefined): DAGDetailsResponse =>
  ({
    cancel_on: cancelOn,
    catchup: false,
    dag_id: "preview_environment",
    dag_run_timeout: null,
    default_args: null,
    description: null,
    end_date: null,
    fileloc: "/files/dags/preview_environment.py",
    has_task_concurrency_limits: false,
    last_expired: null,
    last_parse_duration: null,
    last_parsed: null,
    latest_dag_version: null,
    max_active_runs: 2,
    max_active_tasks: 16,
    max_consecutive_failed_dag_runs: 0,
    params: null,
    start_date: null,
    team_name: null,
    timezone: "UTC",
  }) as unknown as DAGDetailsResponse;

describe("Dag Details", () => {
  // Without the bundle i18n.t() echoes the key, so the text assertions below would pass blindly.
  beforeEach(() => {
    i18n.addResourceBundle("en", "common", commonLocale, true, true);
  });

  it("lists the Dag's cancel_on conditions", () => {
    mockDagDetails.mockReturnValue(
      buildDagDetails([
        {
          correlation_key_conf: "pr",
          events: ["pull_request"],
          match: { action: ["closed"], repository: ["my-org/shop"] },
          webhook: "github",
        },
      ]),
    );

    render(<Details />, { wrapper: Wrapper });

    const row = screen.getByTestId("cancel-on-row");

    expect(within(row).getByText("Cancel On")).toBeInTheDocument();
    expect(within(row).getByText(commonLocale.dagDetails.cancelOnExplanation)).toBeInTheDocument();
    expect(within(row).getByRole("link", { name: "github" })).toHaveAttribute(
      "href",
      "/webhook_deliveries/github",
    );
    expect(within(row).getByText("pull_request")).toBeInTheDocument();
    expect(within(row).getByText("my-org/shop")).toBeInTheDocument();
    expect(
      within(row).getByText(
        "Also the runs triggered with the delivery's correlation key as pr in their conf",
      ),
    ).toBeInTheDocument();
  });

  it.each([null, undefined, []])("shows no Cancel On row for cancel_on %j", (cancelOn) => {
    mockDagDetails.mockReturnValue(buildDagDetails(cancelOn));

    render(<Details />, { wrapper: Wrapper });

    expect(screen.getByTestId("dag-details-table")).toBeInTheDocument();
    expect(screen.queryByTestId("cancel-on-row")).not.toBeInTheDocument();
  });
});
