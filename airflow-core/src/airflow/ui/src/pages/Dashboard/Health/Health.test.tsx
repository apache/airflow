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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { DetailedHealthStatus, HealthInfoResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { Health } from "./Health";

const mocks = vi.hoisted(() => ({ useMonitorServiceGetHealth: vi.fn() }));

vi.mock("openapi/queries", () => ({
  useMonitorServiceGetHealth: mocks.useMonitorServiceGetHealth,
}));

vi.mock("src/utils", () => ({ useAutoRefresh: () => false }));

const mockConfig: Record<string, unknown> = { multi_team: false };

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string, options?: Record<string, unknown>) =>
      key === "health.instances.title"
        ? `${options?.title as string} — ${options?.status as string} (${options?.count as number})`
        : key,
  }),
}));

const health = (overrides: Partial<HealthInfoResponse> = {}): HealthInfoResponse => ({
  dag_processor: null,
  metadatabase: { status: "healthy" },
  scheduler: {
    detailed_status: "healthy",
    instances: null,
    latest_scheduler_heartbeat: "2026-09-11T10:00:00Z",
    status: "healthy",
  },
  triggerer: {
    detailed_status: "healthy",
    instances: null,
    latest_triggerer_heartbeat: "2026-09-11T10:00:00Z",
    status: "healthy",
  },
  ...overrides,
});

const withSchedulers = (
  instances: HealthInfoResponse["scheduler"]["instances"],
  detailedStatus: DetailedHealthStatus = "healthy",
) =>
  health({
    scheduler: {
      detailed_status: detailedStatus,
      instances,
      latest_scheduler_heartbeat: "2026-09-11T10:00:00Z",
      status: "healthy",
    },
  });

const withDagProcessors = (
  instances: NonNullable<HealthInfoResponse["dag_processor"]>["instances"],
  detailedStatus: DetailedHealthStatus = "healthy",
) =>
  health({
    dag_processor: {
      detailed_status: detailedStatus,
      instances,
      latest_dag_processor_heartbeat: "2026-09-11T10:00:00Z",
      status: "healthy",
    },
  });

const renderHealth = (data: HealthInfoResponse) => {
  mocks.useMonitorServiceGetHealth.mockReturnValue({ data, error: undefined, isLoading: false });

  return render(<Health />, { wrapper: Wrapper });
};

const openBadge = async (title: string) => {
  fireEvent.click(screen.getByRole("button", { name: title }));

  await waitFor(() => expect(screen.getByRole("table")).toBeInTheDocument());
};

describe("Health", () => {
  beforeEach(() => {
    mocks.useMonitorServiceGetHealth.mockReset();
  });

  afterEach(() => {
    mockConfig.multi_team = false;
  });

  it("does not make a component clickable when the endpoint reports no instances", () => {
    renderHealth(health());

    expect(screen.getByText("health.scheduler")).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("moves focus into the instance list so it is reachable from the keyboard", async () => {
    renderHealth(
      withSchedulers([
        {
          hostname: "scheduler-1.example.com",
          latest_scheduler_heartbeat: "2026-09-11T10:00:00Z",
        },
      ]),
    );

    await openBadge("health.scheduler");

    await waitFor(() => expect(screen.getByRole("dialog").contains(document.activeElement)).toBe(true));
  });

  it("lists every running scheduler with its own heartbeat", async () => {
    renderHealth(
      withSchedulers([
        {
          hostname: "scheduler-1.example.com",
          latest_scheduler_heartbeat: "2026-09-11T10:00:00Z",
        },
        {
          hostname: "scheduler-2.example.com",
          latest_scheduler_heartbeat: "2026-09-11T09:00:00Z",
        },
      ]),
    );

    await openBadge("health.scheduler");

    expect(screen.getByText("scheduler-1.example.com")).toBeInTheDocument();
    expect(screen.getByText("scheduler-2.example.com")).toBeInTheDocument();
    expect(screen.getAllByTestId("time-display")).toHaveLength(2);
  });

  it("titles the instance list with detailed_status rather than the legacy status", async () => {
    renderHealth(
      withDagProcessors(
        [
          {
            bundle_names: ["dags-team-a"],
            hostname: "dag-processor-1.example.com",
            latest_dag_processor_heartbeat: "2026-09-11T10:00:00Z",
          },
        ],
        "degraded",
      ),
    );

    await openBadge("health.dagProcessor");

    expect(screen.getByText("health.dagProcessor — health.degraded (1)")).toBeInTheDocument();
  });

  it("explains what degraded means for the component reporting it", async () => {
    renderHealth(
      withDagProcessors(
        [
          {
            bundle_names: ["dags-team-a"],
            hostname: "dag-processor-1.example.com",
            latest_dag_processor_heartbeat: "2026-09-11T10:00:00Z",
          },
        ],
        "degraded",
      ),
    );

    await openBadge("health.dagProcessor");

    expect(screen.getByText("health.degradedHint.dagProcessor")).toBeInTheDocument();
  });

  it("omits the degraded explanation when every part of the work is covered", async () => {
    renderHealth(
      withDagProcessors([
        {
          bundle_names: ["dags-team-a"],
          hostname: "dag-processor-1.example.com",
          latest_dag_processor_heartbeat: "2026-09-11T10:00:00Z",
        },
      ]),
    );

    await openBadge("health.dagProcessor");

    expect(screen.queryByText("health.degradedHint.dagProcessor")).not.toBeInTheDocument();
  });

  it("labels a status the UI does not know as unknown", async () => {
    renderHealth(
      withSchedulers(
        [
          {
            hostname: "scheduler-1.example.com",
            latest_scheduler_heartbeat: "2026-09-11T10:00:00Z",
          },
        ],
        // Cast: a newer API can report a status this UI version has no mapping for.
        "sideways" as DetailedHealthStatus,
      ),
    );

    await openBadge("health.scheduler");

    expect(screen.getByText("health.scheduler — health.unknownStatus (1)")).toBeInTheDocument();
  });

  it("falls back to a placeholder when an instance reports no hostname", async () => {
    renderHealth(withSchedulers([{ hostname: null, latest_scheduler_heartbeat: "2026-09-11T10:00:00Z" }]));

    await openBadge("health.scheduler");

    expect(screen.getByText("health.instances.unknownHostname")).toBeInTheDocument();
  });

  it("shows the owning team of each triggerer only when multi-team is enabled", async () => {
    mockConfig.multi_team = true;
    renderHealth(
      health({
        triggerer: {
          detailed_status: "healthy",
          instances: [
            {
              hostname: "triggerer-1.example.com",
              latest_triggerer_heartbeat: "2026-09-11T10:00:00Z",
              team_name: "team-a",
            },
          ],
          latest_triggerer_heartbeat: "2026-09-11T10:00:00Z",
          status: "healthy",
        },
      }),
    );

    await openBadge("health.triggerer");

    expect(screen.getByText("health.instances.team")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "team-a" })).toHaveAttribute("href", "/dags?teams=team-a");
  });

  it("hides the team column when multi-team is disabled", async () => {
    renderHealth(
      health({
        triggerer: {
          detailed_status: "healthy",
          instances: [
            {
              hostname: "triggerer-1.example.com",
              latest_triggerer_heartbeat: "2026-09-11T10:00:00Z",
              team_name: "team-a",
            },
          ],
          latest_triggerer_heartbeat: "2026-09-11T10:00:00Z",
          status: "healthy",
        },
      }),
    );

    await openBadge("health.triggerer");

    expect(screen.queryByText("health.instances.team")).not.toBeInTheDocument();
    expect(screen.queryByText("team-a")).not.toBeInTheDocument();
  });

  it("hides the team column when every triggerer is unscoped", async () => {
    mockConfig.multi_team = true;
    renderHealth(
      health({
        triggerer: {
          detailed_status: "healthy",
          instances: [
            {
              hostname: "triggerer-1.example.com",
              latest_triggerer_heartbeat: "2026-09-11T10:00:00Z",
              team_name: null,
            },
          ],
          latest_triggerer_heartbeat: "2026-09-11T10:00:00Z",
          status: "healthy",
        },
      }),
    );

    await openBadge("health.triggerer");

    expect(screen.queryByText("health.instances.team")).not.toBeInTheDocument();
  });

  it("lists the bundles each Dag processor instance parses", async () => {
    renderHealth(
      withDagProcessors([
        {
          bundle_names: ["dags-team-a", "dags-team-b"],
          hostname: "dag-processor-1.example.com",
          latest_dag_processor_heartbeat: "2026-09-11T10:00:00Z",
        },
      ]),
    );

    await openBadge("health.dagProcessor");

    expect(screen.getByText("health.instances.bundles")).toBeInTheDocument();
    expect(screen.getByText("dags-team-a, dags-team-b")).toBeInTheDocument();
  });

  it("hides the bundles column when no Dag processor reports one", async () => {
    renderHealth(
      withDagProcessors([
        {
          bundle_names: null,
          hostname: "dag-processor-1.example.com",
          latest_dag_processor_heartbeat: "2026-09-11T10:00:00Z",
        },
      ]),
    );

    await openBadge("health.dagProcessor");

    expect(screen.queryByText("health.instances.bundles")).not.toBeInTheDocument();
  });

  it("renders skeletons while the health request is in flight", () => {
    mocks.useMonitorServiceGetHealth.mockReturnValue({
      data: undefined,
      error: undefined,
      isLoading: true,
    });

    render(<Health />, { wrapper: Wrapper });

    expect(screen.queryByText("health.scheduler")).not.toBeInTheDocument();
  });
});
