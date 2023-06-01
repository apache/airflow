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

/* global describe  */

import React from "react";
import * as useHistoricalMetricsDataModule from "src/api/useHistoricalMetricsData";
import * as useDagsModule from "src/api/useDags";
import * as useDagRunsModule from "src/api/useDagRuns";
import * as usePoolsModule from "src/api/usePools";
import * as useHealthModule from "src/api/useHealth";

import { render } from "@testing-library/react";

import { Wrapper } from "src/utils/testUtils";
import ClusterActivity from ".";

const mockHistoricalMetricsData = {
  dag_run_states: { failed: 0, queued: 0, running: 0, success: 306 },
  dag_run_types: {
    backfill: 0,
    dataset_triggered: 0,
    manual: 14,
    scheduled: 292,
  },
  task_instance_states: {
    deferred: 0,
    failed: 0,
    no_status: 0,
    queued: 0,
    removed: 0,
    restarting: 0,
    running: 0,
    scheduled: 0,
    shutdown: 0,
    skipped: 0,
    success: 1634,
    up_for_reschedule: 0,
    up_for_retry: 0,
    upstream_failed: 0,
  },
};

const mockHealthData = {
  metadatabase: {
    status: "healthy",
  },
  scheduler: {
    latest_scheduler_heartbeat: "2023-05-19T12:00:36.109924+00:00",
    status: "healthy",
  },
};

const mockPoolsData = {
  pools: [
    {
      description: "Default pool",
      name: "default_pool",
      occupied_slots: 0,
      open_slots: 128,
      queued_slots: 0,
      running_slots: 0,
      scheduled_slots: 0,
      slots: 128,
    },
  ],
  total_entries: 1,
};

const mockDagsData = {
  dags: ["fake-dag-payload1", "fake-dag-payload2"],
  total_entries: 2,
};

const mockDagRunsData = {
  dags: [],
  total_entries: 0,
};

describe("Test ToggleGroups", () => {
  beforeEach(() => {
    jest.spyOn(useHistoricalMetricsDataModule, "default").mockImplementation(
      () =>
        ({
          data: mockHistoricalMetricsData,
          isSuccess: true,
        } as any)
    );

    jest.spyOn(useHealthModule, "default").mockImplementation(
      () =>
        ({
          data: mockHealthData,
          isSuccess: true,
        } as any)
    );

    jest.spyOn(useDagsModule, "default").mockImplementation(
      () =>
        ({
          data: mockDagsData,
          isSuccess: true,
        } as any)
    );

    jest.spyOn(useDagRunsModule, "default").mockImplementation(
      () =>
        ({
          data: mockDagRunsData,
          isSuccess: true,
        } as any)
    );

    jest.spyOn(usePoolsModule, "default").mockImplementation(
      () =>
        ({
          data: mockPoolsData,
          isSuccess: true,
        } as any)
    );
  });

  test("Components renders properly", () => {
    const { getByText, getAllByTestId, getAllByText } = render(
      <ClusterActivity />,
      {
        wrapper: Wrapper,
      }
    );

    expect(getAllByTestId("echart-container")).toHaveLength(4);

    expect(getAllByText("healthy")).toHaveLength(2);
    expect(getByText("Unpaused DAGs")).toBeInTheDocument();
    expect(getByText("No dag running")).toBeInTheDocument();
  });
});
