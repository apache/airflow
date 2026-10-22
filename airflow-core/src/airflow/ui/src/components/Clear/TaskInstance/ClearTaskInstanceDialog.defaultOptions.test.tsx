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
import { render } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY } from "src/constants/localStorage";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import type { ClearTarget } from "./ClearTaskInstanceDialog";
import ClearTaskInstanceDialog from "./ClearTaskInstanceDialog";

const DAG_ID = "test_dag";
const DAG_RUN_ID = "run_1";

const taskInstance: ClearTarget = {
  dag_id: DAG_ID,
  dag_run_id: DAG_RUN_ID,
  id: "test_task_instance",
  in_loop: false,
  map_index: -1,
  task_id: "task_1",
};

const dryRunsMock = vi.fn();

vi.mock("src/queries/useClearTaskInstancesDryRun", () => ({
  useClearTaskInstancesDryRun: () => ({ data: undefined, isFetching: false, isPending: false }),
  useClearTaskInstancesDryRuns: (args: unknown) => {
    dryRunsMock(args);

    return { data: { task_instances: [], total_entries: 0 }, error: null, isFetching: false };
  },
}));

beforeEach(() => {
  server.use(
    http.get(`/api/v2/dags/${DAG_ID}/details`, () => HttpResponse.json({})),
    http.get(`/api/v2/dags/${DAG_ID}/dagRuns/${DAG_RUN_ID}`, () => HttpResponse.json({ dag_versions: [] })),
  );
});
afterEach(() => {
  dryRunsMock.mockClear();
  localStorage.clear();
});

describe("ClearTaskInstanceDialog default options", () => {
  it.each([
    { ignoreDefaultOptions: true, included: false },
    { ignoreDefaultOptions: false, included: true },
  ])(
    "applies the saved default options unless ignoreDefaultOptions=$ignoreDefaultOptions",
    ({ ignoreDefaultOptions, included }) => {
      localStorage.setItem(CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY, JSON.stringify(["past", "downstream"]));
      render(
        <ClearTaskInstanceDialog
          ignoreDefaultOptions={ignoreDefaultOptions}
          onClose={vi.fn()}
          open
          taskInstances={[taskInstance]}
        />,
        { wrapper: Wrapper },
      );

      expect(dryRunsMock.mock.calls[0]?.[0]).toMatchObject({
        requests: [{ requestBody: { include_downstream: included, include_past: included } }],
      });
    },
  );
});
