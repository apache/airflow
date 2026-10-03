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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeAll, expect, it, vi } from "vitest";

import { TaskInstanceService } from "openapi/requests";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import dagTranslations from "../../../../public/i18n/locales/en/dag.json";
import { ClearExecutionDialog } from "./ClearExecutionDialog";

beforeAll(() => i18n.addResourceBundle("en", "dag", dagTranslations, true, true));
afterEach(() => vi.restoreAllMocks());

it("keeps stale UUID scope visible when the clear preview rejects an archived execution", async () => {
  const clear = vi
    .spyOn(TaskInstanceService, "postClearTaskInstances")
    .mockRejectedValue({ body: { detail: "Selected execution was archived" }, status: 409 });
  const onClose = vi.fn();

  render(
    <ClearExecutionDialog
      dagId="dag"
      executions={[
        {
          id: "archived-uuid",
          map_index: -1,
          region_id: "11111111-1111-1111-1111-111111111111",
          region_index: 3,
          task_display_name: "work",
          task_id: "body.work",
        },
      ]}
      onClose={onClose}
      open
      runId="run"
    />,
    { wrapper: Wrapper },
  );
  expect(await screen.findByText("Selected execution was archived")).toBeVisible();
  expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeDisabled();
  expect(clear).toHaveBeenCalledTimes(1);
  expect(clear.mock.lastCall?.[0].requestBody).toMatchObject({
    dry_run: true,
    task_instance_ids: ["archived-uuid"],
  });
  expect(onClose).not.toHaveBeenCalled();
});

it("previews and clears UUID seeds with separate whole-expansion and later-loop intent", async () => {
  const clear = vi
    .spyOn(TaskInstanceService, "postClearTaskInstances")
    .mockResolvedValue({ task_instances: [], total_entries: 0 });

  render(
    <ClearExecutionDialog
      dagId="dag"
      executions={[
        {
          id: "selected-uuid",
          map_index: 1,
          region_id: "11111111-1111-1111-1111-111111111111",
          region_index: 1,
          task_display_name: "mapped",
          task_id: "body.mapped",
        },
      ]}
      onClose={vi.fn()}
      open
      runId="run"
    />,
    { wrapper: Wrapper },
  );

  await waitFor(() =>
    expect(clear.mock.lastCall?.[0]).toMatchObject({
      dagId: "dag",
      requestBody: {
        dag_run_id: "run",
        dry_run: true,
        include_downstream: true,
        include_later_loop_iterations: true,
        only_failed: false,
        task_instance_ids: ["selected-uuid"],
        whole_expansion_ids: [],
      },
    }),
  );
  fireEvent.click(screen.getByLabelText("Clear whole mapped expansions"));
  fireEvent.click(screen.getByLabelText("Clear later loop iterations"));
  await waitFor(() =>
    expect(clear.mock.lastCall?.[0]).toMatchObject({
      dagId: "dag",
      requestBody: {
        dry_run: true,
        include_later_loop_iterations: false,
        whole_expansion_ids: ["selected-uuid"],
      },
    }),
  );
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "Clear selected executions" })).toBeEnabled(),
  );
  fireEvent.click(screen.getByRole("button", { name: "Clear selected executions" }));
  await waitFor(() =>
    expect(
      clear.mock.calls.map(([request]) => request).find((request) => request.requestBody.dry_run === false),
    ).toMatchObject({
      dagId: "dag",
      requestBody: {
        dry_run: false,
        include_downstream: true,
        include_later_loop_iterations: false,
        task_instance_ids: ["selected-uuid"],
        whole_expansion_ids: ["selected-uuid"],
      },
    }),
  );
});
