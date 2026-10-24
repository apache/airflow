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
import { render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it } from "vitest";

import type { TaskInstanceResponse, TaskResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import commonLocale from "../../../public/i18n/locales/en/common.json";
import { CancelConditions } from "./CancelConditions";

const taskUrl = "/api/v2/dags/:dagId/tasks/:taskId";

const taskInstance = {
  dag_id: "preview_environment",
  dag_version: { version_number: 3 },
  task_id: "run_e2e_tests",
} as unknown as TaskInstanceResponse;

/** Answer with a task that has `cancel_on` and `cancel_state`, recording the URL of each request. */
const serveTask = (task: Pick<TaskResponse, "cancel_on" | "cancel_state">) => {
  const requests: Array<URL> = [];

  server.use(
    http.get(taskUrl, ({ request }) => {
      requests.push(new URL(request.url));

      return HttpResponse.json({ task_id: "run_e2e_tests", ...task });
    }),
  );

  return requests;
};

describe("CancelConditions", () => {
  // Without the bundle i18n.t() echoes the key, so the text assertions below would pass blindly.
  beforeEach(() => {
    i18n.addResourceBundle("en", "common", commonLocale, true, true);
  });

  it("shows the task's conditions in the Dag's latest version", async () => {
    const requests = serveTask({
      cancel_on: [{ correlation_key_conf: null, events: ["pull_request"], match: {}, webhook: "github" }],
      cancel_state: "skipped",
    });

    render(<CancelConditions taskInstance={taskInstance} />, { wrapper: Wrapper });

    const panel = await screen.findByTestId("cancel-conditions");

    expect(requests.at(-1)?.pathname).toBe("/api/v2/dags/preview_environment/tasks/run_e2e_tests");
    expect(requests.at(-1)?.searchParams.has("version_number")).toBe(false);
    expect(within(panel).getByText("Cancel On")).toBeInTheDocument();
    expect(
      within(panel).getByText(
        "A delivery that matches one of these sets this task to Skipped in the queued and running runs " +
          "of this Dag that are about the delivery's correlation key: those started from an event that an " +
          "earlier delivery recorded, including a run the scheduler creates later from such an event, and " +
          "those queued before the delivery with the key in their conf.",
      ),
    ).toBeInTheDocument();
    expect(within(panel).getByRole("link", { name: "github" })).toBeInTheDocument();
  });

  it.each([
    { cancel_on: null, cancel_state: null },
    { cancel_on: [], cancel_state: null },
  ])("shows nothing for a task with cancel_on $cancel_on", async (task) => {
    const requests = serveTask(task);

    render(<CancelConditions taskInstance={taskInstance} />, { wrapper: Wrapper });

    await waitFor(() => expect(requests).toHaveLength(1));
    expect(screen.queryByTestId("cancel-conditions")).not.toBeInTheDocument();
  });

  it("asks for no task before the task instance is loaded", async () => {
    const requested: Array<string> = [];

    server.use(
      http.get("*/api/v2/dags/*", ({ request }) => {
        requested.push(new URL(request.url).pathname);

        return HttpResponse.json({ cancel_on: null, cancel_state: null, task_id: "run_e2e_tests" });
      }),
    );

    const { rerender } = render(<CancelConditions taskInstance={undefined} />, { wrapper: Wrapper });

    rerender(<CancelConditions taskInstance={taskInstance} />);

    await waitFor(() => expect(requested).toContain("/api/v2/dags/preview_environment/tasks/run_e2e_tests"));
    expect(requested).toEqual(["/api/v2/dags/preview_environment/tasks/run_e2e_tests"]);
    expect(screen.queryByTestId("cancel-conditions")).not.toBeInTheDocument();
  });
});
