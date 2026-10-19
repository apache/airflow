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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { setupServer, type SetupServer } from "msw/node";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { Tasks } from "./Tasks";

const tasks = [
  {
    is_mapped: false,
    operator_name: "BashOperator",
    retries: 0,
    task_id: "extract",
    trigger_rule: "all_success",
  },
  {
    is_mapped: false,
    operator_name: "PythonOperator",
    retries: 2,
    task_id: "transform",
    trigger_rule: "all_success",
  },
  {
    is_mapped: false,
    operator_name: "PythonOperator",
    retries: 0,
    task_id: "load",
    trigger_rule: "all_done",
  },
].map((task) => ({ ...task, task_display_name: task.task_id }));

let server: SetupServer;

beforeAll(() => {
  server = setupServer(
    http.get("/api/v2/dags/:dagId/tasks", () => HttpResponse.json({ tasks, total_entries: tasks.length })),
  );
  server.listen({ onUnhandledFrame: "bypass" });
});
afterAll(() => server.close());

const getTaskNames = () =>
  within(screen.getByTestId("table-list"))
    .queryAllByRole("link")
    .map((link) => link.textContent);

const renderTasks = async () => {
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={["/dags/example_dag/tasks"]}>
        <Routes>
          <Route element={<Tasks />} path="/dags/:dagId/tasks" />
        </Routes>
      </MemoryRouter>
    </BaseWrapper>,
  );

  await waitFor(() => expect(getTaskNames()).toEqual(["extract", "transform", "load"]));
};

const selectOption = async (filterTestId: string, option: string) => {
  fireEvent.click(screen.getByTestId(filterTestId));
  fireEvent.click(await screen.findByRole("option", { name: option }));
};

describe("Dag tasks filters", () => {
  it.each([
    { expected: ["extract"], filterTestId: "operator-filter", option: "BashOperator" },
    { expected: ["transform", "load"], filterTestId: "operator-filter", option: "PythonOperator" },
    { expected: ["load"], filterTestId: "trigger-rule-filter", option: "all_done" },
    { expected: ["extract", "transform"], filterTestId: "trigger-rule-filter", option: "all_success" },
    { expected: ["transform"], filterTestId: "retries-filter", option: "2" },
    { expected: ["extract", "load"], filterTestId: "retries-filter", option: "0" },
  ])(
    "narrows the list to $expected when $filterTestId is $option",
    async ({ expected, filterTestId, option }) => {
      await renderTasks();

      await selectOption(filterTestId, option);

      await waitFor(() => expect(getTaskNames()).toEqual(expected));
    },
  );

  it("narrows the list to tasks matching the searched name", async () => {
    await renderTasks();

    fireEvent.change(screen.getByPlaceholderText("searchTasks"), { target: { value: "trans" } });

    await waitFor(() => expect(getTaskNames()).toEqual(["transform"]));
  });
});
