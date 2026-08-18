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
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { TaskLink } from "./TaskLink";

describe("TaskLink", () => {
  it("removes the selected try when linking to another task", () => {
    render(
      <MemoryRouter
        initialEntries={["/dags/test_dag/runs/test_run/tasks/three_tries?try_number=2&log_level=error"]}
      >
        <Routes>
          <Route
            element={<TaskLink id="one_try" label="one_try" />}
            path="/dags/:dagId/runs/:runId/tasks/:taskId"
          />
        </Routes>
      </MemoryRouter>,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByRole("link", { name: "one_try" })).toHaveAttribute(
      "href",
      "/dags/test_dag/runs/test_run/tasks/one_try?log_level=error",
    );
  });
});
