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
import { useParams } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as queries from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";

import { RenderedTemplates } from "./RenderedTemplates";

vi.mock("openapi/queries");
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return { ...actual, useParams: vi.fn() };
});

const SQL = `select
    count(*) as total_rows,
    nvl(count_if(m.a is null), 0) as nulls_count
from my_table as m`;

describe("RenderedTemplates", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(useParams).mockReturnValue({
      dagId: "test-dag",
      mapIndex: "-1",
      runId: "test-run",
      taskId: "test-task",
    });
    vi.mocked(queries.useTaskInstanceServiceGetMappedTaskInstance).mockReturnValue({
      data: { rendered_fields: { sql: SQL } },
    } as unknown as ReturnType<typeof queries.useTaskInstanceServiceGetMappedTaskInstance>);
  });

  it("lays out highlighted lines as blocks so copied text keeps its tokens on one line", async () => {
    const { container } = render(
      <Wrapper>
        <RenderedTemplates />
      </Wrapper>,
    );

    await screen.findByText("sql");

    const lines = container.querySelectorAll("code > span");

    expect(lines.length).toBeGreaterThan(0);
    lines.forEach((line) => {
      expect(line).toHaveStyle({ display: "block" });
    });
  });

  it("overrides the theme's white-space so long lines actually wrap", async () => {
    const { container } = render(
      <Wrapper>
        <RenderedTemplates />
      </Wrapper>,
    );

    await screen.findByText("sql");

    expect(container.querySelector("code")).toHaveStyle({ whiteSpace: "pre-wrap" });
  });
});
