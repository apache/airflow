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
import { render } from "@testing-library/react";
import { useParams } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";

import * as queries from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";
import { SyntaxHighlighter } from "src/utils/syntaxHighlighter";

import { RenderedTemplates } from "./RenderedTemplates";

vi.mock("openapi/queries");
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: vi.fn(),
  };
});
vi.mock("src/utils/syntaxHighlighter", () => ({
  oneDark: {},
  oneLight: {},
  SyntaxHighlighter: vi.fn(() => undefined),
}));

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
      data: {
        rendered_fields: {
          sql: "SELECT * FROM users",
        },
      },
    } as unknown as ReturnType<typeof queries.useTaskInstanceServiceGetMappedTaskInstance>);
  });

  it("forces each highlighted line to display as a block", async () => {
    // With wrapLongLines + showLineNumbers, react-syntax-highlighter defaults each line to
    // `display: flex`, which blockifies every token span inside it and makes browsers insert
    // a newline around each one on manual copy. `lineProps` overrides that default.
    render(
      <Wrapper>
        <RenderedTemplates />
      </Wrapper>,
    );

    await vi.waitFor(() => expect(vi.mocked(SyntaxHighlighter)).toHaveBeenCalled());

    const [props] = vi.mocked(SyntaxHighlighter).mock.lastCall as [{ lineProps?: unknown }, unknown];

    expect(props.lineProps).toEqual({ style: { display: "block" } });
  });
});
