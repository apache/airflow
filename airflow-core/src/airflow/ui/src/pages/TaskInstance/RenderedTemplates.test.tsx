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
import { render, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { RenderedTemplates } from "./RenderedTemplates";

vi.mock("openapi/queries", () => ({
  useTaskInstanceServiceGetMappedTaskInstance: vi.fn(() => ({
    data: {
      rendered_fields: {
        sql: "select count(*) as total_rows, nvl(count_if(m.a is null), 0) as nulls_count from table as m",
      },
    },
  })),
}));

const getLineSpans = (container: HTMLElement) => {
  const code = container.querySelector("pre code");

  return [...(code?.children ?? [])].filter((child): child is HTMLElement => child.tagName === "SPAN");
};

describe("RenderedTemplates", () => {
  // showLineNumbers + wrapLongLines makes react-syntax-highlighter set each
  // line's wrapper span to `display: flex`, which turns every token inside it
  // into a flex item. Browsers insert a line break between flex items on
  // copy, so a manual copy-paste of the SQL puts each token on its own line.
  // `lineProps` overrides that back to `display: block` so a line stays one
  // block and a copy preserves the source's line breaks.
  it("renders each highlighted line as a block, not a flex container", async () => {
    const { container } = render(<RenderedTemplates />, { wrapper: Wrapper });

    const lineSpans = await waitFor(() => {
      const spans = getLineSpans(container);

      expect(spans.length).toBeGreaterThan(0);

      return spans;
    });

    lineSpans.forEach((span) => {
      expect(span.style.display).toBe("block");
    });
  });
});
