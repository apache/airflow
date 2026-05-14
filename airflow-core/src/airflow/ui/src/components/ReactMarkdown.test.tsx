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
/// <reference types="@testing-library/jest-dom" />
import "@testing-library/jest-dom/vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import ReactMarkdown from "./ReactMarkdown";

const { renderMermaidDiagramMock } = vi.hoisted(() => ({
  renderMermaidDiagramMock: vi.fn().mockResolvedValue('<svg data-testid="mermaid-svg"></svg>'),
}));

vi.mock("src/context/colorMode", () => ({
  useColorMode: () => ({ colorMode: "light" }),
}));

vi.mock("src/utils/renderMermaid", () => ({
  renderMermaidDiagram: renderMermaidDiagramMock,
}));

describe("ReactMarkdown", () => {
  it("renders basic math integration", () => {
    const markdown = [
      String.raw`Inline math can stay in a sentence, such as $E = \frac{|y - \hat{y}|}{\max(|y|, \epsilon)}$, without leaving the paragraph.`,
      "",
      "$$",
      String.raw`S = \sum_{i=1}^{n} w_i x_i`,
      "$$",
    ].join("\n");
    const { container } = render(
      <BaseWrapper>
        <ReactMarkdown>{markdown}</ReactMarkdown>
      </BaseWrapper>,
    );

    expect(container).toHaveTextContent("Inline math can stay in a sentence, such as");
    expect(container).toHaveTextContent("without leaving the paragraph.");
    expect(container.querySelectorAll(".katex").length).toBeGreaterThanOrEqual(2);
    expect(container.querySelectorAll(".katex-display")).toHaveLength(1);
  });

  it("falls back to a code block when mermaid rendering fails", async () => {
    renderMermaidDiagramMock.mockRejectedValueOnce(new Error("mermaid render failed"));

    const markdown = ["```mermaid", "graph TD", "  A-->B", "```"].join("\n");

    render(
      <BaseWrapper>
        <ReactMarkdown>{markdown}</ReactMarkdown>
      </BaseWrapper>,
    );

    await waitFor(() => expect(screen.getByTestId("markdown-copy-button")).toBeInTheDocument());

    expect(renderMermaidDiagramMock).toHaveBeenCalled();
    expect(screen.queryByTestId("markdown-mermaid-copy-button")).not.toBeInTheDocument();
    expect(screen.queryByTestId("markdown-mermaid-diagram")).not.toBeInTheDocument();
    expect(screen.getByTestId("markdown-code-scroll-area")).toHaveTextContent("A-->B");
  });
});
