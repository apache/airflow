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
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { SyntaxHighlighter as HighlightSyntaxHighlighter } from "src/utils/syntaxHighlighter";
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
  it("supports a broad highlight.js language set", () => {
    const { supportedLanguages } = HighlightSyntaxHighlighter as unknown as {
      supportedLanguages: Array<string>;
    };

    expect(supportedLanguages).toEqual(expect.arrayContaining(["bash", "javascript", "typescript", "go", "rust"]));
  });

  it("renders inline code spans as inline code", () => {
    const markdown = "Text with `inline_code` inside a paragraph.";

    render(
      <BaseWrapper>
        <ReactMarkdown>{markdown}</ReactMarkdown>
      </BaseWrapper>,
    );

    const inlineCode = screen.getByText("inline_code", { selector: "code" });

    expect(inlineCode).toBeInTheDocument();
    expect(inlineCode.closest("pre")).toBeNull();
    expect(screen.getByText(/Text with/iu)).toBeInTheDocument();
    expect(screen.getByText(/inside a paragraph/iu)).toBeInTheDocument();
  });

  it("renders fenced code blocks with line numbers and copy action", () => {
    const markdown = [
      "```javascript",
      'const longLine = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";',
      "console.log(longLine);",
      "```",
    ].join("\n");
    const { container } = render(
      <BaseWrapper>
        <ReactMarkdown>{markdown}</ReactMarkdown>
      </BaseWrapper>,
    );

    const codeElement = container.querySelector("code");

    expect(screen.getByText("javascript")).toBeInTheDocument();
    expect(screen.getByTestId("markdown-copy-button")).toBeInTheDocument();
    expect(screen.getByLabelText(i18n.t("components:clipboard.copy"))).toBeInTheDocument();
    expect(screen.getByText("console")).toBeInTheDocument();
    expect(container.querySelectorAll(".react-syntax-highlighter-line-number")).toHaveLength(2);
    expect(screen.getByTestId("markdown-code-scroll-area")).toHaveStyle({ overflowX: "auto", overflowY: "hidden", width: "100%" });
    expect(screen.getByTestId("markdown-code-content")).toHaveStyle({ display: "inline-block", minWidth: "100%" });
    expect(codeElement).toHaveStyle({ overflowWrap: "normal", whiteSpace: "pre", wordBreak: "normal" });
  });

  it("stretches the markdown root inside flex layouts without widening the document", () => {
    const markdown = [
      "```javascript",
      'const longLine = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";',
      "console.log(longLine);",
      "```",
    ].join("\n");

    render(
      <BaseWrapper>
        <div data-testid="markdown-host" style={{ alignItems: "flex-start", display: "flex", flexDirection: "column", width: "320px" }}>
          <ReactMarkdown>{markdown}</ReactMarkdown>
        </div>
      </BaseWrapper>,
    );

    const markdownRoot = screen.getByTestId("markdown-host").firstElementChild;

    expect(markdownRoot).toHaveStyle({ alignSelf: "stretch", maxWidth: "100%", minWidth: "0", width: "100%" });
    expect(screen.getByTestId("markdown-code-scroll-area")).toHaveStyle({ overflowX: "auto", width: "100%" });
  });

  it("renders inline math within text and block math as display content", () => {
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

  it("renders mermaid fenced blocks as diagrams", () => {
    const markdown = ["```mermaid", "graph TD", "  A-->B", "```"].join("\n");

    render(
      <BaseWrapper>
        <ReactMarkdown>{markdown}</ReactMarkdown>
      </BaseWrapper>,
    );

    expect(screen.getByLabelText(i18n.t("components:clipboard.copy"))).toBeInTheDocument();
    expect(renderMermaidDiagramMock).toHaveBeenCalled();
    const loadingState = screen.getByTestId("markdown-mermaid-loading");

    expect(loadingState).toBeInTheDocument();
    expect(loadingState.parentElement).toHaveStyle({
      maxWidth: "100%",
      overflow: "hidden",
      width: "100%",
    });
  });
});