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
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import commonLocale from "../../public/i18n/locales/en/common.json";
import componentsLocale from "../../public/i18n/locales/en/components.json";
import RenderedJsonField from "./RenderedJsonField";

vi.mock("src/components/MonacoEditor", () => ({
  default: ({ value }: { readonly value?: string }) => <div data-testid="monaco-editor">{value}</div>,
}));

vi.mock("src/context/colorMode", () => ({
  useMonacoTheme: () => ({ beforeMount: vi.fn(), theme: "airflow-light" }),
}));

const expandLabel = () => i18n.t("expand.expand", { ns: "common" });

describe("RenderedJsonField", () => {
  beforeAll(() => {
    i18n.addResourceBundle("en", "common", commonLocale, true, true);
    i18n.addResourceBundle("en", "components", componentsLocale, true, true);
  });

  it("previews a collapsed object as badges instead of the editor", () => {
    render(<RenderedJsonField collapsed content={{ env: "prod", score: 0.99 }} />, { wrapper: Wrapper });

    expect(screen.getByText("score:")).toBeInTheDocument();
    expect(screen.getByText("0.99")).toBeInTheDocument();
    expect(screen.queryByTestId("monaco-editor")).not.toBeInTheDocument();
  });

  it("expands a summarised value to the editor", () => {
    render(<RenderedJsonField collapsed content={{ options: { deep: true } }} />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: expandLabel() }));

    expect(screen.getByTestId("monaco-editor")).toBeInTheDocument();
    expect(screen.queryByTestId("json-preview-badges")).not.toBeInTheDocument();
  });

  it("offers no expand control once the badges show everything", () => {
    render(<RenderedJsonField collapsed content={{ alpha: 1, bravo: 2, charlie: 3, delta: 4, echo: 5 }} />, {
      wrapper: Wrapper,
    });

    expect(screen.getAllByText(/^(?:alpha|bravo|charlie|delta|echo):$/u)).toHaveLength(5);
    expect(screen.queryByTestId("json-preview-more")).not.toBeInTheDocument();
  });

  it("keeps entries that overflow the line behind a count that expands the full JSON", () => {
    const wide = "x".repeat(20);

    render(
      <RenderedJsonField collapsed content={{ alpha: wide, bravo: wide, charlie: wide, delta: wide }} />,
      { wrapper: Wrapper },
    );

    expect(screen.getByText("alpha:")).toBeInTheDocument();
    expect(screen.queryByText("delta:")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("json-preview-more"));

    expect(screen.getByTestId("monaco-editor")).toBeInTheDocument();
  });

  it("summarises an array of objects on one line instead of rendering the editor", () => {
    render(<RenderedJsonField collapsed content={[{ id: 1 }, { id: 2 }]} />, { wrapper: Wrapper });

    expect(screen.getByText(i18n.t("jsonPreview.items", { count: 2, ns: "components" }))).toBeInTheDocument();
    expect(screen.queryByTestId("monaco-editor")).not.toBeInTheDocument();
  });

  it.each([
    ["an empty object", {}],
    ["an empty array", []],
  ])("renders nothing for %s", (_label, content) => {
    const { container } = render(<RenderedJsonField collapsed content={content} />, { wrapper: Wrapper });

    expect(container).toBeEmptyDOMElement();
  });

  it("renders the editor when the caller never collapses it", () => {
    render(<RenderedJsonField content={{ score: 0.99 }} />, { wrapper: Wrapper });

    expect(screen.getByTestId("monaco-editor")).toBeInTheDocument();
    expect(screen.queryByTestId("json-preview-badges")).not.toBeInTheDocument();
  });

  it("collapses back to badges from the expanded editor", () => {
    render(<RenderedJsonField collapsed content={{ options: { deep: true } }} />, { wrapper: Wrapper });

    fireEvent.click(screen.getByTestId("json-preview-more"));
    expect(screen.getByTestId("monaco-editor")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("json-preview-collapse"));

    expect(screen.getByText("options:")).toBeInTheDocument();
    expect(screen.queryByTestId("monaco-editor")).not.toBeInTheDocument();
  });

  it("offers no collapse control when the caller never collapses it", () => {
    render(<RenderedJsonField content={{ score: 0.99 }} />, { wrapper: Wrapper });

    expect(screen.queryByTestId("json-preview-collapse")).not.toBeInTheDocument();
  });

  it("collapses back to badges when the caller collapses all", () => {
    const { rerender } = render(<RenderedJsonField collapsed={false} content={{ score: 0.99 }} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByTestId("monaco-editor")).toBeInTheDocument();

    rerender(<RenderedJsonField collapsed content={{ score: 0.99 }} />);

    expect(screen.getByText("score:")).toBeInTheDocument();
    expect(screen.queryByTestId("monaco-editor")).not.toBeInTheDocument();
  });

  it("truncates long values and keeps the full value in a tooltip", () => {
    const long = "x".repeat(60);

    render(<RenderedJsonField collapsed content={{ sql: long }} />, { wrapper: Wrapper });

    expect(screen.getByText(`${"x".repeat(40)}…`)).toBeInTheDocument();
  });
});
