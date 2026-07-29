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
import { render, screen } from "@testing-library/react";
import type { CSSProperties, PropsWithChildren } from "react";
import { describe, expect, it, vi } from "vitest";

import { TagFilter } from "./TagFilter";

type StyleProps = PropsWithChildren<{
  readonly maxWidth?: string;
  readonly minWidth?: string;
}>;

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

vi.mock("@chakra-ui/react", () => ({
  Box: ({ children, maxWidth, minWidth }: StyleProps) => (
    <div data-max-width={maxWidth} data-testid="tag-filter" style={{ maxWidth, minWidth }}>
      {children}
    </div>
  ),
  Field: {
    Root: ({ children }: PropsWithChildren) => <div>{children}</div>,
  },
  HStack: ({ children }: PropsWithChildren) => <div>{children}</div>,
  Text: ({ children }: PropsWithChildren) => <span>{children}</span>,
}));

vi.mock("src/components/ui", () => ({
  Switch: () => <button aria-label="Tag match mode" type="button" />,
}));

vi.mock("chakra-react-select", () => ({
  Select: ({
    chakraStyles,
  }: {
    readonly chakraStyles: { readonly container: (provided: CSSProperties) => CSSProperties };
  }) => {
    const containerStyles = chakraStyles.container({});

    return (
      <div data-max-width={containerStyles.maxWidth} data-testid="select-container" style={containerStyles} />
    );
  },
}));

const renderTagFilter = () =>
  render(
    <TagFilter
      onMenuScrollToBottom={vi.fn()}
      onMenuScrollToTop={vi.fn()}
      onSelectTagsChange={vi.fn()}
      onTagModeChange={vi.fn()}
      onUpdate={vi.fn()}
      selectedTags={[]}
      tagFilterMode="any"
      tags={["example"]}
    />,
  );

describe("TagFilter", () => {
  it("allows overriding the max width with a CSS custom property", () => {
    renderTagFilter();

    expect(screen.getByTestId("tag-filter")).toHaveAttribute(
      "data-max-width",
      "var(--airflow-dag-tag-filter-max-width, 300px)",
    );
    expect(screen.getByTestId("select-container")).toHaveAttribute(
      "data-max-width",
      "var(--airflow-dag-tag-filter-max-width, 300px)",
    );
  });
});
