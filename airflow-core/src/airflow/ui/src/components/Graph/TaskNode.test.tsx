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
import { render } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import type { ComponentProps, ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { TaskNode } from "./TaskNode";
import { readableTextForFill } from "./nodeColors";
import type { CustomNodeProps } from "./reactflowUtils";

vi.mock("src/context/groups", () => ({
  useGroups: vi.fn(() => ({ toggleGroupId: vi.fn() })),
}));

const TestWrapper = ({ children }: { readonly children: ReactNode }) => (
  <Wrapper>
    <ReactFlowProvider>{children}</ReactFlowProvider>
  </Wrapper>
);

// Chakra/Panda hashes color props into atomic class names rather than inline styles, so the resolved
// colour cannot be read back in jsdom. Instead we render two otherwise-identical nodes that differ
// only in the prop under test: any markup difference is attributable to that prop (hashing is
// deterministic), and identical markup proves the prop had no effect.
const renderHtml = (data: Partial<CustomNodeProps>): string => {
  const { container } = render(
    // The xyflow NodeProps surface is large; the component only reads `data` and `id`.
    <TaskNode
      {...({
        data: { height: 80, id: "t1", label: "t1", type: "task", width: 200, ...data },
      } as unknown as ComponentProps<typeof TaskNode>)}
    />,
    { wrapper: TestWrapper },
  );

  return container.innerHTML;
};

describe("TaskNode operator colors", () => {
  it("tints a leaf task when ui_color is set", () => {
    expect(renderHtml({ operator: "BashOperator", uiColor: "blue.500" })).not.toBe(
      renderHtml({ operator: "BashOperator" }),
    );
  });

  it("colors the operator text when ui_fgcolor is set", () => {
    expect(renderHtml({ operator: "BashOperator", uiFgcolor: "red.700" })).not.toBe(
      renderHtml({ operator: "BashOperator" }),
    );
  });

  it("tints a leaf task when ui_color is a raw hex color", () => {
    expect(renderHtml({ operator: "BashOperator", uiColor: "#e8b7e4" })).not.toBe(
      renderHtml({ operator: "BashOperator" }),
    );
  });

  it("tints a group node when ui_color is a token (2.x parity: ui_color is the group fill)", () => {
    expect(renderHtml({ isGroup: true, uiColor: "blue.500" })).not.toBe(renderHtml({ isGroup: true }));
  });

  it("alternates the group fill shade by nesting depth so nested groups stay distinct", () => {
    expect(renderHtml({ depth: 0, isGroup: true, isOpen: true, uiColor: "blue.500" })).not.toBe(
      renderHtml({ depth: 1, isGroup: true, isOpen: true, uiColor: "blue.500" }),
    );
  });
});

describe("readableTextForFill", () => {
  it.each([
    { color: "#ffffff", expected: "black" },
    { color: "#fff", expected: "black" },
    { color: "#000000", expected: "gray.50" },
    { color: "#e8b7e4", expected: "black" },
    { color: "#1f77b4", expected: "gray.50" },
    { color: "blue.500", expected: undefined },
    { color: undefined, expected: undefined },
  ])("returns $expected for a fill of $color", ({ color, expected }) => {
    expect(readableTextForFill(color)).toBe(expected);
  });
});
