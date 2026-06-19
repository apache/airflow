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
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { TaskNode } from "./TaskNode";
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
      {...({ data: { height: 80, id: "t1", label: "t1", type: "task", width: 200, ...data } } as never)}
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

  it("does not tint a group node (2.x parity: groups keep their own background)", () => {
    expect(renderHtml({ isGroup: true, uiColor: "blue.500" })).toBe(renderHtml({ isGroup: true }));
  });
});
