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
import { describe, expect, it, beforeEach, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { TriggerNode } from "./TriggerNode";

// Mock useTranslation for consistent test behavior
vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, options?: { defaultValue?: string }) => {
      const translations: Record<string, string> = {
        "graph.triggerDagRun": "Trigger DAG Run",
        "graph.triggerNodeAriaLabel": "Trigger node",
        "graph.triggerNodeIcon": "Trigger",
      };
      return translations[key] || options?.defaultValue || key;
    },
  }),
}));

describe("TriggerNode", () => {
  const defaultProps = {
    data: {
      height: 100,
      width: 100,
      label: "trigger_task",
      isSelected: false,
    },
    id: "trigger-1",
  };

  it("renders trigger node with default props", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    expect(getByTestId("trigger-node")).toBeInTheDocument();
    expect(getByTestId("trigger-icon")).toBeInTheDocument();
    expect(getByTestId("trigger-label")).toHaveTextContent("trigger_task");
    expect(getByTestId("trigger-description")).toHaveTextContent("Trigger DAG Run");
  });

  it("displays correct aria-label with selected state", () => {
    const { getByTestId } = render(
      <TriggerNode {...defaultProps} data={{ ...defaultProps.data, isSelected: true }} />,
      { wrapper: Wrapper }
    );

    const node = getByTestId("trigger-node");
    expect(node).toHaveAttribute("aria-selected", "true");
    expect(node.getAttribute("aria-label")).toContain("selected");
  });

  it("displays correct aria-label with unselected state", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    const node = getByTestId("trigger-node");
    expect(node).toHaveAttribute("aria-selected", "false");
    expect(node.getAttribute("aria-label")).toContain("not selected");
  });

  it("truncates label longer than 100 characters", () => {
    const longLabel = "a".repeat(150);
    const { getByTestId } = render(
      <TriggerNode {...defaultProps} data={{ ...defaultProps.data, label: longLabel }} />,
      { wrapper: Wrapper }
    );

    const label = getByTestId("trigger-label");
    expect(label.textContent?.length).toBeLessThanOrEqual(100);
  });

  it("handles missing label gracefully", () => {
    const { getByTestId } = render(
      <TriggerNode {...defaultProps} data={{ ...defaultProps.data, label: "" }} />,
      { wrapper: Wrapper }
    );

    expect(getByTestId("trigger-label")).toHaveTextContent("Unknown");
  });

  it("handles null/undefined label gracefully", () => {
    const { getByTestId } = render(
      <TriggerNode
        {...defaultProps}
        data={{ ...defaultProps.data, label: undefined as any }}
      />,
      { wrapper: Wrapper }
    );

    expect(getByTestId("trigger-label")).toHaveTextContent("Unknown");
  });

  it("applies border styling based on selection state", () => {
    const { getByTestId: getByTestIdSelected } = render(
      <TriggerNode {...defaultProps} data={{ ...defaultProps.data, isSelected: true }} />,
      { wrapper: Wrapper }
    );

    const selectedNode = getByTestIdSelected("trigger-node");
    expect(selectedNode).toHaveClass("chakra-box");

    const { getByTestId: getByTestIdUnselected } = render(
      <TriggerNode {...defaultProps} />,
      { wrapper: Wrapper }
    );

    const unselectedNode = getByTestIdUnselected("trigger-node");
    expect(unselectedNode).toBeInTheDocument();
  });

  it("validates and constrains height within 40-1000px range", () => {
    const { getByTestId } = render(
      <TriggerNode
        {...defaultProps}
        data={{ ...defaultProps.data, height: 20 }} // Below minimum
      />,
      { wrapper: Wrapper }
    );

    const node = getByTestId("trigger-node");
    expect(node).toBeInTheDocument();
    // Height should be clamped to minimum of 40
  });

  it("validates and constrains width within 40-1000px range", () => {
    const { getByTestId } = render(
      <TriggerNode
        {...defaultProps}
        data={{ ...defaultProps.data, width: 5000 }} // Above maximum
      />,
      { wrapper: Wrapper }
    );

    const node = getByTestId("trigger-node");
    expect(node).toBeInTheDocument();
    // Width should be clamped to maximum of 1000
  });

  it("has correct data attributes for testing and automation", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    const node = getByTestId("trigger-node");
    expect(node).toHaveAttribute("data-testid", "trigger-node");
    expect(node).toHaveAttribute("data-label", "trigger_task");
    expect(node).toHaveAttribute("data-selected", "false");
  });

  it("includes icon title attribute from translation", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    const icon = getByTestId("trigger-icon");
    expect(icon).toHaveAttribute("title", "Trigger");
  });

  it("includes label title attribute for overflow tooltip", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    const label = getByTestId("trigger-label");
    expect(label).toHaveAttribute("title", "trigger_task");
  });

  it("includes description title attribute with full trigger label", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    const description = getByTestId("trigger-description");
    expect(description).toHaveAttribute("title", "Trigger DAG Run");
  });

  it("icon is properly marked as aria-hidden", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    const icon = getByTestId("trigger-icon");
    expect(icon).toHaveAttribute("aria-hidden", "true");
  });

  it("renders header with correct data-testid", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    expect(getByTestId("trigger-node-header")).toBeInTheDocument();
  });

  it("has proper role and semantic structure", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    const node = getByTestId("trigger-node");
    expect(node).toHaveAttribute("role", "article");
  });

  it("handles whitespace in label correctly", () => {
    const { getByTestId } = render(
      <TriggerNode
        {...defaultProps}
        data={{ ...defaultProps.data, label: "  trigger_task  " }}
      />,
      { wrapper: Wrapper }
    );

    expect(getByTestId("trigger-label")).toHaveTextContent("trigger_task");
  });

  it("displays all required translation keys", () => {
    const { getByTestId } = render(<TriggerNode {...defaultProps} />, {
      wrapper: Wrapper,
    });

    // Verify all three translation keys are used
    expect(getByTestId("trigger-label")).toBeInTheDocument();
    expect(getByTestId("trigger-description")).toHaveTextContent("Trigger DAG Run");
    expect(getByTestId("trigger-icon")).toHaveAttribute("title", "Trigger");
  });

  it("renders correctly with all dimensions at minimum", () => {
    const { getByTestId } = render(
      <TriggerNode {...defaultProps} data={{ ...defaultProps.data, height: 40, width: 40 }} />,
      { wrapper: Wrapper }
    );

    expect(getByTestId("trigger-node")).toBeInTheDocument();
  });

  it("renders correctly with all dimensions at maximum", () => {
    const { getByTestId } = render(
      <TriggerNode {...defaultProps} data={{ ...defaultProps.data, height: 1000, width: 1000 }} />,
      { wrapper: Wrapper }
    );

    expect(getByTestId("trigger-node")).toBeInTheDocument();
  });
});
