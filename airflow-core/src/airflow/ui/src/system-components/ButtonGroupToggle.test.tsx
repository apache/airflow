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
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { ButtonGroupToggle } from "./ButtonGroupToggle";

describe("ButtonGroupToggle", () => {
  const options = [
    { label: "All", value: "all" },
    { label: "Active", value: "active" },
    { label: "Paused", value: "paused" },
  ];

  it("renders all options", () => {
    render(<ButtonGroupToggle onChange={vi.fn()} options={options} value="all" />, {
      wrapper: BaseWrapper,
    });

    expect(screen.getByText("All")).toBeInTheDocument();
    expect(screen.getByText("Active")).toBeInTheDocument();
    expect(screen.getByText("Paused")).toBeInTheDocument();
  });

  it("calls onChange when clicking a button", () => {
    const onChange = vi.fn();

    render(<ButtonGroupToggle onChange={onChange} options={options} value="all" />, {
      wrapper: BaseWrapper,
    });

    fireEvent.click(screen.getByText("Active"));

    expect(onChange).toHaveBeenCalledWith("active");
  });

  it("renders disabled options", () => {
    const optionsWithDisabled = [
      { label: "All", value: "all" },
      { disabled: true, label: "Disabled", value: "disabled" },
    ];

    render(<ButtonGroupToggle onChange={vi.fn()} options={optionsWithDisabled} value="all" />, {
      wrapper: BaseWrapper,
    });

    expect(screen.getByText("Disabled")).toBeDisabled();
  });

  it("supports render function labels", () => {
    const optionsWithRenderFn = [
      { label: "All", value: "all" },
      {
        label: (isSelected: boolean) => (isSelected ? "Selected!" : "Not Selected"),
        value: "toggle",
      },
    ];

    const { rerender } = render(
      <ButtonGroupToggle onChange={vi.fn()} options={optionsWithRenderFn} value="all" />,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByText("Not Selected")).toBeInTheDocument();

    rerender(<ButtonGroupToggle onChange={vi.fn()} options={optionsWithRenderFn} value="toggle" />);

    expect(screen.getByText("Selected!")).toBeInTheDocument();
  });
});
