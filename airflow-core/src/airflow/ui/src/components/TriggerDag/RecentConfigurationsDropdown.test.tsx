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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";

import { Wrapper } from "src/utils/Wrapper";
import type { RecentConfiguration } from "src/queries/useRecentConfigurations";

import { RecentConfigurationsDropdown } from "./RecentConfigurationsDropdown";

// Mock the useRecentConfigurations hook
vi.mock("src/queries/useRecentConfigurations", () => ({
  useRecentConfigurations: vi.fn(),
}));

// Import the mocked hook
import { useRecentConfigurations } from "src/queries/useRecentConfigurations";
const mockedUseRecentConfigurations = vi.mocked(useRecentConfigurations);

describe("RecentConfigurationsDropdown", () => {
  const mockOnConfigSelect = vi.fn();
  const defaultProps = {
    dagId: "test_dag",
    onConfigSelect: mockOnConfigSelect,
    selectedConfig: null,
  };

  const mockConfigurations: RecentConfiguration[] = [
    {
      run_id: "manual_1",
      conf: { param1: "value1", param2: "value2" },
      logical_date: "2024-01-01T10:00:00Z",
      start_date: "2024-01-01T10:00:00Z",
    },
    {
      run_id: "manual_2",
      conf: { param1: "value3", param2: "value4" },
      logical_date: "2024-01-01T09:00:00Z",
      start_date: "2024-01-01T09:00:00Z",
    },
  ];

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders loading state", () => {
    mockedUseRecentConfigurations.mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as any);

    render(<RecentConfigurationsDropdown {...defaultProps} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText("components:triggerDag.recentConfigs.loading")).toBeDefined();
  });

  it("renders error state", () => {
    mockedUseRecentConfigurations.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error("API Error"),
    } as any);

    render(<RecentConfigurationsDropdown {...defaultProps} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText("components:triggerDag.recentConfigs.error")).toBeDefined();
  });

  it("renders no configurations message when empty", () => {
    mockedUseRecentConfigurations.mockReturnValue({
      data: { configurations: [] },
      isLoading: false,
      error: null,
    } as any);

    render(<RecentConfigurationsDropdown {...defaultProps} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText("components:triggerDag.recentConfigs.noConfigs")).toBeDefined();
  });

  it("renders configurations dropdown when data is available", () => {
    mockedUseRecentConfigurations.mockReturnValue({
      data: { configurations: mockConfigurations },
      isLoading: false,
      error: null,
    } as any);

    render(<RecentConfigurationsDropdown {...defaultProps} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText("components:triggerDag.recentConfigs.label")).toBeDefined();
    expect(screen.getByRole("combobox")).toBeDefined(); // Check for select element
    expect(screen.getByText("components:triggerDag.recentConfigs.help")).toBeDefined();
  });

  it("calls onConfigSelect when a configuration is selected", async () => {
    mockedUseRecentConfigurations.mockReturnValue({
      data: { configurations: mockConfigurations },
      isLoading: false,
      error: null,
    } as any);

    render(<RecentConfigurationsDropdown {...defaultProps} />, {
      wrapper: Wrapper,
    });

    const select = screen.getByRole("combobox");
    fireEvent.change(select, { target: { value: "manual_1" } });

    await waitFor(() => {
      expect(mockOnConfigSelect).toHaveBeenCalledWith(mockConfigurations[0]);
    });
  });

  it("shows clear button when a configuration is selected", () => {
    mockedUseRecentConfigurations.mockReturnValue({
      data: { configurations: mockConfigurations },
      isLoading: false,
      error: null,
    } as any);

    const propsWithSelected = {
      ...defaultProps,
      selectedConfig: mockConfigurations[0] as RecentConfiguration | null,
    };

    render(<RecentConfigurationsDropdown {...propsWithSelected} />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText("components:triggerDag.recentConfigs.clear")).toBeDefined();
  });

  it("calls onConfigSelect with null when clear button is clicked", async () => {
    mockedUseRecentConfigurations.mockReturnValue({
      data: { configurations: mockConfigurations },
      isLoading: false,
      error: null,
    } as any);

    const propsWithSelected = {
      ...defaultProps,
      selectedConfig: mockConfigurations[0] as RecentConfiguration | null,
    };

    render(<RecentConfigurationsDropdown {...propsWithSelected} />, {
      wrapper: Wrapper,
    });

    const clearButton = screen.getByText("components:triggerDag.recentConfigs.clear");
    fireEvent.click(clearButton);

    await waitFor(() => {
      expect(mockOnConfigSelect).toHaveBeenCalledWith(null);
    });
  });

  it("displays configuration options with run_id and logical_date", () => {
    mockedUseRecentConfigurations.mockReturnValue({
      data: { configurations: mockConfigurations },
      isLoading: false,
      error: null,
    } as any);

    render(<RecentConfigurationsDropdown {...defaultProps} />, {
      wrapper: Wrapper,
    });

    const select = screen.getByRole("combobox");
    const options = Array.from(select.querySelectorAll("option"));
    
    // Check that options contain run_id and formatted date
    expect(options[0]?.textContent).toContain("manual_1");
    expect(options[0]?.textContent).toContain("2024-01-01T10:00:00Z");
    expect(options[1]?.textContent).toContain("manual_2");
    expect(options[1]?.textContent).toContain("2024-01-01T09:00:00Z");
  });
});
