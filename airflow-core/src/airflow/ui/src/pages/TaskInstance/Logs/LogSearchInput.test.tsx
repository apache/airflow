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
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { LogSearchInput } from "./LogSearchInput";

const defaultProps = {
  currentMatchIndex: 0,
  onSearchChange: vi.fn(),
  onSearchNext: vi.fn(),
  onSearchPrevious: vi.fn(),
  searchQuery: "",
  totalMatches: 0,
};

describe("LogSearchInput", () => {
  describe("initial render", () => {
    it("renders the search input", () => {
      render(<LogSearchInput {...defaultProps} />, { wrapper: Wrapper });
      expect(screen.getByTestId("log-search-input")).toBeInTheDocument();
    });

    it("does not show match counter or navigation buttons when searchQuery is empty", () => {
      render(<LogSearchInput {...defaultProps} />, { wrapper: Wrapper });
      expect(screen.queryByRole("button", { name: /next match/iu })).toBeNull();
      expect(screen.queryByRole("button", { name: /previous match/iu })).toBeNull();
    });

    it("shows match counter and navigation buttons when searchQuery is non-empty", () => {
      render(<LogSearchInput {...defaultProps} searchQuery="error" totalMatches={3} />, {
        wrapper: Wrapper,
      });
      expect(screen.getByRole("button", { name: /next match/iu })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /previous match/iu })).toBeInTheDocument();
    });
  });

  describe("keyboard navigation", () => {
    it("calls onSearchNext when Enter is pressed in the input", () => {
      const onSearchNext = vi.fn();

      render(
        <LogSearchInput {...defaultProps} onSearchNext={onSearchNext} searchQuery="info" totalMatches={5} />,
        {
          wrapper: Wrapper,
        },
      );

      fireEvent.keyDown(screen.getByTestId("log-search-input"), { key: "Enter" });
      expect(onSearchNext).toHaveBeenCalledTimes(1);
    });

    it("calls onSearchPrevious when Shift+Enter is pressed", () => {
      const onSearchPrevious = vi.fn();

      render(
        <LogSearchInput
          {...defaultProps}
          onSearchPrevious={onSearchPrevious}
          searchQuery="info"
          totalMatches={5}
        />,
        { wrapper: Wrapper },
      );

      fireEvent.keyDown(screen.getByTestId("log-search-input"), { key: "Enter", shiftKey: true });
      expect(onSearchPrevious).toHaveBeenCalledTimes(1);
    });

    it("calls onSearchChange with empty string when Escape is pressed", () => {
      const onSearchChange = vi.fn();

      render(
        <LogSearchInput
          {...defaultProps}
          onSearchChange={onSearchChange}
          searchQuery="warning"
          totalMatches={2}
        />,
        {
          wrapper: Wrapper,
        },
      );

      fireEvent.keyDown(screen.getByTestId("log-search-input"), { key: "Escape" });
      expect(onSearchChange).toHaveBeenCalledWith("");
    });
  });

  describe("navigation buttons", () => {
    it("calls onSearchNext when the next-match button is clicked", () => {
      const onSearchNext = vi.fn();

      render(
        <LogSearchInput {...defaultProps} onSearchNext={onSearchNext} searchQuery="task" totalMatches={4} />,
        {
          wrapper: Wrapper,
        },
      );

      fireEvent.click(screen.getByRole("button", { name: /next match/iu }));
      expect(onSearchNext).toHaveBeenCalledTimes(1);
    });

    it("calls onSearchPrevious when the previous-match button is clicked", () => {
      const onSearchPrevious = vi.fn();

      render(
        <LogSearchInput
          {...defaultProps}
          onSearchPrevious={onSearchPrevious}
          searchQuery="task"
          totalMatches={4}
        />,
        {
          wrapper: Wrapper,
        },
      );

      fireEvent.click(screen.getByRole("button", { name: /previous match/iu }));
      expect(onSearchPrevious).toHaveBeenCalledTimes(1);
    });

    it("navigation buttons are disabled when there are no matches", () => {
      render(<LogSearchInput {...defaultProps} searchQuery="notfound" totalMatches={0} />, {
        wrapper: Wrapper,
      });

      expect(screen.getByRole("button", { name: /next match/iu })).toBeDisabled();
      expect(screen.getByRole("button", { name: /previous match/iu })).toBeDisabled();
    });
  });

  describe("clear button", () => {
    it("calls onSearchChange with empty string when the clear button is clicked", () => {
      const onSearchChange = vi.fn();

      render(
        <LogSearchInput
          {...defaultProps}
          onSearchChange={onSearchChange}
          searchQuery="error"
          totalMatches={1}
        />,
        {
          wrapper: Wrapper,
        },
      );

      // The CloseButton has no label text; find it by its role inside the search widget
      const closeButton = screen.getByRole("button", { name: /close/iu });

      fireEvent.click(closeButton);
      expect(onSearchChange).toHaveBeenCalledWith("");
    });
  });
});
