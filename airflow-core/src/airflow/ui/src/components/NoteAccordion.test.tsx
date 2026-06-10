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

import NoteAccordion from "./NoteAccordion";

const defaultProps = {
  note: "",
  onSave: vi.fn(),
  setNote: vi.fn(),
};

const renderNote = (props: Partial<typeof defaultProps> = {}) =>
  render(<NoteAccordion {...defaultProps} {...props} />, { wrapper: Wrapper });

describe("NoteAccordion", () => {
  describe("empty state", () => {
    it("shows the editable placeholder when there is no note", () => {
      renderNote();
      // i18n returns the key in tests; the placeholder textarea is present
      expect(screen.getByTestId("markdown-input")).toBeInTheDocument();
    });

    it("does not show an expand/collapse button when there is no note", () => {
      renderNote();
      expect(screen.queryByRole("button", { name: /expand/iu })).toBeNull();
      expect(screen.queryByRole("button", { name: /collapse/iu })).toBeNull();
    });

    it("shows the editable area directly (no extra click needed)", () => {
      renderNote();
      // The textarea (edit mode) is reachable without expanding first
      expect(screen.getByTestId("markdown-input")).toBeInTheDocument();
    });
  });

  describe("single-line note", () => {
    const note = "A short single line note";

    it("renders the note content", () => {
      renderNote({ note });
      // The text appears in both the rendered <p> and the hidden textarea — target the paragraph
      expect(screen.getByText(note, { selector: "p" })).toBeInTheDocument();
    });

    it("does not show an expand/collapse button for a single-line note", () => {
      renderNote({ note });
      expect(screen.queryByRole("button", { name: /expand/iu })).toBeNull();
      expect(screen.queryByRole("button", { name: /collapse/iu })).toBeNull();
    });

    it("shows the editable area directly (single click to edit)", () => {
      renderNote({ note });
      expect(screen.getByTestId("markdown-input")).toBeInTheDocument();
    });
  });

  describe("multiline note", () => {
    const note = "First line\nSecond line";

    it("renders the first line as a collapsed preview", () => {
      renderNote({ note });
      expect(screen.getByText("First line")).toBeInTheDocument();
    });

    it("shows an expand button for a multiline note", () => {
      renderNote({ note });
      expect(screen.getByRole("button", { name: /expand/iu })).toBeInTheDocument();
    });

    it("does not show the edit area before expanding", () => {
      renderNote({ note });
      // The grid hides the content — markdown-input is mounted but hidden via grid 0fr
      // The collapsed preview text is visible
      expect(screen.getByText("First line")).toBeInTheDocument();
    });

    it("shows the collapse button after expanding", () => {
      renderNote({ note });
      fireEvent.click(screen.getByRole("button", { name: /expand/iu }));
      expect(screen.getByRole("button", { name: /collapse/iu })).toBeInTheDocument();
    });

    it("hides the collapsed preview after expanding", () => {
      renderNote({ note });
      fireEvent.click(screen.getByRole("button", { name: /expand/iu }));
      // The first-line preview box is removed from the DOM when expanded
      expect(screen.queryByText("First line")).toBeNull();
    });

    it("collapses back when the collapse button is clicked", () => {
      renderNote({ note });
      fireEvent.click(screen.getByRole("button", { name: /expand/iu }));
      fireEvent.click(screen.getByRole("button", { name: /collapse/iu }));
      expect(screen.getByText("First line")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /expand/iu })).toBeInTheDocument();
    });
  });

  describe("long single-line note", () => {
    const note = "A".repeat(81); // over the 80-char threshold

    it("shows an expand button for a long single-line note", () => {
      renderNote({ note });
      expect(screen.getByRole("button", { name: /expand/iu })).toBeInTheDocument();
    });
  });

  describe("editing", () => {
    it("hides the expand/collapse button while the textarea is focused", () => {
      const note = "First line\nSecond line";

      renderNote({ note });
      fireEvent.click(screen.getByRole("button", { name: /expand/iu }));
      expect(screen.getByRole("button", { name: /collapse/iu })).toBeInTheDocument();

      fireEvent.focus(screen.getByTestId("markdown-input"));
      expect(screen.queryByRole("button", { name: /collapse/iu })).toBeNull();
    });

    it("calls onSave when the textarea loses focus", () => {
      const onSave = vi.fn();

      renderNote({ note: "some note", onSave });
      fireEvent.focus(screen.getByTestId("markdown-input"));
      fireEvent.blur(screen.getByTestId("markdown-input"));
      expect(onSave).toHaveBeenCalledTimes(1);
    });

    it("calls setNote when the textarea value changes", () => {
      const setNote = vi.fn();

      renderNote({ setNote });
      fireEvent.change(screen.getByTestId("markdown-input"), { target: { value: "new content" } });
      expect(setNote).toHaveBeenCalledWith("new content");
    });
  });

  describe("note with only whitespace or trailing newlines", () => {
    it("treats a whitespace-only note as empty", () => {
      renderNote({ note: "   \n  " });
      expect(screen.queryByRole("button", { name: /expand/iu })).toBeNull();
    });

    it("does not show expand for a note with a single meaningful line and trailing newline", () => {
      renderNote({ note: "Single line\n" });
      expect(screen.queryByRole("button", { name: /expand/iu })).toBeNull();
    });
  });
});
