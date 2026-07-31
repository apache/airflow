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

import NotePreview from "./NotePreview";

const defaultProps = {
  header: "Note",
  isPending: false,
  note: "",
  onOpen: vi.fn(),
  onSave: vi.fn(),
  setNote: vi.fn(),
};

const renderNote = (props: Partial<typeof defaultProps> = {}) =>
  render(<NotePreview {...defaultProps} {...props} />, { wrapper: Wrapper });

describe("NotePreview", () => {
  describe("preview", () => {
    it("shows the placeholder when there is no note", () => {
      renderNote();
      expect(screen.getByText("note.placeholder")).toBeInTheDocument();
    });

    it("renders the note content as a read-only preview", () => {
      const note = "A short single line note";

      renderNote({ note });
      expect(screen.getByText(note, { selector: "p" })).toBeInTheDocument();
    });

    it("shows only the first non-empty line with an ellipsis for a multiline note", () => {
      renderNote({ note: "First line\nSecond line" });
      expect(screen.getByText(/First line …/u)).toBeInTheDocument();
      expect(screen.queryByText("Second line")).toBeNull();
    });

    it("does not append an ellipsis for a single-line note", () => {
      renderNote({ note: "Only line" });
      expect(screen.getByText("Only line", { selector: "p" })).toBeInTheDocument();
      expect(screen.queryByText(/…/u)).toBeNull();
    });

    it("does not render an editable textarea before opening the modal", () => {
      renderNote({ note: "some note" });
      expect(screen.queryByTestId("markdown-input")).toBeNull();
    });
  });

  describe("opening the modal", () => {
    it("resets state and opens the modal when the edit button is clicked", async () => {
      const onOpen = vi.fn();

      renderNote({ note: "some note", onOpen });
      fireEvent.click(screen.getByRole("button", { name: "note.edit" }));

      expect(onOpen).toHaveBeenCalledTimes(1);
      // Modal header confirms the dialog is open
      expect(await screen.findByRole("heading", { name: "Note" })).toBeInTheDocument();
    });

    it("opens an existing note in read mode (markdown shown, textarea hidden)", async () => {
      renderNote({ note: "some note" });
      fireEvent.click(screen.getByRole("button", { name: "note.edit" }));

      expect(await screen.findByTestId("edit-markdown")).toBeInTheDocument();
      expect(screen.queryByTestId("markdown-input")).toBeNull();
    });

    it("opens an empty note straight into editing", async () => {
      renderNote({ note: "" });
      fireEvent.click(screen.getByRole("button", { name: "note.edit" }));

      expect(await screen.findByTestId("markdown-input")).toBeInTheDocument();
    });
  });
});
