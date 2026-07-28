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
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import MarkdownModal, { MAX_NOTE_LENGTH } from "./MarkdownModal";

const defaultProps = {
  header: "Note",
  isOpen: true,
  isPending: false,
  mdContent: "An existing note",
  onClose: vi.fn(),
  onConfirm: vi.fn(),
  placeholder: "Add a note...",
  setMdContent: vi.fn(),
};

const renderModal = (props: Partial<typeof defaultProps> = {}) =>
  render(<MarkdownModal {...defaultProps} {...props} />, { wrapper: Wrapper });

// The modal is controlled, so drive mdContent from a stateful wrapper.
const ControlledModal = () => {
  const [value, setValue] = useState("");

  return <MarkdownModal {...defaultProps} mdContent={value} setMdContent={setValue} />;
};

describe("MarkdownModal", () => {
  it("shows rendered markdown (read-only) with an edit toggle for an existing note", () => {
    renderModal();
    expect(screen.getByText("An existing note", { selector: "p" })).toBeInTheDocument();
    expect(screen.queryByTestId("markdown-input")).toBeNull();
    expect(screen.getByTestId("edit-markdown")).toBeInTheDocument();
  });

  it("reveals the textarea when the edit toggle is clicked", () => {
    renderModal();
    fireEvent.click(screen.getByTestId("edit-markdown"));
    expect(screen.getByTestId("markdown-input")).toBeInTheDocument();
  });

  it("opens straight into editing when there is no content", () => {
    renderModal({ mdContent: "" });
    expect(screen.getByTestId("markdown-input")).toBeInTheDocument();
  });

  it("calls setMdContent as the textarea value changes", () => {
    const setMdContent = vi.fn();

    renderModal({ mdContent: "", setMdContent });
    fireEvent.change(screen.getByTestId("markdown-input"), { target: { value: "new content" } });
    expect(setMdContent).toHaveBeenCalledWith("new content");
  });

  describe("character limit", () => {
    it("caps the textarea at the maximum length", () => {
      renderModal({ mdContent: "" });
      expect(screen.getByTestId("markdown-input")).toHaveAttribute("maxlength", String(MAX_NOTE_LENGTH));
    });

    it("shows the live character count and keeps saving enabled under the limit", () => {
      renderModal({ mdContent: "hello" });
      fireEvent.click(screen.getByTestId("edit-markdown"));
      expect(screen.getByText(`5/${MAX_NOTE_LENGTH}`)).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /confirm/iu })).toBeEnabled();
    });

    it("keeps saving enabled at exactly the limit", () => {
      renderModal({ mdContent: "x".repeat(MAX_NOTE_LENGTH) });
      fireEvent.click(screen.getByTestId("edit-markdown"));
      expect(screen.getByText(`${MAX_NOTE_LENGTH}/${MAX_NOTE_LENGTH}`)).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /confirm/iu })).toBeEnabled();
    });

    it("shows the count and disables saving when over the limit", () => {
      renderModal({ mdContent: "x".repeat(MAX_NOTE_LENGTH + 1) });
      fireEvent.click(screen.getByTestId("edit-markdown"));
      expect(screen.getByText(`${MAX_NOTE_LENGTH + 1}/${MAX_NOTE_LENGTH}`)).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /confirm/iu })).toBeDisabled();
    });
  });

  it("toggles between the textarea and a rendered preview while editing", () => {
    render(<ControlledModal />, { wrapper: Wrapper });

    // Starts in the editor
    fireEvent.change(screen.getByTestId("markdown-input"), { target: { value: "**bold**" } });
    fireEvent.click(screen.getByTestId("preview-toggle"));

    // Preview hides the textarea and renders the markdown
    expect(screen.queryByTestId("markdown-input")).toBeNull();
    expect(screen.getByText("bold", { selector: "strong" })).toBeInTheDocument();

    // Toggling back returns to the editor
    fireEvent.click(screen.getByTestId("preview-toggle"));
    expect(screen.getByTestId("markdown-input")).toBeInTheDocument();
  });

  it("confirms and closes when save is clicked", () => {
    const onClose = vi.fn();
    const onConfirm = vi.fn();

    renderModal({ mdContent: "valid note", onClose, onConfirm });
    fireEvent.click(screen.getByTestId("edit-markdown"));
    fireEvent.click(screen.getByRole("button", { name: /confirm/iu }));

    expect(onConfirm).toHaveBeenCalledTimes(1);
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
