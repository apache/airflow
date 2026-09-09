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
import { Button } from "@chakra-ui/react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { Modal } from "./Modal";

describe("Modal", () => {
  it("renders the heading and body content when open", () => {
    render(
      <Modal open title="Modal Heading">
        Lorem Ipsum Content
      </Modal>,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByText("Modal Heading")).toBeInTheDocument();
    expect(screen.getByText("Lorem Ipsum Content")).toBeInTheDocument();
  });

  it("renders nothing while closed", () => {
    render(
      <Modal open={false} title="Modal Heading">
        Lorem Ipsum Content
      </Modal>,
      { wrapper: BaseWrapper },
    );

    expect(screen.queryByText("Modal Heading")).not.toBeInTheDocument();
    expect(screen.queryByText("Lorem Ipsum Content")).not.toBeInTheDocument();
  });

  it("renders a close button by default", () => {
    render(<Modal open title="Modal Heading" />, { wrapper: BaseWrapper });

    expect(screen.getByRole("button", { name: "Close" })).toBeInTheDocument();
  });

  it("hides the close button when hideCloseButton is set", () => {
    render(<Modal hideCloseButton open title="Modal Heading" />, { wrapper: BaseWrapper });

    expect(screen.queryByRole("button", { name: "Close" })).not.toBeInTheDocument();
  });

  it("calls onOpenChange when the close button is clicked", async () => {
    const onOpenChange = vi.fn();

    render(<Modal onOpenChange={onOpenChange} open title="Modal Heading" />, {
      wrapper: BaseWrapper,
    });

    fireEvent.click(screen.getByRole("button", { name: "Close" }));

    await waitFor(() => expect(onOpenChange).toHaveBeenCalledWith({ open: false }));
  });

  it("renders no footer when no footer content is given", () => {
    const { container } = render(
      <Modal open title="Modal Heading">
        Lorem Ipsum Content
      </Modal>,
      { wrapper: BaseWrapper },
    );

    expect(container.ownerDocument.querySelector(".chakra-dialog__footer")).toBeNull();
  });

  it("renders footerActions with the primary action first in DOM order", () => {
    render(
      <Modal footerActions={<Button>Save</Button>} open title="Modal Heading">
        Lorem Ipsum Content
      </Modal>,
      { wrapper: BaseWrapper },
    );

    const footer = screen.getByRole("button", { name: "Save" }).closest(".chakra-dialog__footer");

    expect(footer).not.toBeNull();
    // The primary action is first in the document, so it is reached first when
    // tabbing, while `row-reverse` still renders it rightmost. The trailing entry is
    // the built-in cancel action; i18n resources are not loaded here, so it renders
    // its translation key.
    expect([...(footer?.querySelectorAll("button") ?? [])].map((button) => button.textContent)).toStrictEqual(
      ["Save", "modal.cancel"],
    );
    expect(globalThis.getComputedStyle(footer?.firstElementChild as HTMLElement).flexDirection).toBe(
      "row-reverse",
    );
  });

  it("lets headerProps and bodyProps children override the defaults", () => {
    render(
      <Modal
        bodyProps={{ children: <span>Body Override</span> }}
        headerProps={{ children: <span>Header Override</span> }}
        open
        title="Modal Heading"
      >
        Ignored Content
      </Modal>,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByText("Header Override")).toBeInTheDocument();
    expect(screen.getByText("Body Override")).toBeInTheDocument();
    expect(screen.queryByText("Modal Heading")).not.toBeInTheDocument();
    expect(screen.queryByText("Ignored Content")).not.toBeInTheDocument();
  });

  it("hides the built-in cancel action when hideCancelAction is set", () => {
    render(
      <Modal footerActions={<Button>Save</Button>} hideCancelAction open title="Modal Heading">
        Lorem Ipsum Content
      </Modal>,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByRole("button", { name: "Save" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "modal.cancel" })).not.toBeInTheDocument();
  });

  it("adds no cancel action when there are no footer actions", () => {
    const { container } = render(
      <Modal open title="Modal Heading">
        Lorem Ipsum Content
      </Modal>,
      { wrapper: BaseWrapper },
    );

    expect(container.ownerDocument.querySelector(".chakra-dialog__footer")).toBeNull();
    expect(screen.queryByRole("button", { name: "modal.cancel" })).not.toBeInTheDocument();
  });

  it("closes the dialog when the built-in cancel action is clicked", async () => {
    const onOpenChange = vi.fn();

    render(
      <Modal footerActions={<Button>Save</Button>} onOpenChange={onOpenChange} open title="Modal Heading">
        Lorem Ipsum Content
      </Modal>,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByRole("button", { name: "modal.cancel" }));

    await waitFor(() => expect(onOpenChange).toHaveBeenCalledWith({ open: false }));
  });

  it("lets cancelActionProps override the built-in cancel action", () => {
    render(
      <Modal
        cancelActionProps={{ children: "Never mind", "data-testid": "custom-cancel" }}
        footerActions={<Button>Save</Button>}
        open
        title="Modal Heading"
      >
        Lorem Ipsum Content
      </Modal>,
      { wrapper: BaseWrapper },
    );

    expect(screen.getByTestId("custom-cancel")).toHaveTextContent("Never mind");
  });
});
