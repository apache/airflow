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
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { ErrorModal } from "./ErrorModal";

describe("ErrorModal", () => {
  it("names the dialog after its title and renders the error details", () => {
    render(
      <ErrorModal onClose={vi.fn()} open title="Something failed">
        Full error details
      </ErrorModal>,
      { wrapper: BaseWrapper },
    );

    const dialog = screen.getByRole("dialog", { name: "Something failed" });

    expect(within(dialog).getByText("Full error details")).toBeInTheDocument();
  });

  it("calls onClose when the close button is clicked", async () => {
    const onClose = vi.fn();

    render(
      <ErrorModal onClose={onClose} open title="Something failed">
        Full error details
      </ErrorModal>,
      { wrapper: BaseWrapper },
    );

    fireEvent.click(screen.getByRole("button", { name: "Close" }));

    await waitFor(() => expect(onClose).toHaveBeenCalled());
  });
});
