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
import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { MaxChangesField } from "./MaxChangesField";

const revealTruncationNotice = async () => {
  const warning = screen.getByTestId("max-changes-truncated");

  await act(async () => {
    fireEvent.focus(warning);
    fireEvent.pointerEnter(warning);
    await vi.advanceTimersByTimeAsync(500);
  });
};

// The test harness passes translation keys through untranslated, so assertions on chrome use the
// key and assertions on content use what the caller passed.
describe("MaxChangesField", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("says nothing about a limit while the comparison fits inside it", () => {
    render(<MaxChangesField maxChanges={10} onCommit={vi.fn()} truncated={false} />, { wrapper: Wrapper });

    expect(screen.queryByTestId("max-changes-truncated")).not.toBeInTheDocument();
  });

  it("keeps the truncation notice on the field, behind the warning", async () => {
    // A banner over the table repeated itself on every comparison and sat nowhere near the bound
    // that caused it; the field is the one control that can lift it.
    vi.useFakeTimers();
    render(<MaxChangesField maxChanges={10} onCommit={vi.fn()} truncated />, { wrapper: Wrapper });

    expect(screen.queryByText("versions.limitReached.description")).not.toBeInTheDocument();

    await revealTruncationNotice();

    expect(screen.getByRole("tooltip")).toHaveTextContent("versions.limitReached.description");
  });

  it("names the endpoint's own limit when no bound was set", async () => {
    vi.useFakeTimers();
    render(<MaxChangesField onCommit={vi.fn()} truncated />, { wrapper: Wrapper });

    await revealTruncationNotice();

    expect(screen.getByRole("tooltip")).toHaveTextContent("versions.limitReached.descriptionDefault");
  });
});
