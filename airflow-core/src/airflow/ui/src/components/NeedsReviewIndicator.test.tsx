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
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { NeedsReviewIndicator } from "./NeedsReviewIndicator";

const label = (count: number) => i18n.t("requiredActionCount", { count, ns: "hitl" });

describe("NeedsReviewIndicator", () => {
  it("renders nothing when there is nothing to review", () => {
    render(<NeedsReviewIndicator count={0} onClick={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.queryByTestId("needs-review-badge")).not.toBeInTheDocument();
  });

  it("shows the count and names itself for assistive tech", () => {
    render(<NeedsReviewIndicator count={3} onClick={vi.fn()} />, { wrapper: Wrapper });

    expect(screen.getByTestId("needs-review-badge")).toHaveTextContent("3");
    expect(screen.getByRole("button", { name: label(3) })).toBeInTheDocument();
  });

  it("opens the review modal when given a handler", () => {
    const onClick = vi.fn();

    render(<NeedsReviewIndicator count={1} onClick={onClick} />, { wrapper: Wrapper });
    fireEvent.click(screen.getByTestId("needs-review-badge"));

    expect(onClick).toHaveBeenCalledTimes(1);
  });

  it("navigates instead when given a destination", () => {
    render(<NeedsReviewIndicator count={1} to="/required_actions" />, { wrapper: Wrapper });

    expect(screen.getByRole("link", { name: label(1) })).toHaveAttribute("href", "/required_actions");
  });
});
