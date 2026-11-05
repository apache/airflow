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
import { describe, expect, it } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { ErrorAlert } from "./ErrorAlert";

describe("ErrorAlert", () => {
  it("renders nothing without an error", () => {
    render(
      <Wrapper>
        <ErrorAlert />
      </Wrapper>,
    );

    expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument();
  });

  it("offers no way to dismiss unless asked, so existing callers are unaffected", () => {
    render(
      <Wrapper>
        <ErrorAlert error={new Error("request failed")} />
      </Wrapper>,
    );

    expect(screen.getByTestId("error-alert")).toBeVisible();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("closes when dismissed", () => {
    render(
      <Wrapper>
        <ErrorAlert closable error={new Error("request failed")} />
      </Wrapper>,
    );

    fireEvent.click(screen.getByRole("button"));

    expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument();
  });

  it("returns for the next failure, which a dismissal must not suppress", () => {
    const { rerender } = render(
      <Wrapper>
        <ErrorAlert closable error={new Error("first failure")} />
      </Wrapper>,
    );

    fireEvent.click(screen.getByRole("button"));
    expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument();

    rerender(
      <Wrapper>
        <ErrorAlert closable error={new Error("second failure")} />
      </Wrapper>,
    );

    expect(screen.getByTestId("error-alert")).toBeVisible();
  });
});
