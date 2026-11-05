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
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { HeaderCard } from "./HeaderCard";

const stats = [{ label: "Run Type", value: "manual" }];

describe("HeaderCard", () => {
  it("shows why the object could not be loaded", () => {
    render(
      <Wrapper>
        <HeaderCard
          error={new Error("request failed")}
          icon={undefined}
          stats={stats}
          title="a run"
          type="dagRun"
        />
      </Wrapper>,
    );

    expect(screen.getByTestId("error-alert")).toHaveTextContent("request failed");
  });

  // A card that could not load has nothing but the error to say, so it must not be closable there.
  it("offers to dismiss the error only when asked", () => {
    const { rerender } = render(
      <Wrapper>
        <HeaderCard
          error={new Error("request failed")}
          icon={undefined}
          stats={[]}
          title="a run"
          type="dagRun"
        />
      </Wrapper>,
    );

    expect(screen.queryByRole("button")).not.toBeInTheDocument();

    rerender(
      <Wrapper>
        <HeaderCard
          error={new Error("request failed")}
          errorClosable
          icon={undefined}
          stats={stats}
          title="a run"
          type="dagRun"
        />
      </Wrapper>,
    );

    expect(screen.getByRole("button")).toBeVisible();
  });

  it("leaves no room for an error when there is none", () => {
    render(
      <Wrapper>
        <HeaderCard icon={undefined} stats={stats} title="a run" type="dagRun" />
      </Wrapper>,
    );

    expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument();
  });

  // The loading variant passes a skeleton as a label, which cannot serve as a React key.
  it("keys its stats when a label is not a string", () => {
    render(
      <Wrapper>
        <HeaderCard
          icon={undefined}
          stats={[
            { label: <span>first</span>, value: "one" },
            { label: <span>second</span>, value: "two" },
          ]}
          title="a run"
          type="dagRun"
        />
      </Wrapper>,
    );

    expect(screen.getAllByTestId("stat")).toHaveLength(2);
  });
});
