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
import { HeaderCardSkeleton } from "./HeaderCardSkeleton";

const stats = [
  { labelWidth: 64, valueWidth: 96 },
  { labelWidth: 80, valueWidth: 72 },
  { labelWidth: 48, valueWidth: 130 },
];

describe("HeaderCardSkeleton", () => {
  it("holds a column for every stat the loaded card will show", () => {
    render(
      <Wrapper>
        <HeaderCardSkeleton stats={stats} type="dagRun" />
      </Wrapper>,
    );

    expect(screen.getAllByTestId("stat")).toHaveLength(stats.length);
  });

  // A skeleton answering to "header-card" would let a test waiting on the real card match the
  // placeholder instead, and read its empty state as loaded.
  it("is not mistaken for a loaded card", () => {
    render(
      <Wrapper>
        <HeaderCardSkeleton stats={stats} type="dagRun" />
      </Wrapper>,
    );

    expect(screen.getByTestId("header-card-skeleton")).toBeVisible();
    expect(screen.queryByTestId("header-card")).not.toBeInTheDocument();
  });

  it("keeps the structure of the card it stands in for", () => {
    const { unmount } = render(
      <Wrapper>
        <HeaderCardSkeleton stats={stats} type="dagRun" />
      </Wrapper>,
    );

    const placeholderStats = screen.getAllByTestId("stat").length;

    unmount();

    render(
      <Wrapper>
        <HeaderCard
          icon={undefined}
          stats={stats.map((_unused, index) => ({ label: `label ${index}`, value: `value ${index}` }))}
          title="a run"
          type="dagRun"
        />
      </Wrapper>,
    );

    expect(screen.getAllByTestId("stat")).toHaveLength(placeholderStats);
  });
});
