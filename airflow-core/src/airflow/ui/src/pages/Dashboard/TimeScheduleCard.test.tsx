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
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { TimeScheduleCard } from "./TimeScheduleCard";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { dir: () => "ltr" },
    // eslint-disable-next-line id-length
    t: (key: string) =>
      key === "timeSchedule.title" ? "Time Schedule" : "Explore Dag run timing and overlaps.",
  }),
}));

describe("TimeScheduleCard", () => {
  it.each(["", "/airflow"])("preserves the router base path %s", (basename) => {
    render(
      <BaseWrapper>
        <MemoryRouter basename={basename} initialEntries={[`${basename}/home`]}>
          <TimeScheduleCard />
        </MemoryRouter>
      </BaseWrapper>,
    );

    expect(screen.getByRole("link", { name: "Time Schedule" })).toHaveAttribute(
      "href",
      `${basename}/time-schedule`,
    );
    expect(screen.getByRole("link", { name: "Time Schedule" })).toContainElement(
      screen.getByText("Explore Dag run timing and overlaps."),
    );
  });
});
