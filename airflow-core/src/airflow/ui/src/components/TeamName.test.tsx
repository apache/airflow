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
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { TeamName } from "./TeamName";

const mockConfig: Record<string, unknown> = { multi_team: false };

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

describe("TeamName", () => {
  beforeEach(() => {
    mockConfig.multi_team = false;
  });

  it("links to the Dags list filtered on the team when multi-team is enabled", () => {
    mockConfig.multi_team = true;
    render(<TeamName teamName="team a" />, { wrapper: Wrapper });

    expect(screen.getByRole("link", { name: "team a" })).toHaveAttribute("href", "/dags?teams=team%20a");
  });

  it.each([{ teamName: null }, { teamName: undefined }])(
    "renders nothing when the team is $teamName",
    ({ teamName }) => {
      mockConfig.multi_team = true;
      render(<TeamName teamName={teamName} />, { wrapper: Wrapper });

      expect(screen.queryByRole("link")).not.toBeInTheDocument();
    },
  );

  it("renders nothing when multi-team is disabled", () => {
    render(<TeamName teamName="team-a" />, { wrapper: Wrapper });

    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });
});
