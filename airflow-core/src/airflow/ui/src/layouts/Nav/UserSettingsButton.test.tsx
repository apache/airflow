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
import { afterEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";

import { UserSettingsButton } from "./UserSettingsButton";

const mockCurrentUser: { data: { id: string; teams?: Array<string> | null; username: string } } = {
  data: { id: "test", teams: null, username: "test" },
};

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenapiQueries>()),
  useAuthLinksServiceGetCurrentUserInfo: () => mockCurrentUser,
}));

const openUserMenu = () => fireEvent.click(screen.getByRole("button", { name: /user/iu }));

describe("UserSettingsButton", () => {
  afterEach(() => {
    mockCurrentUser.data = { id: "test", teams: null, username: "test" };
  });

  it("links to the settings page from the user menu", async () => {
    render(<UserSettingsButton externalViews={[]} />, { wrapper: Wrapper });

    openUserMenu();

    const settingsLink = await screen.findByRole("menuitem", { name: /settings.title/iu });

    expect(settingsLink).toHaveAttribute("href", "/settings");
  });

  it("lists each team the user belongs to when multi-team is enabled", async () => {
    mockCurrentUser.data = { id: "test", teams: ["team-a", "team-b"], username: "test" };
    render(<UserSettingsButton externalViews={[]} />, { wrapper: Wrapper });

    openUserMenu();

    expect(await screen.findByText("team-a")).toBeInTheDocument();
    expect(screen.getByText("team-b")).toBeInTheDocument();
  });

  it("counts the teams it does not list when the user belongs to many", async () => {
    mockCurrentUser.data = {
      id: "test",
      teams: Array.from({ length: 8 }, (_, index) => `team-${index}`),
      username: "test",
    };
    render(<UserSettingsButton externalViews={[]} />, { wrapper: Wrapper });

    openUserMenu();

    expect(await screen.findByText("team-4")).toBeInTheDocument();
    expect(screen.queryByText("team-5")).not.toBeInTheDocument();
    expect(screen.getByText("teams.more")).toBeInTheDocument();
  });

  it("reveals the teams it does not list when hovering the overflow count", async () => {
    mockCurrentUser.data = {
      id: "test",
      teams: Array.from({ length: 8 }, (_, index) => `team-${index}`),
      username: "test",
    };
    render(<UserSettingsButton externalViews={[]} />, { wrapper: Wrapper });

    openUserMenu();

    fireEvent.pointerMove(await screen.findByText("teams.more"), { pointerType: "mouse" });

    expect(await screen.findByText("team-5")).toBeInTheDocument();
    expect(screen.getByText("team-6")).toBeInTheDocument();
    expect(screen.getByText("team-7")).toBeInTheDocument();
  });

  it("tells the user they belong to no team when multi-team is enabled", async () => {
    mockCurrentUser.data = { id: "test", teams: [], username: "test" };
    render(<UserSettingsButton externalViews={[]} />, { wrapper: Wrapper });

    openUserMenu();

    expect(await screen.findByText("teams.none")).toBeInTheDocument();
  });

  it("hides teams when the deployment does not run in multi-team mode", async () => {
    render(<UserSettingsButton externalViews={[]} />, { wrapper: Wrapper });

    openUserMenu();

    await screen.findByRole("menuitem", { name: /settings.title/iu });

    expect(screen.queryByText("teams.title")).not.toBeInTheDocument();
  });
});
