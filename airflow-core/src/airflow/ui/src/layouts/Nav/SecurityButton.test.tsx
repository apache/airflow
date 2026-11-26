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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { SecurityButton } from "./SecurityButton";

const mocks = vi.hoisted(() => ({ useAuthLinksServiceGetAuthMenus: vi.fn() }));

vi.mock("openapi/queries", () => ({
  useAuthLinksServiceGetAuthMenus: mocks.useAuthLinksServiceGetAuthMenus,
}));

const withMenuItems = (items: Array<{ href: string; text: string }>) =>
  mocks.useAuthLinksServiceGetAuthMenus.mockReturnValue({
    data: { authorized_menu_items: [], extra_menu_items: items },
    isLoading: false,
  });

const openMenu = async () => {
  fireEvent.click(screen.getByRole("button", { name: "nav.security" }));
  await waitFor(() => expect(screen.getAllByRole("menuitem").length).toBeGreaterThan(0));
};

beforeAll(() => {
  // AppWrapper fetches the locale files over HTTP and jsdom does not serve them, so without a
  // bundle `translate` hands back the key and there is no translated wording to assert on.
  i18n.addResourceBundle("en", "common", { security: { users: "Benutzer" } }, true, true);
});

beforeEach(() => {
  mocks.useAuthLinksServiceGetAuthMenus.mockReset();
});

describe("SecurityButton", () => {
  it("names each link by its translated label, not the English text the API sent", async () => {
    withMenuItems([{ href: "/auth/users/list/", text: "Users" }]);

    render(<SecurityButton />, { wrapper: Wrapper });
    await openMenu();

    // An aria-label overrides the visible text, so naming the link from the API's own English
    // left a screen reader reading "Users" while the menu showed the translated word.
    expect(screen.getByRole("menuitem", { name: "Benutzer" })).toBeInTheDocument();
    expect(screen.queryByRole("menuitem", { name: "Users" })).not.toBeInTheDocument();
  });

  it("falls back to the item's own text when the auth manager sends one we have no key for", async () => {
    withMenuItems([{ href: "/auth/audit-logs/list/", text: "Audit Logs" }]);

    render(<SecurityButton />, { wrapper: Wrapper });
    await openMenu();

    expect(screen.getByRole("menuitem", { name: "Audit Logs" })).toBeInTheDocument();
    expect(screen.queryByText("security.audit-logs")).not.toBeInTheDocument();
  });
});
