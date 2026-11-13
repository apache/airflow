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
import { FiHome } from "react-icons/fi";
import { describe, expect, it } from "vitest";

import { Menu } from "src/system-components";

import { Wrapper } from "src/utils/Wrapper";

import { NavButton } from "./NavButton";
import { NavMenu } from "./NavMenu";

const renderMenus = () =>
  render(
    <>
      <NavMenu trigger={<NavButton icon={FiHome} title="First" />}>
        <Menu.Item value="first-item">First item</Menu.Item>
      </NavMenu>
      <NavMenu trigger={<NavButton icon={FiHome} title="Second" />}>
        <Menu.Item value="second-item">Second item</Menu.Item>
      </NavMenu>
    </>,
    { wrapper: Wrapper },
  );

const getTrigger = (name: string) => screen.getByRole("button", { name });

const hover = (name: string) => fireEvent.pointerEnter(getTrigger(name), { pointerType: "mouse" });

// Comfortably past the delay on which a menu the pointer has left closes itself.
const waitOutCloseDelay = async () =>
  new Promise((resolve) => {
    setTimeout(resolve, 500);
  });

describe("NavMenu", () => {
  it("opens once the pointer has rested on the trigger", async () => {
    renderMenus();

    hover("First");

    expect(getTrigger("First")).toHaveAttribute("aria-expanded", "false");

    await waitFor(() => {
      expect(getTrigger("First")).toHaveAttribute("aria-expanded", "true");
    });
  });

  it("hands off to the next menu without leaving the previous one open", async () => {
    renderMenus();

    hover("First");
    await waitFor(() => {
      expect(getTrigger("First")).toHaveAttribute("aria-expanded", "true");
    });

    fireEvent.pointerLeave(getTrigger("First"), { pointerType: "mouse" });
    hover("Second");

    await waitFor(() => {
      expect(getTrigger("Second")).toHaveAttribute("aria-expanded", "true");
    });
    expect(getTrigger("First")).toHaveAttribute("aria-expanded", "false");

    // The menu being left has to close as part of the handoff. Closing later on its own
    // delay would pull focus back to its trigger and dismiss the menu just opened.
    await waitOutCloseDelay();

    expect(getTrigger("Second")).toHaveAttribute("aria-expanded", "true");
    expect(getTrigger("First")).toHaveAttribute("aria-expanded", "false");
  });
});
