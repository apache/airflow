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
import { Box, Icon } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronRight } from "react-icons/fi";
import { LuPlug } from "react-icons/lu";

import { Menu } from "src/components/ui";
import type { NavItemResponse } from "src/utils/types";

import { NavButton } from "./NavButton";
import { PluginMenuItem } from "./PluginMenuItem";

export const PluginMenus = ({ navItems }: { readonly navItems: Array<NavItemResponse> }) => {
  const { t: translate } = useTranslation("common");

  if (navItems.length === 0) {
    return undefined;
  }

  const promotedItems = navItems.filter((item) => item.nav_top_level === true);
  const remainingItems = navItems.filter((item) => item.nav_top_level !== true);

  // Build category structure for remaining items that go into the submenu
  const remainingCategories: Record<string, Array<NavItemResponse>> = {};
  const remainingButtons: Array<NavItemResponse> = [];

  remainingItems.forEach((navItem) => {
    if (navItem.category !== null && navItem.category !== undefined) {
      remainingCategories[navItem.category] = [...(remainingCategories[navItem.category] ?? []), navItem];
    } else {
      remainingButtons.push(navItem);
    }
  });

  // Remaining items go into a submenu only when there are 2 or more of them.
  // A single remaining item is promoted to the toolbar to avoid a one-item submenu.
  const showRemainingInMenu = remainingItems.length >= 2;

  return (
    <>
      {promotedItems.map((navItem) => (
        <PluginMenuItem key={navItem.name} {...navItem} topLevel={true} />
      ))}
      {showRemainingInMenu ? (
        <Menu.Root positioning={{ placement: "right" }}>
          <Menu.Trigger>
            <NavButton as={Box} icon={LuPlug} title={translate("nav.plugins")} />
          </Menu.Trigger>
          <Menu.Content>
            {remainingButtons.map((navItem) => (
              <PluginMenuItem key={navItem.name} {...navItem} />
            ))}
            {Object.entries(remainingCategories).map(([key, menuButtons]) => (
              <Menu.Root key={key} positioning={{ placement: "right" }}>
                <Menu.TriggerItem display="flex" justifyContent="space-between">
                  {key}
                  <Icon as={FiChevronRight} boxSize={4} color="fg.muted" />
                </Menu.TriggerItem>
                <Menu.Content>
                  {menuButtons.map((navItem) => (
                    <PluginMenuItem {...navItem} key={navItem.name} />
                  ))}
                </Menu.Content>
              </Menu.Root>
            ))}
          </Menu.Content>
        </Menu.Root>
      ) : (
        remainingItems.map((navItem) => <PluginMenuItem key={navItem.name} {...navItem} topLevel={true} />)
      )}
    </>
  );
};
