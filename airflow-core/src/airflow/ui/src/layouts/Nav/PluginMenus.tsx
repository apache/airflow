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

  const categories: Record<string, Array<NavItemResponse>> = {};
  const buttons: Array<NavItemResponse> = [];

  navItems.forEach((navItem) => {
    if (navItem.category !== null && navItem.category !== undefined) {
      categories[navItem.category] = [...(categories[navItem.category] ?? []), navItem];
    } else {
      buttons.push(navItem);
    }
  });

  if (!buttons.length && !Object.keys(categories).length && navItems.length === 0) {
    return undefined;
  }

  // Show plugins in menu if there are more than or equal to 2
  return navItems.length >= 2 ? (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger>
        <NavButton as={Box} icon={LuPlug} title={translate("nav.plugins")} />
      </Menu.Trigger>
      <Menu.Content>
        {buttons.map((navItem) => (
          <PluginMenuItem key={navItem.name} {...navItem} />
        ))}
        {Object.entries(categories).map(([key, menuButtons]) => (
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
    navItems.map((navItem) => <PluginMenuItem {...navItem} key={navItem.name} topLevel={true} />)
  );
};
