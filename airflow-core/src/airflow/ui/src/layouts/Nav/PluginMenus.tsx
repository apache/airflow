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
import { Box, Link } from "@chakra-ui/react";
import { FiChevronRight } from "react-icons/fi";
import { LuPlug } from "react-icons/lu";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { AppBuilderMenuItemResponse } from "openapi/requests/types.gen";
import { Menu } from "src/components/ui";

import { NavButton } from "./NavButton";

export const PluginMenus = () => {
  const { data } = usePluginServiceGetPlugins();

  const menuPlugins = data?.plugins.filter((plugin) => plugin.appbuilder_menu_items.length > 0);

  if (data === undefined || menuPlugins === undefined) {
    return undefined;
  }

  const categories: Record<string, Array<AppBuilderMenuItemResponse>> = {};
  const buttons: Array<AppBuilderMenuItemResponse> = [];

  menuPlugins.forEach((plugin) => {
    plugin.appbuilder_menu_items.forEach((mi) => {
      if (mi.category !== null && mi.category !== undefined) {
        categories[mi.category] = [...(categories[mi.category] ?? []), mi];
      } else {
        buttons.push(mi);
      }
    });
  });

  if (!buttons.length && !Object.keys(categories).length) {
    return undefined;
  }

  return (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger>
        <NavButton as={Box} icon={<LuPlug />} title="Plugins" />
      </Menu.Trigger>
      <Menu.Content>
        {buttons.map(({ href, name }) =>
          href !== null && href !== undefined ? (
            <Menu.Item asChild key={name} value={name}>
              <Link aria-label={name} href={href} rel="noopener noreferrer" target="_blank">
                {name}
              </Link>
            </Menu.Item>
          ) : undefined,
        )}
        {Object.entries(categories).map(([key, menuButtons]) => (
          <Menu.Root key={key} positioning={{ placement: "right" }}>
            <Menu.TriggerItem display="flex" justifyContent="space-between">
              {key}
              <FiChevronRight />
            </Menu.TriggerItem>
            <Menu.Content>
              {menuButtons.map(({ href, name }) =>
                href !== undefined && href !== null ? (
                  <Menu.Item asChild key={name} value={name}>
                    <Link aria-label={name} href={href} rel="noopener noreferrer" target="_blank">
                      {name}
                    </Link>
                  </Menu.Item>
                ) : undefined,
              )}
            </Menu.Content>
          </Menu.Root>
        ))}
      </Menu.Content>
    </Menu.Root>
  );
};
