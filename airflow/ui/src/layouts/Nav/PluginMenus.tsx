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
import { type ButtonProps, Link } from "@chakra-ui/react";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { AppBuilderMenuItemResponse } from "openapi/requests/types.gen";
import { Button, Menu } from "src/components/ui";

const styles = {
  alignItems: "center",
  borderRadius: "none",
  colorPalette: "blue",
  flexDir: "column",
  height: 10,
  variant: "ghost",
  whiteSpace: "wrap",
  width: 20,
} satisfies ButtonProps;

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

  return (
    <>
      {buttons.map((button) =>
        button.href !== null && button.href !== undefined ? (
          <Button asChild {...styles} key={button.name}>
            <Link href={button.href} rel="noopener noreferrer" target="_blank">
              {button.name}
            </Link>
          </Button>
        ) : undefined,
      )}
      {Object.entries(categories).map(([key, menuButtons]) => (
        <Menu.Root key={key} positioning={{ placement: "right" }}>
          <Menu.Trigger asChild>
            <Button {...styles}>{key}</Button>
          </Menu.Trigger>
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
    </>
  );
};
