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
import { Box, Link, Image } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronRight } from "react-icons/fi";
import { LuPlug } from "react-icons/lu";
import { Link as RouterLink } from "react-router-dom";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { AppBuilderMenuItemResponse } from "openapi/requests/types.gen";
import { Menu } from "src/components/ui";

import { NavButton } from "./NavButton";

export const PluginMenus = () => {
  const { t: translate } = useTranslation("common");
  const { data } = usePluginServiceGetPlugins();

  const menuPlugins = data?.plugins.filter((plugin) => plugin.appbuilder_menu_items.length > 0);
  const iframePlugins =
    data?.plugins.flatMap((plugin) => plugin.iframe_views).filter((view) => view.destination === "nav") ?? [];

  // Only show iframe plugins in menu if there are more than 2
  const menuIframePlugins = iframePlugins.length > 2 ? iframePlugins : [];
  const directIframePlugins = iframePlugins.length <= 2 ? iframePlugins : [];

  if (data === undefined || (menuPlugins === undefined && iframePlugins.length === 0)) {
    return undefined;
  }

  const categories: Record<string, Array<AppBuilderMenuItemResponse>> = {};
  const buttons: Array<AppBuilderMenuItemResponse> = [];

  menuPlugins?.forEach((plugin) => {
    plugin.appbuilder_menu_items.forEach((mi) => {
      if (mi.category !== null && mi.category !== undefined) {
        categories[mi.category] = [...(categories[mi.category] ?? []), mi];
      } else {
        buttons.push(mi);
      }
    });
  });

  if (!buttons.length && !Object.keys(categories).length && iframePlugins.length === 0) {
    return undefined;
  }

  return (
    <>
      {directIframePlugins.map((plugin) => (
        <NavButton
          icon={
            typeof plugin.icon === "string" ? (
              <Image height="1.75rem" src={plugin.icon} width="1.75rem" />
            ) : (
              <LuPlug size="1.75rem" />
            )
          }
          key={plugin.name}
          title={plugin.name}
          to={`plugin/${plugin.url_route ?? plugin.name.toLowerCase().replace(" ", "-")}`}
        />
      ))}
      {(menuIframePlugins.length > 0 || buttons.length > 0 || Object.keys(categories).length > 0) && (
        <Menu.Root positioning={{ placement: "right" }}>
          <Menu.Trigger>
            <NavButton as={Box} icon={<LuPlug />} title={translate("nav.plugins")} />
          </Menu.Trigger>
          <Menu.Content>
            {menuIframePlugins.map((plugin) => (
              <Menu.Item key={plugin.name} value={plugin.name}>
                <Box alignItems="center" display="flex" gap={2}>
                  {typeof plugin.icon === "string" ? (
                    <Image height="1.25rem" src={plugin.icon} width="1.25rem" />
                  ) : (
                    <LuPlug size="1.25rem" />
                  )}
                  <RouterLink
                    to={`plugin/${plugin.url_route ?? plugin.name.toLowerCase().replace(" ", "-")}`}
                  >
                    {plugin.name}
                  </RouterLink>
                </Box>
              </Menu.Item>
            ))}
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
      )}
    </>
  );
};
