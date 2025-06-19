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
import { Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronRight } from "react-icons/fi";
import { LuPlug } from "react-icons/lu";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { ExternalViewResponse } from "openapi/requests/types.gen";
import { Menu } from "src/components/ui";

import { NavButton } from "./NavButton";
import { PluginMenuItem } from "./PluginMenuItem";

export const PluginMenus = () => {
  const { t: translate } = useTranslation("common");
  const { data } = usePluginServiceGetPlugins();

  const menuPlugins =
    data?.plugins.flatMap((plugin) => plugin.external_views).filter((view) => view.destination === "nav") ??
    [];

  // Only show external plugins in menu if there are more than 2
  const menuExternalViews = menuPlugins.length > 2 ? menuPlugins : [];
  const directExternalViews = menuPlugins.length <= 2 ? menuPlugins : [];

  if (data === undefined || menuPlugins.length === 0) {
    return undefined;
  }

  const categories: Record<string, Array<ExternalViewResponse>> = {};
  const buttons: Array<ExternalViewResponse> = [];

  menuPlugins.forEach((externalView) => {
    if (externalView.category !== null && externalView.category !== undefined) {
      categories[externalView.category] = [...(categories[externalView.category] ?? []), externalView];
    } else {
      buttons.push(externalView);
    }
  });

  if (!buttons.length && !Object.keys(categories).length && menuPlugins.length === 0) {
    return undefined;
  }

  return (
    <>
      {directExternalViews.map((externalView) => (
        <PluginMenuItem {...externalView} key={externalView.name} topLevel={true} />
      ))}
      {menuExternalViews.length > 0 && (
        <Menu.Root positioning={{ placement: "right" }}>
          <Menu.Trigger>
            <NavButton as={Box} icon={<LuPlug />} title={translate("nav.plugins")} />
          </Menu.Trigger>
          <Menu.Content>
            {buttons.map((externalView) => (
              <Menu.Item asChild key={externalView.name} value={externalView.name}>
                <PluginMenuItem {...externalView} />
              </Menu.Item>
            ))}
            {Object.entries(categories).map(([key, menuButtons]) => (
              <Menu.Root key={key} positioning={{ placement: "right" }}>
                <Menu.TriggerItem display="flex" justifyContent="space-between">
                  {key}
                  <FiChevronRight />
                </Menu.TriggerItem>
                <Menu.Content>
                  {menuButtons.map((externalView) => (
                    <Menu.Item asChild key={externalView.name} value={externalView.name}>
                      <PluginMenuItem {...externalView} />
                    </Menu.Item>
                  ))}
                </Menu.Content>
              </Menu.Root>
            ))}
          </Menu.Content>
        </Menu.Root>
      )}
    </>
  );
};
