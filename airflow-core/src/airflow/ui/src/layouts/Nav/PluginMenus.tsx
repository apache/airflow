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

// Define existing button categories to filter out
const existingCategories = ["user", "docs", "admin", "browse"];

export const PluginMenus = () => {
  const { t: translate } = useTranslation("common");
  const { data } = usePluginServiceGetPlugins();

  let menuPlugins =
    data?.plugins.flatMap((plugin) => plugin.external_views).filter((view) => view.destination === "nav") ??
    [];

  // Filter out plugins with categories that match existing buttons
  menuPlugins = menuPlugins.filter((view) => {
    const category = view.category?.toLowerCase();

    return category === undefined || !existingCategories.includes(category);
  });

  const hasLegacyViews =
    (
      data?.plugins
        .flatMap((plugin) => plugin.appbuilder_views)
        // Only include legacy views that have a visible link in the menu. No menu items views
        // are accessible via direct URLs.
        .filter((view) => view.name !== undefined && view.name !== null) ?? []
    ).length >= 1;

  if (hasLegacyViews) {
    menuPlugins = [
      ...menuPlugins,
      {
        destination: "nav",
        href: "/pluginsv2",
        name: translate("nav.legacyFabViews"),
        url_route: "legacy-fab-views",
      },
    ];
  }

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

  // Show plugins in menu if there are more than 2
  return menuPlugins.length > 2 ? (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger>
        <NavButton as={Box} icon={<LuPlug />} title={translate("nav.plugins")} />
      </Menu.Trigger>
      <Menu.Content>
        {buttons.map((externalView) => (
          <PluginMenuItem key={externalView.name} {...externalView} />
        ))}
        {Object.entries(categories).map(([key, menuButtons]) => (
          <Menu.Root key={key} positioning={{ placement: "right" }}>
            <Menu.TriggerItem display="flex" justifyContent="space-between">
              {key}
              <FiChevronRight />
            </Menu.TriggerItem>
            <Menu.Content>
              {menuButtons.map((externalView) => (
                <PluginMenuItem {...externalView} key={externalView.name} />
              ))}
            </Menu.Content>
          </Menu.Root>
        ))}
      </Menu.Content>
    </Menu.Root>
  ) : (
    menuPlugins.map((plugin) => <PluginMenuItem {...plugin} key={plugin.name} topLevel={true} />)
  );
};
