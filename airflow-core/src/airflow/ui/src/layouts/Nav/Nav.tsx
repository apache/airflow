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
import { Box, Flex, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiDatabase, FiHome } from "react-icons/fi";
import { Link } from "react-router-dom";

import {
  useAuthLinksServiceGetAuthMenus,
  useVersionServiceGetVersion,
  usePluginServiceGetPlugins,
} from "openapi/queries";
import type { ExternalViewResponse } from "openapi/requests/types.gen";
import { AirflowPin } from "src/assets/AirflowPin";
import { DagIcon } from "src/assets/DagIcon";
import type { NavItemResponse } from "src/utils/types";

import { AdminButton } from "./AdminButton";
import { BrowseButton } from "./BrowseButton";
import { DocsButton } from "./DocsButton";
import { NavButton } from "./NavButton";
import { PluginMenus } from "./PluginMenus";
import { SecurityButton } from "./SecurityButton";
import { UserSettingsButton } from "./UserSettingsButton";

// Define existing button categories to filter out
const existingCategories = ["user", "docs", "admin", "browse"];

// Function to categorize navigation items in a single pass
const categorizeNavItems = (
  items: Array<NavItemResponse>,
): {
  adminItems: Array<NavItemResponse>;
  browseItems: Array<NavItemResponse>;
  docsItems: Array<NavItemResponse>;
  topNavItems: Array<NavItemResponse>;
  userItems: Array<NavItemResponse>;
} => {
  const adminItems: Array<NavItemResponse> = [];
  const browseItems: Array<NavItemResponse> = [];
  const docsItems: Array<NavItemResponse> = [];
  const topNavItems: Array<NavItemResponse> = [];
  const userItems: Array<NavItemResponse> = [];

  items.forEach((item) => {
    const category = item.category?.toLowerCase();

    // Categorize items for specific buttons
    if (category === "browse") {
      browseItems.push(item);
    } else if (category === "admin") {
      adminItems.push(item);
    } else if (category === "docs") {
      docsItems.push(item);
    } else if (category === "user") {
      userItems.push(item);
    }

    // Add to top nav items if not in existing categories
    if (category === undefined || !existingCategories.includes(category)) {
      topNavItems.push(item);
    }
  });

  return {
    adminItems,
    browseItems,
    docsItems,
    topNavItems,
    userItems,
  };
};

export const Nav = () => {
  const { data } = useVersionServiceGetVersion();
  const { data: authLinks } = useAuthLinksServiceGetAuthMenus();
  const { data: pluginData } = usePluginServiceGetPlugins();
  const { t: translate } = useTranslation("common");

  // Get both external views and react apps with nav destination
  const navItems: Array<NavItemResponse> =
    pluginData?.plugins
      .flatMap((plugin) => [...plugin.external_views, ...plugin.react_apps])
      .filter((item) => item.destination === "nav") ?? [];

  // Categorize all navigation items in a single pass
  const { adminItems, browseItems, docsItems, topNavItems, userItems } = categorizeNavItems(navItems);

  // Check for legacy views
  const hasLegacyViews =
    (
      pluginData?.plugins
        .flatMap((plugin) => plugin.appbuilder_views)
        // Only include legacy views that have a visible link in the menu. No menu items views
        // are accessible via direct URLs.
        .filter((view) => typeof view.name === "string" && view.name.length > 0) ?? []
    ).length >= 1;

  // Add legacy views if they exist
  const navItemsWithLegacy = hasLegacyViews
    ? [
        ...topNavItems,
        {
          destination: "nav",
          href: "/pluginsv2",
          name: translate("nav.legacyFabViews"),
          url_route: "legacy-fab-views",
        } as ExternalViewResponse,
      ]
    : topNavItems;

  return (
    <VStack
      _ltr={{
        left: 0,
        right: "auto",
      }}
      _rtl={{
        left: "auto",
        right: 0,
      }}
      alignItems="center"
      bg="brand.muted"
      height="100%"
      justifyContent="space-between"
      position="fixed"
      py={1}
      top={0}
      width={16}
      zIndex="docked"
    >
      <Flex alignItems="center" flexDir="column" gap={1} width="100%">
        <Box alignItems="center" asChild boxSize={14} display="flex" justifyContent="center">
          <Link title={translate("nav.home")} to="/">
            <AirflowPin
              _motionSafe={{
                _hover: {
                  transform: "rotate(360deg)",
                  transition: "transform 0.8s ease-in-out",
                },
              }}
              boxSize={8}
            />
          </Link>
        </Box>
        <NavButton icon={FiHome} title={translate("nav.home")} to="/" />
        <NavButton
          disabled={!authLinks?.authorized_menu_items.includes("Dags")}
          icon={DagIcon}
          title={translate("nav.dags")}
          to="dags"
        />
        <NavButton
          disabled={!authLinks?.authorized_menu_items.includes("Assets")}
          icon={FiDatabase}
          title={translate("nav.assets")}
          to="assets"
        />
        <BrowseButton
          authorizedMenuItems={authLinks?.authorized_menu_items ?? []}
          externalViews={browseItems}
        />
        <AdminButton
          authorizedMenuItems={authLinks?.authorized_menu_items ?? []}
          externalViews={adminItems}
        />
        <SecurityButton />
        <PluginMenus navItems={navItemsWithLegacy} />
      </Flex>
      <Flex flexDir="column" gap={1}>
        <DocsButton
          externalViews={docsItems}
          showAPI={authLinks?.authorized_menu_items.includes("Docs")}
          version={data?.version}
        />
        <UserSettingsButton externalViews={userItems} />
      </Flex>
    </VStack>
  );
};
