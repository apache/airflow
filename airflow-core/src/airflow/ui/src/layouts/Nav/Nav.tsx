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
import { NavLink } from "react-router-dom";

import {
  useAuthLinksServiceGetAuthMenus,
  useVersionServiceGetVersion,
  useConfigServiceGetConfigs,
} from "openapi/queries";
import type { AppBuilderMenuItemResponse } from "openapi/requests/types.gen";
import { AirflowPin } from "src/assets/AirflowPin";
import { DagIcon } from "src/assets/DagIcon";

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
  items: Array<AppBuilderMenuItemResponse>,
): {
  adminItems: Array<AppBuilderMenuItemResponse>;
  browseItems: Array<AppBuilderMenuItemResponse>;
  docsItems: Array<AppBuilderMenuItemResponse>;
  topNavItems: Array<AppBuilderMenuItemResponse>;
  userItems: Array<AppBuilderMenuItemResponse>;
} => {
  const adminItems: Array<AppBuilderMenuItemResponse> = [];
  const browseItems: Array<AppBuilderMenuItemResponse> = [];
  const docsItems: Array<AppBuilderMenuItemResponse> = [];
  const topNavItems: Array<AppBuilderMenuItemResponse> = [];
  const userItems: Array<AppBuilderMenuItemResponse> = [];

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
  const { data: config } = useConfigServiceGetConfigs();
  const { t: translate } = useTranslation("common");

  // Get plugin menu items from config
  const navItems = config?.plugins_extra_menu_items ?? [];

  // Categorize navigation items
  const { adminItems, browseItems, docsItems, topNavItems, userItems } = categorizeNavItems(navItems);

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
      bg="blue.muted"
      height="100%"
      justifyContent="space-between"
      position="fixed"
      py={3}
      top={0}
      width={20}
      zIndex={2}
    >
      <Flex alignItems="center" flexDir="column" width="100%">
        <Box mb={3}>
          <NavLink to="/">
            <AirflowPin height="35px" width="35px" />
          </NavLink>
        </Box>
        <NavButton icon={<FiHome size="1.75rem" />} title={translate("nav.home")} to="/" />
        <NavButton
          disabled={!authLinks?.authorized_menu_items.includes("Dags")}
          icon={<DagIcon height="1.75rem" width="1.75rem" />}
          title={translate("nav.dags")}
          to="dags"
        />
        <NavButton
          disabled={!authLinks?.authorized_menu_items.includes("Assets")}
          icon={<FiDatabase size="1.75rem" />}
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
        <PluginMenus navItems={topNavItems} />
      </Flex>
      <Flex flexDir="column">
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
