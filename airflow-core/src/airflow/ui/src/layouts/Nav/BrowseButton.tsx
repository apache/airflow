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
import { useTranslation } from "react-i18next";
import { FiGlobe } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import type { MenuItem } from "openapi/requests/types.gen";
import { Menu } from "src/components/ui";
import type { NavItemResponse } from "src/utils/types";

import { NavButton } from "./NavButton";
import { PluginMenuItem } from "./PluginMenuItem";

const links = [
  {
    href: "/events",
    key: "auditLog",
    title: "Audit Log",
  },
  {
    href: "/xcoms",
    key: "xcoms",
    title: "XComs",
  },
  {
    href: "/required_actions",
    key: "requiredActions",
    title: "Required Actions",
  },
];

export const BrowseButton = ({
  authorizedMenuItems,
  externalViews,
}: {
  readonly authorizedMenuItems: Array<MenuItem>;
  readonly externalViews: Array<NavItemResponse>;
}) => {
  const { t: translate } = useTranslation("common");
  const menuItems = links
    .filter(({ title }) => authorizedMenuItems.includes(title as MenuItem))
    .map((link) => (
      <Menu.Item asChild key={link.key} value={translate(`browse.${link.key}`)}>
        <RouterLink aria-label={translate(`browse.${link.key}`)} to={link.href}>
          {translate(`browse.${link.key}`)}
        </RouterLink>
      </Menu.Item>
    ));

  if (!menuItems.length && !externalViews.length) {
    return undefined;
  }

  return (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger asChild>
        <NavButton icon={FiGlobe} title={translate("nav.browse")} />
      </Menu.Trigger>
      <Menu.Content>
        {menuItems}
        {externalViews.map((view) => (
          <PluginMenuItem {...view} key={view.name} />
        ))}
      </Menu.Content>
    </Menu.Root>
  );
};
