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
import { FiSettings } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import type { MenuItem, AppBuilderMenuItemResponse } from "openapi/requests/types.gen";
import { Menu } from "src/components/ui";

import { NavButton } from "./NavButton";

const links = [
  {
    href: "/variables",
    title: "Variables",
  },
  {
    href: "/pools",
    title: "Pools",
  },
  {
    href: "/providers",
    title: "Providers",
  },
  {
    href: "/plugins",
    title: "Plugins",
  },
  {
    href: "/connections",
    title: "Connections",
  },
  {
    href: "/configs",
    title: "Config",
  },
];

export const AdminButton = ({
  authorizedMenuItems,
  externalViews,
}: {
  readonly authorizedMenuItems: Array<MenuItem>;
  readonly externalViews: Array<AppBuilderMenuItemResponse>;
}) => {
  const { t: translate } = useTranslation("common");

  type CombinedView = {
    href: string;
    title: string;
  };

  // Combine static links and external views
  const allViews: Array<CombinedView> = [
    ...links,
    ...externalViews
      .filter((view) => Boolean(view.href) && Boolean(view.name))
      .map((view) => ({
        href: view.href,
        title: view.name,
      })),
  ];

  const menuItems = allViews
    .filter(({ title }) => authorizedMenuItems.includes(title as MenuItem))
    .map((view) => (
      <Menu.Item asChild key={view.title} value={view.title}>
        <RouterLink
          aria-label={translate(`admin.${view.title}`, { defaultValue: view.title })}
          to={view.href}
        >
          {translate(`admin.${view.title}`, { defaultValue: view.title })}
        </RouterLink>
      </Menu.Item>
    ));

  if (!menuItems.length) {
    return undefined;
  }

  return (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger asChild>
        <NavButton icon={<FiSettings size="1.75rem" />} title={translate("nav.admin")} />
      </Menu.Trigger>
      <Menu.Content>{menuItems}</Menu.Content>
    </Menu.Root>
  );
};
