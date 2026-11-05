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

import { Menu } from "src/system-components";

import type { NavItemResponse } from "src/utils/types";

import { NavButton } from "./NavButton";
import { NavMenu } from "./NavMenu";
import { PluginMenuItem } from "./PluginMenuItem";

type BrowseLink = {
  readonly href: string;
  readonly key: string;
  readonly labelKey?: string;
  // Shown to users who have any of these menu items.
  readonly titles: Array<MenuItem>;
};

const links: Array<BrowseLink> = [
  {
    href: "/events",
    key: "auditLog",
    titles: ["Audit Log"],
  },
  {
    href: "/dag_bundles",
    key: "dagBundles",
    // Must stay byte-identical to MenuItem.DAG_BUNDLES: the filter below matches on this string.
    titles: ["Dag Bundles"],
  },
  {
    href: "/dag_runs",
    key: "dagRuns",
    // Reuses the label the Dags pages already show on their tabs.
    labelKey: "dagRun_other",
    // There is no menu item of its own for Dag runs: they follow the Dags permission.
    titles: ["Dags"],
  },
  {
    href: "/deadlines",
    key: "deadlines",
    titles: ["Deadlines"],
  },
  {
    href: "/jobs",
    key: "jobs",
    titles: ["Jobs"],
  },
  {
    href: "/task_instances",
    key: "taskInstances",
    labelKey: "taskInstance_other",
    titles: ["Dags"],
  },
  {
    href: "/triggers",
    key: "triggers",
    // No menu item of its own: a trigger is shown to whoever can see its owner, a Dag's task instance or
    // run, or a watched asset, so the entry follows the Dags and Assets permissions.
    titles: ["Dags", "Assets"],
  },
  {
    href: "/xcoms",
    key: "xcoms",
    titles: ["XComs"],
  },
  {
    href: "/required_actions",
    key: "requiredActions",
    titles: ["Required Actions"],
  },
];

export const BrowseButton = ({
  authorizedMenuItems,
  externalViews,
}: {
  readonly authorizedMenuItems: Array<MenuItem>;
  readonly externalViews: Array<NavItemResponse>;
}) => {
  const { t: translate } = useTranslation();
  const authorizedLinks = links.filter(({ titles }) =>
    titles.some((title) => authorizedMenuItems.includes(title)),
  );
  const menuItems = authorizedLinks.map((link) => {
    const label = translate(link.labelKey ?? `browse.${link.key}`);

    return (
      <Menu.Item asChild key={link.key} value={label}>
        <RouterLink aria-label={label} to={link.href}>
          {label}
        </RouterLink>
      </Menu.Item>
    );
  });

  if (!menuItems.length && !externalViews.length) {
    return undefined;
  }

  return (
    <NavMenu
      trigger={
        <NavButton
          icon={FiGlobe}
          title={translate("nav.browse")}
          to={authorizedLinks.map(({ href }) => href)}
        />
      }
    >
      {menuItems}
      {externalViews.map((view) => (
        <PluginMenuItem {...view} key={view.name} />
      ))}
    </NavMenu>
  );
};
