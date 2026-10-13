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
import { Link as RouterLink } from "react-router-dom";

import type { MenuItem } from "openapi/requests/types.gen";

import { Menu } from "src/system-components";

import { DagIcon } from "src/assets/DagIcon";

import { NavButton } from "./NavButton";
import { NavMenu } from "./NavMenu";

const links = [
  {
    href: "/dags",
    key: "dag_other",
  },
  {
    href: "/dag_runs",
    key: "dagRun_other",
  },
  {
    href: "/task_instances",
    key: "taskInstance_other",
  },
];

export const DagsButton = ({ authorizedMenuItems }: { readonly authorizedMenuItems: Array<MenuItem> }) => {
  const { t: translate } = useTranslation();
  const isAuthorized = authorizedMenuItems.includes("Dags");

  // Stays a link to the Dags list: hovering reveals the menu, clicking goes straight there.
  const trigger = (
    <NavButton
      data-testid="nav-dags-link"
      disabled={!isAuthorized}
      icon={DagIcon}
      matchPaths={["dag_runs", "task_instances"]}
      title={translate("nav.dags")}
      to="/dags"
    />
  );

  if (!isAuthorized) {
    return trigger;
  }

  return (
    <NavMenu trigger={trigger}>
      {links.map(({ href, key }) => (
        <Menu.Item asChild key={key} value={key}>
          <RouterLink aria-label={translate(key)} to={href}>
            {translate(key)}
          </RouterLink>
        </Menu.Item>
      ))}
    </NavMenu>
  );
};
