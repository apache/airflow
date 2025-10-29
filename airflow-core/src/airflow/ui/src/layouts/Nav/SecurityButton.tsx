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
import { FiLock } from "react-icons/fi";
import { Link } from "react-router-dom";

import { useAuthLinksServiceGetAuthMenus } from "openapi/queries";
import { Menu } from "src/components/ui";

import { NavButton } from "./NavButton";

export const SecurityButton = () => {
  const { data: authLinks } = useAuthLinksServiceGetAuthMenus();
  const { t: translate } = useTranslation("common");

  if (authLinks?.extra_menu_items === undefined || authLinks.extra_menu_items.length < 1) {
    return undefined;
  }

  return (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger asChild>
        <NavButton icon={FiLock} title={translate("nav.security")} />
      </Menu.Trigger>
      <Menu.Content>
        {authLinks.extra_menu_items.map(({ text }) => {
          const securityKey = text.toLowerCase().replace(" ", "-");

          return (
            <Menu.Item asChild key={text} value={text}>
              <Link aria-label={text} to={`security/${securityKey}`}>
                {translate(`security.${securityKey}`)}
              </Link>
            </Menu.Item>
          );
        })}
      </Menu.Content>
    </Menu.Root>
  );
};
