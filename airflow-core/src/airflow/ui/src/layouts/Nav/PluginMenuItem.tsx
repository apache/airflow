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
import { Link, Image, Menu } from "@chakra-ui/react";
import { FiExternalLink } from "react-icons/fi";
import { LuPlug } from "react-icons/lu";
import { RiArchiveStackLine } from "react-icons/ri";
import { Link as RouterLink } from "react-router-dom";

import type { ExternalViewResponse } from "openapi/requests/types.gen";

import { NavButton } from "./NavButton";

type Props = { readonly topLevel?: boolean } & ExternalViewResponse;

export const PluginMenuItem = ({ href, icon, name, topLevel = false, url_route: urlRoute }: Props) => {
  const pluginIcon =
    typeof icon === "string" ? (
      <Image height="1.25rem" mr={topLevel ? 0 : 2} src={icon} width="1.25rem" />
    ) : urlRoute === "legacy-fab-views" ? (
      <RiArchiveStackLine size="1.25rem" style={{ marginRight: topLevel ? 0 : "8px" }} />
    ) : (
      <LuPlug size="1.25rem" style={{ marginRight: topLevel ? 0 : "8px" }} />
    );

  const isExternal = urlRoute === undefined || urlRoute === null;

  if (topLevel) {
    return (
      <NavButton
        icon={pluginIcon}
        isExternal={isExternal}
        key={name}
        title={name}
        to={isExternal ? href : `plugin/${urlRoute}`}
      />
    );
  }

  return (
    <Menu.Item asChild value={name}>
      {isExternal ? (
        <Link
          aria-label={name}
          fontSize="sm"
          href={href}
          outline="none"
          rel="noopener noreferrer"
          target="_blank"
          width="100%"
        >
          {pluginIcon}
          {name}
          <FiExternalLink />
        </Link>
      ) : (
        <RouterLink style={{ outline: "none" }} to={`plugin/${urlRoute}`}>
          {pluginIcon}
          {name}
        </RouterLink>
      )}
    </Menu.Item>
  );
};
