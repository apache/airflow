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
import { Link, Image, Menu, Icon, Box } from "@chakra-ui/react";
import { FiExternalLink } from "react-icons/fi";
import { LuPlug } from "react-icons/lu";
import { RiArchiveStackLine } from "react-icons/ri";
import { Link as RouterLink } from "react-router-dom";

import type { ExternalViewResponse } from "openapi/requests/types.gen";
import { useColorMode } from "src/context/colorMode";
import type { NavItemResponse } from "src/utils/types";

import { NavButton } from "./NavButton";

type Props = { readonly topLevel?: boolean } & NavItemResponse;

export const PluginMenuItem = ({
  icon,
  icon_dark_mode: iconDarkMode,
  name,
  topLevel = false,
  url_route: urlRoute,
  ...rest
}: Props) => {
  // Determine if this is an external view or react app based on the presence of href
  const { colorMode } = useColorMode();
  const isExternalView = "href" in rest;
  const href = isExternalView ? (rest as ExternalViewResponse).href : undefined;

  const displayIcon = colorMode === "dark" && typeof iconDarkMode === "string" ? iconDarkMode : icon;
  const pluginIcon =
    typeof displayIcon === "string" ? (
      <Image boxSize={5} src={displayIcon} />
    ) : urlRoute === "legacy-fab-views" ? (
      <Icon as={RiArchiveStackLine} boxSize={5} />
    ) : (
      <Icon as={LuPlug} boxSize={5} />
    );

  const isExternal = urlRoute === undefined || urlRoute === null;

  if (topLevel) {
    return (
      <NavButton
        icon={LuPlug}
        isExternal={isExternal}
        key={name}
        pluginIcon={pluginIcon}
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
          <Box flex="1">{name}</Box>
          <Icon as={FiExternalLink} boxSize={4} color="fg.muted" />
        </Link>
      ) : (
        <RouterLink style={{ outline: "none" }} to={`plugin/${urlRoute}`}>
          {pluginIcon}
          <Box flex="1" ml={2}>
            {name}
          </Box>
        </RouterLink>
      )}
    </Menu.Item>
  );
};
