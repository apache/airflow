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
import { Box, Link, Image } from "@chakra-ui/react";
import { LuPlug } from "react-icons/lu";
import { RiArchiveStackLine } from "react-icons/ri";
import { Link as RouterLink } from "react-router-dom";

import type { ExternalViewResponse } from "openapi/requests/types.gen";

import { NavButton } from "./NavButton";

type Props = { readonly topLevel?: boolean } & ExternalViewResponse;

export const PluginMenuItem = ({ href, icon, name, topLevel = false, url_route: urlRoute }: Props) => {
  // External Link
  if (urlRoute === undefined || urlRoute === null) {
    return topLevel ? (
      <NavButton
        icon={
          typeof icon === "string" ? (
            <Image height="1.75rem" src={icon} width="1.75rem" />
          ) : (
            <LuPlug size="1.75rem" />
          )
        }
        isExternal={true}
        key={name}
        title={name}
        to={href}
      />
    ) : (
      <Box alignItems="center" display="flex" width="100%">
        <Link
          aria-label={name}
          fontSize="sm"
          href={href}
          outline="none"
          rel="noopener noreferrer"
          target="_blank"
          width="100%"
        >
          {name}
        </Link>
      </Box>
    );
  }

  // Embedded External Link via iframes
  if (topLevel) {
    return (
      <NavButton
        icon={
          typeof icon === "string" ? (
            <Image height="1.75rem" src={icon} width="1.75rem" />
          ) : (
            <LuPlug size="1.75rem" />
          )
        }
        key={name}
        title={name}
        to={`plugin/${urlRoute}`}
      />
    );
  }

  return (
    <Box width="100%">
      <RouterLink style={{ outline: "none" }} to={`plugin/${urlRoute}`}>
        <Box alignItems="center" display="flex" fontSize="sm">
          {typeof icon === "string" ? (
            <Image height="1.25rem" src={icon} width="1.25rem" />
          ) : urlRoute === "legacy-fab-views" ? (
            <RiArchiveStackLine size="1.25rem" />
          ) : (
            <LuPlug size="1.25rem" />
          )}
          {name}
        </Box>
      </RouterLink>
    </Box>
  );
};
