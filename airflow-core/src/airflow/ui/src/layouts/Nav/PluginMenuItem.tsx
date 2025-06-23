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
      <Box alignItems="center" display="flex" gap={2} outline="none" px={2} py="6px">
        <Link
          aria-label={name}
          fontSize="sm"
          href={href}
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
    <RouterLink to={`plugin/${urlRoute}`}>
      <Box alignItems="center" display="flex" fontSize="sm" gap={2} px={2} py="6px">
        {typeof icon === "string" ? (
          <Image height="1.25rem" src={icon} width="1.25rem" />
        ) : (
          <LuPlug size="1.25rem" />
        )}
        {name}
      </Box>
    </RouterLink>
  );
};
