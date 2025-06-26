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

import { Box, Image, Link } from "@chakra-ui/react";
import { LuPlug } from "react-icons/lu";
import { Link as RouterLink, type To } from "react-router-dom";

// @ts-ignore
import type { ExternalViewResponse } from "openapi/requests/types.gen";

import { NavButton } from "./NavButton";

type Props = {
  readonly topLevel?: boolean;
} & ExternalViewResponse

const PluginMenuItemContent = ({
  icon,
  name,
  topLevel,
}: Pick<Props, "icon" | "name" | "topLevel">) => (
  <Box alignItems="center" display="flex" gap={2}>
    {typeof icon === "string" ? (
      <Image height={topLevel ? "1.75rem" : "1.25rem"} src={icon} width={topLevel ? "1.75rem" : "1.25rem"} />
    ) : (
      <LuPlug size={topLevel ? "1.75rem" : "1.25rem"} />
    )}
    {name}
  </Box>
);

export const PluginMenuItem = ({
  href,
  icon,
  name,
  topLevel,
  url_route,
}: Props) => {
  let to: To | undefined;

  if (url_route) {
    to = {
      pathname: `/plugin/${url_route}`,
    };
  }

  if (topLevel) {
    const topLevelTo = typeof to === "object" ? to.pathname : to;

    return (
      <NavButton
        icon={
          typeof icon === "string" ? (
            <Image height="1.75rem" src={icon} width="1.75rem" />
          ) : (
            <LuPlug size="1.75rem" />
          )
        }
        title={name}
        to={topLevelTo}
      />
    );
  }

  if (to) {
    return (
      <RouterLink style={{ width: "100%" }} to={to}>
        <PluginMenuItemContent icon={icon} name={name} topLevel={topLevel} />
      </RouterLink>
    );
  }
  if (href) {
    return (
      <Link
        aria-label={name}
        href={href ?? undefined}
        rel="noopener noreferrer"
        target="_blank"
        width="100%"
      >
        <PluginMenuItemContent icon={icon} name={name} topLevel={topLevel} />
      </Link>
    );
  }

  return null;
};
