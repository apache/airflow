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
import { Box, type BoxProps, Button, Icon, type IconProps, Link, type ButtonProps } from "@chakra-ui/react";
import { type ReactNode, useMemo, type ForwardRefExoticComponent, type RefAttributes } from "react";
import type { IconType } from "react-icons";
import { Link as RouterLink, useMatch } from "react-router-dom";

const commonLabelProps: BoxProps = {
  fontSize: "2xs",
  overflow: "hidden",
  textAlign: "center",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
  width: "full",
};

type NavButtonProps = {
  readonly icon: ForwardRefExoticComponent<IconProps & RefAttributes<SVGSVGElement>> | IconType;
  readonly isExternal?: boolean;
  readonly pluginIcon?: ReactNode;
  readonly title: string;
  readonly to?: string;
} & ButtonProps;

export const NavButton = ({ icon, isExternal = false, pluginIcon, title, to, ...rest }: NavButtonProps) => {
  // Use useMatch to determine if the current route matches the button's destination
  // This provides the same functionality as NavLink's isActive prop
  // Only applies to buttons with a to prop (but needs to be before any return statements)
  const match = useMatch({
    end: to === "/", // Only exact match for root path
    path: to ?? "",
  });
  // Only applies to buttons with a to prop
  const isActive = Boolean(to) ? Boolean(match) : false;

  const commonButtonProps = useMemo<ButtonProps>(
    () => ({
      _expanded: isActive
        ? undefined
        : {
            bg: "brand.emphasized", // Even darker for better light mode contrast
            color: "fg",
          },
      _focus: isActive
        ? undefined
        : {
            color: "fg",
          },
      _hover: isActive
        ? undefined
        : {
            _active: {
              bg: "brand.solid",
              color: "white",
            },
            bg: "brand.emphasized", // Even darker for better light mode contrast
            color: "fg",
          },
      alignItems: "center",
      bg: isActive ? "brand.solid" : undefined,
      borderRadius: "md",
      borderWidth: 0,
      boxSize: 14,
      color: isActive ? "white" : "fg.muted",
      colorPalette: "brand",
      cursor: "pointer",
      flexDir: "column",
      gap: 0,
      overflow: "hidden",
      padding: 0,
      textDecoration: "none",
      title,
      transition: "background-color 0.2s ease, color 0.2s ease",
      variant: "plain",
      whiteSpace: "wrap",
      ...rest,
    }),
    [isActive, rest, title],
  );

  if (to === undefined) {
    return (
      <Button {...commonButtonProps}>
        {pluginIcon ?? <Icon as={icon} boxSize={5} />}
        <Box {...commonLabelProps}>{title}</Box>
      </Button>
    );
  }

  if (isExternal) {
    return (
      <Link asChild href={to} rel="noopener noreferrer" target="_blank">
        <Button {...commonButtonProps}>
          {pluginIcon ?? <Icon as={icon} boxSize={5} />}
          <Box {...commonLabelProps}>{title}</Box>
        </Button>
      </Link>
    );
  }

  return (
    <Button as={Link} asChild {...commonButtonProps}>
      <RouterLink to={to}>
        {pluginIcon ?? <Icon as={icon} boxSize={5} />}
        <Box {...commonLabelProps}>{title}</Box>
      </RouterLink>
    </Button>
  );
};
