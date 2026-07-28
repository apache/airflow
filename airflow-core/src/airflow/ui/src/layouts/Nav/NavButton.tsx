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
import type { ReactNode, ForwardRefExoticComponent, RefAttributes } from "react";
import type { IconType } from "react-icons";
import { Link as RouterLink, matchPath, useLocation } from "react-router-dom";

const noMatchPaths: Array<string> = [];

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
  // Extra routes that should also mark this button active, on top of `to` (e.g. the Dags button
  // should also highlight for the standalone dag runs and task instances routes).
  readonly matchPaths?: Array<string>;
  readonly pluginIcon?: ReactNode;
  readonly title: string;
  // A single destination renders the button as a link; an array only affects isActive matching
  // (used for buttons like menu triggers that should highlight for any of several routes).
  readonly to?: Array<string> | string;
} & ButtonProps;

export const NavButton = ({
  icon,
  isExternal = false,
  matchPaths = noMatchPaths,
  pluginIcon,
  title,
  to,
  ...rest
}: NavButtonProps) => {
  const { pathname } = useLocation();

  const activePaths = [...(to === undefined ? [] : Array.isArray(to) ? to : [to]), ...matchPaths];
  const isActive = activePaths.some((path) => matchPath({ end: path === "/", path }, pathname) !== null);

  const commonButtonProps: ButtonProps = {
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
    "aria-current": isActive ? "page" : undefined,
    "aria-label": title,
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

    transition: "background-color 0.2s ease, color 0.2s ease",
    variant: "plain",
    whiteSpace: "wrap",
    ...rest,
  };

  if (to === undefined || Array.isArray(to)) {
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
