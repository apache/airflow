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
import { useRef, type ReactNode } from "react";

import { Center, Flex } from "@chakra-ui/react";
import { NavLink, useLocation, useResolvedPath } from "react-router-dom";

import { useContainerWidth } from "src/utils";

export type NavTab = {
  readonly icon?: ReactNode;
  readonly label: string;
  /** Additional route segments that should also mark this tab as active. */
  readonly matchPaths?: Array<string>;
  readonly search?: string;
  readonly value: string;
};

type Props = {
  /** Query parameters to copy from the current url onto every tab link. */
  readonly persistentKeys?: ReadonlyArray<string>;
  readonly tabs: Array<NavTab>;
};

const INDICATOR_HEIGHT = "2px";
const NO_KEYS: ReadonlyArray<string> = [];

const trimTrailingSlash = (path: string) => path.replace(/\/$/u, "");

// A tab link replaces the query string rather than adding to it, so only the persistent keys carry over.
const buildTabSearch = (
  currentSearch: string,
  tabSearch: string | undefined,
  persistentKeys: ReadonlyArray<string>,
) => {
  const current = new URLSearchParams(currentSearch);
  const tabParams = new URLSearchParams(tabSearch);

  for (const key of persistentKeys) {
    if (!tabParams.has(key)) {
      for (const value of current.getAll(key)) {
        tabParams.append(key, value);
      }
    }
  }

  return tabParams.toString();
};

const NavTabLink = ({
  containerWidth,
  icon,
  label,
  matchPaths,
  persistentKeys,
  search,
  value,
}: { readonly containerWidth: number; readonly persistentKeys: ReadonlyArray<string> } & NavTab) => {
  const { pathname, search: currentSearch } = useLocation();
  const to = { pathname: value, search: buildTabSearch(currentSearch, search, persistentKeys) };
  const resolved = useResolvedPath(to);
  // NavLink compares the raw location against its resolved target, and a run id carries ":" and
  // "+". Landing on a link where those arrived percent-encoded leaves the two spellings of the
  // same path looking different, so the tab for the page you are on reads as inactive.
  const current = trimTrailingSlash(decodeURIComponent(pathname));
  const target = trimTrailingSlash(decodeURIComponent(resolved.pathname));
  const lastSegment = current.split("/").pop() ?? "";
  const active = current === target || (matchPaths ?? []).includes(lastSegment);

  return (
    <NavLink end title={label} to={to}>
      <Center
        _focus={{ color: active ? "fg" : "brand.solid" }}
        _hover={{ color: active ? "fg" : "brand.solid" }}
        aria-current={active ? "page" : undefined}
        borderBottomColor={active ? "brand.solid" : "transparent"}
        borderBottomWidth={INDICATOR_HEIGHT}
        color={active ? "fg" : "fg.muted"}
        fontSize="md"
        fontWeight={active ? "bold" : "medium"}
        height="40px"
        mb={`-${INDICATOR_HEIGHT}`}
        px={4}
        transition="all 0.2s ease"
      >
        {containerWidth > 600 || !Boolean(icon) ? label : icon}
      </Center>
    </NavLink>
  );
};

export const NavTabs = ({ persistentKeys = NO_KEYS, tabs }: Props) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const containerWidth = useContainerWidth(containerRef);

  return (
    <Flex
      alignItems="center"
      borderBottomColor="border.emphasized"
      borderBottomWidth={INDICATOR_HEIGHT}
      mb={2}
      ref={containerRef}
    >
      {tabs.map((tab) => (
        <NavTabLink
          containerWidth={containerWidth}
          key={tab.value}
          persistentKeys={persistentKeys}
          {...tab}
        />
      ))}
    </Flex>
  );
};
