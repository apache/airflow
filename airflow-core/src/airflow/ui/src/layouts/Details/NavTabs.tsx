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
import { useLayoutEffect, useRef, useState, type ReactNode } from "react";

import { Box, Center, type CenterProps, Flex, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronDown } from "react-icons/fi";
import { NavLink, resolvePath, useLocation, useResolvedPath } from "react-router-dom";

import { Menu } from "src/system-components";

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
  readonly tabs: Array<NavTab>;
};

const INDICATOR_HEIGHT = "2px";

const trimTrailingSlash = (path: string) => path.replace(/\/$/u, "");

const TabContent = ({ active, children, ...rest }: { readonly active: boolean } & CenterProps) => (
  <Center
    _focus={{ color: active ? "fg" : "brand.solid" }}
    _hover={{ color: active ? "fg" : "brand.solid" }}
    borderBottomColor={active ? "brand.solid" : "transparent"}
    borderBottomWidth={INDICATOR_HEIGHT}
    color={active ? "fg" : "fg.muted"}
    fontSize="md"
    fontWeight={active ? "bold" : "medium"}
    height="40px"
    mb={`-${INDICATOR_HEIGHT}`}
    px={4}
    transition="all 0.2s ease"
    whiteSpace="nowrap"
    {...rest}
  >
    {children}
  </Center>
);

export const NavTabs = ({ tabs }: Props) => {
  const { t: translate } = useTranslation();
  const containerRef = useRef<HTMLDivElement>(null);
  const measureRef = useRef<HTMLDivElement>(null);
  const containerWidth = useContainerWidth(containerRef);
  const [widths, setWidths] = useState<{ more: number; tabs: Array<number> }>({ more: 0, tabs: [] });

  const { pathname } = useLocation();
  const basePath = useResolvedPath(".").pathname;
  // NavLink compares the raw location against its resolved target, and a run id carries ":" and
  // "+". Landing on a link where those arrived percent-encoded leaves the two spellings of the
  // same path looking different, so the tab for the page you are on reads as inactive.
  const current = trimTrailingSlash(decodeURIComponent(pathname));
  const lastSegment = current.split("/").pop() ?? "";
  const activeIndex = tabs.findIndex(
    ({ matchPaths, value }) =>
      current === trimTrailingSlash(decodeURIComponent(resolvePath(value, basePath).pathname)) ||
      (matchPaths ?? []).includes(lastSegment),
  );
  const showLabels = containerWidth > 600;
  const getTabContent = ({ icon, label }: NavTab) => (showLabels || !Boolean(icon) ? label : icon);
  const moreLabel = (
    <HStack gap={1}>
      {translate("moreTabs")}
      <FiChevronDown />
    </HStack>
  );

  // Measure every tab in a hidden copy, including the ones moved to the menu. The copy is rendered
  // bold, as the active tab is, so a tab never grows once it is selected.
  useLayoutEffect(() => {
    const measured = [...(measureRef.current?.children ?? [])].map(
      (child) => (child as HTMLElement).offsetWidth,
    );
    const more = measured.pop() ?? 0;

    setWidths((previous) =>
      previous.more === more && previous.tabs.join(",") === measured.join(",")
        ? previous
        : { more, tabs: measured },
    );
  }, [tabs, showLabels]);

  // Fill the row in order and move whatever does not fit behind "More". The active tab always
  // stays in the row, so the page being viewed is never hidden in the menu.
  const visibleIndexes = new Set(tabs.map((_, index) => index));

  if (widths.tabs.reduce((total, width) => total + width, 0) > containerWidth) {
    let available = containerWidth - widths.more - (widths.tabs[activeIndex] ?? 0);
    let isFull = false;

    visibleIndexes.clear();
    widths.tabs.forEach((width, index) => {
      if (index === activeIndex) {
        visibleIndexes.add(index);
      } else if (!isFull && width <= available) {
        available -= width;
        visibleIndexes.add(index);
      } else {
        isFull = true;
      }
    });
  }

  const overflowTabs = tabs.filter((_, index) => !visibleIndexes.has(index));

  return (
    <Flex
      alignItems="center"
      borderBottomColor="border.emphasized"
      borderBottomWidth={INDICATOR_HEIGHT}
      mb={2}
      position="relative"
      ref={containerRef}
    >
      {tabs.map((tab, index) =>
        visibleIndexes.has(index) ? (
          <NavLink end key={tab.value} title={tab.label} to={{ pathname: tab.value, search: tab.search }}>
            <TabContent
              active={index === activeIndex}
              aria-current={index === activeIndex ? "page" : undefined}
            >
              {getTabContent(tab)}
            </TabContent>
          </NavLink>
        ) : undefined,
      )}
      {overflowTabs.length > 0 ? (
        <Menu.Root positioning={{ placement: "bottom-end" }}>
          <Menu.Trigger asChild>
            <TabContent active={false} as="button" cursor="pointer" data-testid="nav-tabs-more">
              {moreLabel}
            </TabContent>
          </Menu.Trigger>
          <Menu.Content>
            {overflowTabs.map((tab) => (
              <Menu.Item asChild key={tab.value} value={tab.value}>
                <NavLink to={{ pathname: tab.value, search: tab.search }}>
                  {tab.icon}
                  {tab.label}
                </NavLink>
              </Menu.Item>
            ))}
          </Menu.Content>
        </Menu.Root>
      ) : undefined}
      <Box aria-hidden height={0} left={0} overflow="hidden" position="absolute" top={0} width={0}>
        <Flex ref={measureRef} visibility="hidden" width="max-content">
          {tabs.map((tab) => (
            <TabContent active key={tab.value}>
              {getTabContent(tab)}
            </TabContent>
          ))}
          <TabContent active={false}>{moreLabel}</TabContent>
        </Flex>
      </Box>
    </Flex>
  );
};
