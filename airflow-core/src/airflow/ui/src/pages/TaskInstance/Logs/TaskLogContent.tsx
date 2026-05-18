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
import { Box, Code, VStack } from "@chakra-ui/react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useLayoutEffect, useRef, useCallback, useEffect } from "react";
import { useHotkeys } from "react-hotkeys-hook";

import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar } from "src/components/ui";
import type { ParsedLogEntry } from "src/queries/useLogs";

import { HighlightedText } from "./HighlightedText";
import { ScrollToButton } from "./ScrollToButton";
import { useLogGroups } from "./useLogGroups";
import { getHighlightColor, scrollToBottom, scrollToTop } from "./utils";

export type TaskLogContentProps = {
  readonly currentMatchLineIndex?: number;
  readonly error: unknown;
  readonly expanded: boolean;
  readonly isLoading: boolean;
  readonly logError: unknown;
  readonly parsedLogs: Array<ParsedLogEntry>;
  readonly searchMatchIndices?: Set<number>;
  readonly searchQuery?: string;
  readonly wrap: boolean;
};

// How close to the bottom (in px) before we consider the user "at the bottom"
const SCROLL_BOTTOM_THRESHOLD = 100;

export const TaskLogContent = ({
  currentMatchLineIndex,
  error,
  expanded,
  isLoading,
  logError,
  parsedLogs,
  searchMatchIndices,
  searchQuery,
  wrap,
}: TaskLogContentProps) => {
  const hash = location.hash.replace("#", "");
  const parentRef = useRef<HTMLDivElement | null>(null);

  const {
    expandedGroups,
    originalToVisibleIndex,
    toggleGroup,
    visibleCurrentMatchIndex,
    visibleItems,
    visibleSearchMatchIndices,
  } = useLogGroups({ currentMatchLineIndex, expanded, parsedLogs, searchMatchIndices });

  const isAtBottomRef = useRef<boolean>(true);
  const prevVisibleCountRef = useRef<number>(0);

  const rowVirtualizer = useVirtualizer({
    count: visibleItems.length,
    estimateSize: () => 20,
    getScrollElement: () => parentRef.current,
    overscan: 10,
  });

  const contentHeight = rowVirtualizer.getTotalSize();
  const containerHeight = rowVirtualizer.scrollElement?.clientHeight ?? 0;
  const showScrollButtons = visibleItems.length > 1 && contentHeight > containerHeight;

  const handleScroll = useCallback(() => {
    const el = parentRef.current;

    if (!el) {
      return;
    }
    isAtBottomRef.current = el.scrollHeight - el.scrollTop - el.clientHeight <= SCROLL_BOTTOM_THRESHOLD;
  }, []);

  useEffect(() => {
    const el = parentRef.current;

    el?.addEventListener("scroll", handleScroll, { passive: true });

    return () => el?.removeEventListener("scroll", handleScroll);
  }, [handleScroll]);

  useLayoutEffect(() => {
    if (visibleItems.length === 0) {
      return;
    }
    const isFirstLoad = prevVisibleCountRef.current === 0;
    const hasNewLines = visibleItems.length > prevVisibleCountRef.current;

    if ((isFirstLoad || (hasNewLines && isAtBottomRef.current)) && !location.hash) {
      rowVirtualizer.scrollToIndex(visibleItems.length - 1, { align: "end" });
    }
    prevVisibleCountRef.current = visibleItems.length;
  }, [visibleItems.length, rowVirtualizer]);

  useLayoutEffect(() => {
    if (location.hash && !isLoading) {
      const hashVisibleIndex = originalToVisibleIndex.get(Number(hash) - 1);

      if (hashVisibleIndex !== undefined) {
        rowVirtualizer.scrollToIndex(Math.min(hashVisibleIndex + 5, visibleItems.length - 1));
      }
    }
    // React Compiler auto-memoizes; safe to include in deps
  }, [isLoading, rowVirtualizer, hash, visibleItems, originalToVisibleIndex]);

  useLayoutEffect(() => {
    if (visibleCurrentMatchIndex !== undefined && !isLoading) {
      rowVirtualizer.scrollToIndex(Math.min(visibleCurrentMatchIndex + 3, visibleItems.length - 1));
    }
    // React Compiler auto-memoizes; safe to include in deps
  }, [visibleCurrentMatchIndex, isLoading, rowVirtualizer, visibleItems]);

  const handleScrollTo = (to: "bottom" | "top") => {
    if (visibleItems.length === 0) {
      return;
    }
    const el = rowVirtualizer.scrollElement ?? parentRef.current;

    if (!el) {
      return;
    }
    if (to === "top") {
      isAtBottomRef.current = false;
      scrollToTop({ element: el, virtualizer: rowVirtualizer });
    } else {
      isAtBottomRef.current = true;
      scrollToBottom({ element: el, virtualizer: rowVirtualizer });
    }
  };

  useHotkeys("mod+ArrowDown", () => handleScrollTo("bottom"), { enabled: !isLoading });
  useHotkeys("mod+ArrowUp", () => handleScrollTo("top"), { enabled: !isLoading });

  return (
    <Box display="flex" flexDirection="column" flexGrow={1} h="100%" minHeight={0} position="relative">
      <ErrorAlert error={error ?? logError} />
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      <Box
        data-testid="virtual-scroll-container"
        flexGrow={1}
        minHeight={0}
        overflow="auto"
        position="relative"
        py={3}
        ref={parentRef}
        width="100%"
      >
        <Code
          css={{ "& *::selection": { bg: "blue.emphasized" } }}
          data-testid="virtualized-list"
          display="block"
          overflowX="auto"
          textWrap={wrap ? "pre" : "nowrap"}
          width="100%"
        >
          <VStack
            alignItems="flex-start"
            gap={0}
            h={`${rowVirtualizer.getTotalSize()}px`}
            position="relative"
          >
            {rowVirtualizer.getVirtualItems().map((virtualRow) => {
              const item = visibleItems[virtualRow.index];

              if (!item) {
                return undefined;
              }

              const { entry, originalIndex } = item;
              const isGroupHeader = entry.group?.type === "header";
              const groupLevel = entry.group?.level ?? 0;
              const indent = entry.group ? groupLevel * 4 + (isGroupHeader ? 0 : 4) : 0;

              if (isGroupHeader && entry.group) {
                const isExpanded = expandedGroups.has(entry.group.id);

                return (
                  <Box
                    _ltr={{ left: 0, right: "auto" }}
                    _rtl={{ left: "auto", right: 0 }}
                    bgColor={getHighlightColor({
                      currentMatchLineIndex: visibleCurrentMatchIndex,
                      hash,
                      index: virtualRow.index,
                      searchMatchIndices: visibleSearchMatchIndices,
                    })}
                    cursor="pointer"
                    data-index={virtualRow.index}
                    data-testid={`group-header-${virtualRow.index}`}
                    key={virtualRow.key}
                    onClick={() => entry.group && toggleGroup(entry.group.id)}
                    pl={indent}
                    position="absolute"
                    ref={rowVirtualizer.measureElement}
                    top={0}
                    transform={`translateY(${virtualRow.start}px)`}
                    width={wrap ? "100%" : "max-content"}
                  >
                    <Box
                      as="span"
                      color="fg.info"
                      data-testid={`summary-${typeof entry.element === "string" ? entry.element : ""}`}
                    >
                      <Box
                        as="span"
                        display="inline-block"
                        mr={1}
                        transform={isExpanded ? "rotate(90deg)" : "rotate(0deg)"}
                        transition="transform 0.15s"
                      >
                        {"\u25B6"}
                      </Box>
                      {visibleSearchMatchIndices?.has(virtualRow.index) ? (
                        <HighlightedText query={searchQuery}>
                          {typeof entry.element === "string" ? entry.element : undefined}
                        </HighlightedText>
                      ) : (
                        entry.element
                      )}
                    </Box>
                  </Box>
                );
              }

              return (
                <Box
                  _ltr={{ left: 0, right: "auto" }}
                  _rtl={{ left: "auto", right: 0 }}
                  bgColor={getHighlightColor({
                    currentMatchLineIndex: visibleCurrentMatchIndex,
                    hash,
                    index: virtualRow.index,
                    searchMatchIndices: visibleSearchMatchIndices,
                  })}
                  data-index={virtualRow.index}
                  data-original-index={originalIndex}
                  data-testid={`virtualized-item-${virtualRow.index}`}
                  key={virtualRow.key}
                  pl={indent}
                  position="absolute"
                  ref={rowVirtualizer.measureElement}
                  top={0}
                  transform={`translateY(${virtualRow.start}px)`}
                  width={wrap ? "100%" : "max-content"}
                >
                  {visibleSearchMatchIndices?.has(virtualRow.index) ? (
                    <HighlightedText query={searchQuery}>
                      {typeof entry.element === "string" ? entry.element : (entry.element ?? undefined)}
                    </HighlightedText>
                  ) : (
                    (entry.element ?? undefined)
                  )}
                </Box>
              );
            })}
          </VStack>
        </Code>
      </Box>
      {showScrollButtons ? (
        <>
          <ScrollToButton direction="top" onClick={() => handleScrollTo("top")} />
          <ScrollToButton direction="bottom" onClick={() => handleScrollTo("bottom")} />
        </>
      ) : undefined}
    </Box>
  );
};
