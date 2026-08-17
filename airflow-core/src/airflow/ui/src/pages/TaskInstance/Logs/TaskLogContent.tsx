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
import { defaultRangeExtractor, useVirtualizer } from "@tanstack/react-virtual";
import type { Range as VirtualizerRange } from "@tanstack/react-virtual";
import dayjs from "dayjs";
import tz from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import { useLayoutEffect, useRef, useCallback, useEffect } from "react";

import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar } from "src/components/ui";
import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useTimezone } from "src/context/timezone";
import { useShortcut } from "src/hooks/useShortcut";
import type { ParsedLogEntry } from "src/queries/useLogs";
import { DEFAULT_DATETIME_FORMAT } from "src/utils/datetimeUtils";

import { HighlightedText } from "./HighlightedText";
import { ScrollToButton } from "./ScrollToButton";
import {
  getBottomDragBoundary,
  getBottomDragClampTarget,
  extractSelectedLogText,
  getEntryText,
  getSelectionPinnedRows,
  mergePinnedIndexes,
} from "./logSelection";
import { useLogGroups } from "./useLogGroups";
import {
  getGroupHeaderMarker,
  getHighlightColor,
  isSelectionWithin,
  scrollToBottom,
  scrollToTop,
} from "./utils";

dayjs.extend(utc);
dayjs.extend(tz);

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

// How close to the very end (in px) the user must be for the log to keep
// following new lines. Small so that scrolling up even a little to read
// stops the follow; returning to the end resumes it.
const SCROLL_BOTTOM_THRESHOLD = 40;

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
  const { selectedTimezone } = useTimezone();
  const hash = location.hash.replace("#", "");
  const parentRef = useRef<HTMLDivElement | null>(null);

  const {
    expandedGroups,
    lineNumberToVisibleIndex,
    toggleGroup,
    visibleCurrentMatchIndex,
    visibleItems,
    visibleSearchMatchIndices,
  } = useLogGroups({ currentMatchLineIndex, expanded, parsedLogs, searchMatchIndices });

  const hashVisibleIndex = hash === "" ? undefined : lineNumberToVisibleIndex.get(Number(hash));

  const isAtBottomRef = useRef<boolean>(true);
  const prevVisibleCountRef = useRef<number>(0);
  const pinnedRowsRef = useRef<Array<number>>([]);
  const isSelectingRef = useRef<boolean>(false);
  // NaN disables clamping between drags.
  const lastPointerYRef = useRef<number>(Number.NaN);
  const dragClampRafRef = useRef<number>(0);

  const rangeExtractor = (range: VirtualizerRange) =>
    mergePinnedIndexes(defaultRangeExtractor(range), pinnedRowsRef.current, range.count);

  const rowVirtualizer = useVirtualizer({
    count: visibleItems.length,
    estimateSize: () => 20,
    getScrollElement: () => parentRef.current,
    overscan: 10,
    rangeExtractor,
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

  useEffect(() => {
    const container = parentRef.current;

    if (!container) {
      return undefined;
    }
    const clampSelectionToBottom = () => {
      dragClampRafRef.current = 0;

      if (!isSelectingRef.current) {
        return;
      }
      const selection = document.getSelection();

      if (!selection) {
        return;
      }
      const clampTarget = getBottomDragClampTarget({
        container,
        pointerY: lastPointerYRef.current,
        selection,
      });

      if (clampTarget) {
        selection.extend(clampTarget.node, clampTarget.offset);
      }
    };
    const scheduleBottomClamp = () => {
      if (!isSelectingRef.current || dragClampRafRef.current !== 0) {
        return;
      }
      const boundary = getBottomDragBoundary(container);

      if (boundary === undefined || lastPointerYRef.current < boundary.y) {
        return;
      }
      dragClampRafRef.current = requestAnimationFrame(clampSelectionToBottom);
    };
    const handleSelectionChange = () => {
      const selection = document.getSelection();

      pinnedRowsRef.current = getSelectionPinnedRows(selection, container);
      scheduleBottomClamp();
    };
    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target instanceof Element ? event.target.closest("[data-index]") : null;

      if (event.button !== 0 || event.pointerType !== "mouse" || !target || !container.contains(target)) {
        return;
      }
      isSelectingRef.current = true;
      lastPointerYRef.current = event.clientY;
    };
    const stopSelecting = () => {
      isSelectingRef.current = false;
      lastPointerYRef.current = Number.NaN;
      cancelAnimationFrame(dragClampRafRef.current);
      dragClampRafRef.current = 0;
    };
    const handlePointerMove = (event: PointerEvent) => {
      if (!isSelectingRef.current) {
        return;
      }
      lastPointerYRef.current = event.clientY;
      scheduleBottomClamp();
    };

    container.addEventListener("pointerdown", handlePointerDown);
    container.addEventListener("scroll", scheduleBottomClamp, { passive: true });
    document.addEventListener("selectionchange", handleSelectionChange);
    document.addEventListener("pointermove", handlePointerMove, { passive: true });
    document.addEventListener("pointerup", stopSelecting);
    document.addEventListener("pointercancel", stopSelecting);
    globalThis.addEventListener("blur", stopSelecting);

    return () => {
      container.removeEventListener("pointerdown", handlePointerDown);
      container.removeEventListener("scroll", scheduleBottomClamp);
      document.removeEventListener("selectionchange", handleSelectionChange);
      document.removeEventListener("pointermove", handlePointerMove);
      document.removeEventListener("pointerup", stopSelecting);
      document.removeEventListener("pointercancel", stopSelecting);
      globalThis.removeEventListener("blur", stopSelecting);
      cancelAnimationFrame(dragClampRafRef.current);
    };
  }, []);

  useEffect(() => {
    const handleCopy = (event: ClipboardEvent) => {
      const container = parentRef.current;
      const selection = document.getSelection();

      if (!container || !selection || !event.clipboardData) {
        return;
      }
      const text = extractSelectedLogText({
        container,
        getRowText: (index) => {
          const entry = visibleItems[index]?.entry;

          if (!entry) {
            return "";
          }
          const entryText = getEntryText(entry, expandedGroups);

          if (entry.timestamp === undefined || entry.timestamp === "") {
            return entryText;
          }
          const rawTimestampPrefix = `[${entry.timestamp}] `;

          if (!entryText.startsWith(rawTimestampPrefix)) {
            return entryText;
          }
          const timestamp = dayjs(entry.timestamp);
          const formattedTimestamp = timestamp.isValid()
            ? timestamp.tz(selectedTimezone).format(DEFAULT_DATETIME_FORMAT)
            : entry.timestamp;

          return `[${formattedTimestamp}] ${entryText.slice(rawTimestampPrefix.length)}`;
        },
        selection,
      });

      if (text === undefined) {
        return;
      }
      event.preventDefault();
      event.clipboardData.setData("text/plain", text);
    };

    document.addEventListener("copy", handleCopy);

    return () => document.removeEventListener("copy", handleCopy);
  }, [visibleItems, expandedGroups, selectedTimezone]);

  useLayoutEffect(() => {
    if (visibleItems.length === 0) {
      return;
    }
    const isFirstLoad = prevVisibleCountRef.current === 0;
    const hasNewLines = visibleItems.length > prevVisibleCountRef.current;
    // Pause following while the user is selecting text in the log — scrolling
    // would move the text out from under the cursor and clear the selection.
    const isSelecting = isSelectionWithin(document.getSelection(), parentRef.current);

    if ((isFirstLoad || (hasNewLines && isAtBottomRef.current && !isSelecting)) && !location.hash) {
      rowVirtualizer.scrollToIndex(visibleItems.length - 1, { align: "end" });
    }
    prevVisibleCountRef.current = visibleItems.length;
  }, [visibleItems.length, rowVirtualizer]);

  useLayoutEffect(() => {
    if (location.hash && !isLoading && hashVisibleIndex !== undefined) {
      rowVirtualizer.scrollToIndex(Math.min(hashVisibleIndex + 5, visibleItems.length - 1));
    }
    // React Compiler auto-memoizes; safe to include in deps
  }, [isLoading, rowVirtualizer, visibleItems, hashVisibleIndex]);

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

  useShortcut({
    ...SHORTCUTS.logs.scrollBottom,
    callback: () => handleScrollTo("bottom"),
    options: { enabled: !isLoading },
  });
  useShortcut({
    ...SHORTCUTS.logs.scrollTop,
    callback: () => handleScrollTo("top"),
    options: { enabled: !isLoading },
  });

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
                      hashIndex: hashVisibleIndex,
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
                      {getGroupHeaderMarker(isExpanded)}{" "}
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
                    hashIndex: hashVisibleIndex,
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
