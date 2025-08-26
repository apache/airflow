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
import { Box, Code, VStack, IconButton } from "@chakra-ui/react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useLayoutEffect, useMemo, useRef } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { FiChevronDown, FiChevronUp } from "react-icons/fi";

import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar, Tooltip } from "src/components/ui";
import { getMetaKey } from "src/utils";

type Props = {
  readonly error: unknown;
  readonly isLoading: boolean;
  readonly logError: unknown;
  readonly parsedLogs: Array<JSX.Element | string | undefined>;
  readonly wrap: boolean;
};

const ScrollToButton = ({
  direction,
  onClick,
}: {
  readonly direction: "bottom" | "top";
  readonly onClick: () => void;
}) => {
  const { t: translate } = useTranslation("common");

  return (
    <Tooltip
      closeDelay={100}
      content={translate("scroll.tooltip", {
        direction: translate(`scroll.direction.${direction}`),
        hotkey: `${getMetaKey()}+${direction === "bottom" ? "↓" : "↑"}`,
      })}
      openDelay={100}
    >
      <IconButton
        _ltr={{
          left: "auto",
          right: 4,
        }}
        _rtl={{
          left: 4,
          right: "auto",
        }}
        aria-label={translate(`scroll.direction.${direction}`)}
        bg="bg.panel"
        bottom={direction === "bottom" ? 4 : 14}
        onClick={onClick}
        position="absolute"
        rounded="full"
        size="xs"
        variant="outline"
      >
        {direction === "bottom" ? <FiChevronDown /> : <FiChevronUp />}
      </IconButton>
    </Tooltip>
  );
};

export const TaskLogContent = ({ error, isLoading, logError, parsedLogs, wrap }: Props) => {
  const hash = location.hash.replace("#", "");
  const parentRef = useRef<HTMLDivElement | null>(null);

  const rowVirtualizer = useVirtualizer({
    count: parsedLogs.length,
    estimateSize: () => 20,
    getScrollElement: () => parentRef.current,
    overscan: 10,
  });

  const showScrollButtons = useMemo(() => {
    const contentHeight = rowVirtualizer.getTotalSize();
    const containerHeight = rowVirtualizer.scrollElement?.clientHeight ?? 0;

    return parsedLogs.length > 1 && contentHeight > containerHeight;
  }, [rowVirtualizer, parsedLogs]);

  useLayoutEffect(() => {
    if (!location.hash || isLoading || parsedLogs.length === 0) {
      return;
    }

    const targetIndex = Math.max(0, Math.min(parsedLogs.length - 1, Number(hash) || 0));
    const el = parentRef.current;

    const total = rowVirtualizer.getTotalSize();
    const clientH = el?.clientHeight ?? 0;

    const vItem = rowVirtualizer.getVirtualItems().find((virtualRow) => virtualRow.index === targetIndex);
    const approxPerItem = 20;
    const anchor = vItem?.start ?? targetIndex * approxPerItem;

    const offset = Math.max(0, Math.min(total - clientH, anchor));

    if (el) {
      if (typeof rowVirtualizer.scrollToOffset === "function") {
        try {
          rowVirtualizer.scrollToOffset(offset);
        } catch {
          rowVirtualizer.scrollToIndex(targetIndex, { align: "start" });
        }
      } else {
        rowVirtualizer.scrollToIndex(targetIndex, { align: "start" });
      }

      el.scrollTop = offset;

      requestAnimationFrame(() => {
        el.scrollTop = offset;
      });
    } else {
      rowVirtualizer.scrollToIndex(targetIndex, { align: "start" });
    }
  }, [isLoading, rowVirtualizer, hash, parsedLogs]);

  const handleScrollTo = (to: "bottom" | "top") => {
    if (parsedLogs.length === 0) {
      return;
    }

    const el = rowVirtualizer.scrollElement ?? parentRef.current;

    if (!el) {
      return;
    }

    if (to === "top") {
      if (typeof rowVirtualizer.scrollToOffset === "function") {
        try {
          rowVirtualizer.scrollToOffset(0);
        } catch {
          rowVirtualizer.scrollToIndex(0, { align: "start" });
        }
      } else {
        rowVirtualizer.scrollToIndex(0, { align: "start" });
      }
      el.scrollTop = 0;
      requestAnimationFrame(() => {
        el.scrollTop = 0;
      });

      return;
    }

    // === bottom === (instant jump even for huge lists)
    const total = rowVirtualizer.getTotalSize();
    const clientH = el.clientHeight || 0;
    const offset = Math.max(0, Math.floor(total - clientH));

    // First tell the virtualizer where we want to be
    if (typeof rowVirtualizer.scrollToOffset === "function") {
      try {
        rowVirtualizer.scrollToOffset(offset);
      } catch {
        rowVirtualizer.scrollToIndex(parsedLogs.length - 1, { align: "end" });
      }
    } else {
      rowVirtualizer.scrollToIndex(parsedLogs.length - 1, { align: "end" });
    }

    el.scrollTop = offset;

    requestAnimationFrame(() => {
      el.scrollTop = offset;
      requestAnimationFrame(() => {
        el.scrollTop = offset;
        const lastItem = el.querySelector<HTMLElement>(
          `[data-testid="virtualized-item-${parsedLogs.length - 1}"]`,
        );

        if (lastItem) {
          lastItem.scrollIntoView({ behavior: "auto", block: "end" });
        }
      });
    });
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
          css={{
            "& *::selection": { bg: "blue.subtle" },
          }}
          data-testid="virtualized-list"
          display="block"
          textWrap={wrap ? "pre" : "nowrap"}
          width="100%"
        >
          <VStack
            alignItems="flex-start"
            gap={0}
            h={`${rowVirtualizer.getTotalSize()}px`}
            position="relative"
          >
            {rowVirtualizer.getVirtualItems().map((virtualRow) => (
              <Box
                _ltr={{ left: 0, right: "auto" }}
                _rtl={{ left: "auto", right: 0 }}
                bgColor={
                  Boolean(hash) && virtualRow.index === Number(hash) - 1 ? "blue.emphasized" : "transparent"
                }
                data-index={virtualRow.index}
                data-testid={`virtualized-item-${virtualRow.index}`}
                key={virtualRow.key}
                position="absolute"
                ref={rowVirtualizer.measureElement}
                top={0}
                transform={`translateY(${virtualRow.start}px)`}
                width={wrap ? "100%" : "max-content"}
              >
                {parsedLogs[virtualRow.index] ?? undefined}
              </Box>
            ))}
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
