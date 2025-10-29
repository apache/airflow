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
  const parentRef = useRef(null);
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
    if (location.hash && !isLoading) {
      rowVirtualizer.scrollToIndex(Math.min(Number(hash) + 5, parsedLogs.length - 1));
    }
  }, [isLoading, rowVirtualizer, hash, parsedLogs]);

  useLayoutEffect(() => {
    // Force remeasurement when wrap changes since item heights will change
    rowVirtualizer.measure();
  }, [wrap, rowVirtualizer]);

  const handleScrollTo = (to: "bottom" | "top") => {
    if (parsedLogs.length > 0) {
      rowVirtualizer.scrollToIndex(to === "bottom" ? parsedLogs.length - 1 : 0);
    }
  };

  useHotkeys("mod+ArrowDown", () => handleScrollTo("bottom"), { enabled: !isLoading });
  useHotkeys("mod+ArrowUp", () => handleScrollTo("top"), { enabled: !isLoading });

  return (
    <Box display="flex" flexDirection="column" flexGrow={1} h="100%" minHeight={0} position="relative">
      <ErrorAlert error={error ?? logError} />
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      <Code
        css={{
          "& *::selection": {
            bg: "blue.emphasized",
          },
        }}
        data-testid="virtualized-list"
        flexGrow={1}
        h="auto"
        overflow="auto"
        position="relative"
        py={3}
        ref={parentRef}
        textWrap={wrap ? "pre-wrap" : "nowrap"}
        width="100%"
      >
        <VStack
          alignItems="flex-start"
          gap={0}
          h={`${rowVirtualizer.getTotalSize()}px`}
          minH="100%"
          position="relative"
          width="100%"
        >
          {rowVirtualizer.getVirtualItems().map((virtualRow) => (
            <Box
              _ltr={{
                left: 0,
                right: "auto",
              }}
              _rtl={{
                left: "auto",
                right: 0,
              }}
              bgColor={
                Boolean(hash) && virtualRow.index === Number(hash) - 1 ? "brand.emphasized" : "transparent"
              }
              data-index={virtualRow.index}
              data-testid={`virtualized-item-${virtualRow.index}`}
              key={virtualRow.key}
              minWidth="100%"
              position="absolute"
              ref={rowVirtualizer.measureElement}
              top={`${virtualRow.start}px`}
              width={wrap ? "100%" : "max-content"}
            >
              {parsedLogs[virtualRow.index] ?? undefined}
            </Box>
          ))}
          <Box
            bottom={0}
            left={0}
            minH={`calc(100% - ${rowVirtualizer.getTotalSize()}px)`}
            position="absolute"
            top={`${rowVirtualizer.getTotalSize()}px`}
            width="100%"
          />
        </VStack>
      </Code>

      {showScrollButtons ? (
        <>
          <ScrollToButton direction="top" onClick={() => handleScrollTo("top")} />
          <ScrollToButton direction="bottom" onClick={() => handleScrollTo("bottom")} />
        </>
      ) : undefined}
    </Box>
  );
};
