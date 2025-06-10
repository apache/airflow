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
import { Box, Code, VStack, useToken } from "@chakra-ui/react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useLayoutEffect, useRef } from "react";

import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar } from "src/components/ui";

type Props = {
  readonly error: unknown;
  readonly isLoading: boolean;
  readonly logError: unknown;
  readonly parsedLogs: Array<JSX.Element | string | undefined>;
  readonly wrap: boolean;
};

export const TaskLogContent = ({ error, isLoading, logError, parsedLogs, wrap }: Props) => {
  const [bgLine] = useToken("colors", ["blue.emphasized"]);
  const parentRef = useRef(null);
  const rowVirtualizer = useVirtualizer({
    count: parsedLogs.length,
    estimateSize: () => 20,
    getScrollElement: () => parentRef.current,
    overscan: 10,
  });

  useLayoutEffect(() => {
    if (location.hash) {
      const hash = location.hash.replace("#", "");

      setTimeout(() => {
        const element = document.querySelector<HTMLElement>(`[id='${hash}']`);

        if (element !== null) {
          element.style.background = bgLine as string;
        }
        element?.scrollIntoView({
          behavior: "smooth",
          block: "center",
        });
      }, 100);
    }
  }, [isLoading, bgLine]);

  return (
    <Box display="flex" flexDirection="column" flexGrow={1} h="100%" minHeight={0}>
      <ErrorAlert error={error ?? logError} />
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      <Code
        css={{
          "& *::selection": {
            bg: "blue.subtle",
          },
        }}
        data-testid="virtualized-list"
        flexGrow={1}
        h="auto"
        overflow="auto"
        position="relative"
        py={3}
        ref={parentRef}
        textWrap={wrap ? "pre" : "nowrap"}
        width="100%"
      >
        <VStack alignItems="flex-start" gap={0} h={`${rowVirtualizer.getTotalSize()}px`}>
          {rowVirtualizer.getVirtualItems().map((virtualRow) => (
            <Box
              data-index={virtualRow.index}
              data-testid={`virtualized-item-${virtualRow.index}`}
              key={virtualRow.key}
              left={0}
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
  );
};
