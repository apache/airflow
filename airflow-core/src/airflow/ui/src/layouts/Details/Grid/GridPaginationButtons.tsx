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
import { Box, Flex } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronLeft, FiChevronRight, FiChevronsRight } from "react-icons/fi";

import { IconButton } from "src/system-components";

type Props = {
  readonly hasNewerRuns: boolean;
  readonly hasOlderRuns: boolean;
  readonly isLoading: boolean;
  readonly latestNotVisible: boolean;
  readonly onJumpToLatest: () => void;
  readonly onNewerRuns: () => void;
  readonly onOlderRuns: () => void;
  readonly side: "newer" | "older";
  readonly taskNameColumnWidthPx: number;
};

/**
 * Navigation buttons pinned to the left and right edges of the grid's scrollport, so paging stays
 * reachable however far the runs are scrolled. Each side is a zero-width sticky box: sticky keeps
 * it at the scrollport edge instead of the content's, and zero width keeps it out of the flex
 * layout, so the buttons still never affect column alignment.
 *
 * Sticky holds a box at its natural position until that position would cross the edge, so each side
 * has to sit where it belongs in the flow: `side` picks which one this is, and the Grid renders the
 * older side before the run columns and the newer side after them.
 */
export const GridPaginationButtons = ({
  hasNewerRuns,
  hasOlderRuns,
  isLoading,
  latestNotVisible,
  onJumpToLatest,
  onNewerRuns,
  onOlderRuns,
  side,
  taskNameColumnWidthPx,
}: Props) => {
  const { t: translate } = useTranslation("dag");

  // Shared props applied to every pagination button.
  const buttonProps = { loading: isLoading, size: "2xs", variant: "ghost" } as const;

  const newerLabel = translate("grid.buttons.newerRuns");
  const resetLabel = translate("grid.buttons.resetToLatest");
  const olderLabel = translate("grid.buttons.olderRuns");

  if (side === "newer") {
    return latestNotVisible || hasNewerRuns ? (
      <Box insetEnd={1} position="sticky" width={0} zIndex={1}>
        <Flex bottom={0} flexDirection="column" gap={1} insetEnd={0} position="absolute">
          {latestNotVisible ? (
            <IconButton label={resetLabel} onClick={onJumpToLatest} {...buttonProps}>
              <FiChevronsRight />
            </IconButton>
          ) : undefined}
          {hasNewerRuns ? (
            <IconButton label={newerLabel} onClick={onNewerRuns} {...buttonProps}>
              <FiChevronRight />
            </IconButton>
          ) : undefined}
        </Flex>
      </Box>
    ) : undefined;
  }

  // Stopping at the task name column's inner edge rather than the scrollport's keeps the button
  // overlapping that column by the same few pixels as when nothing is scrolled.
  return hasOlderRuns ? (
    <Box insetStart={`${taskNameColumnWidthPx}px`} position="sticky" width={0} zIndex={1}>
      <IconButton
        bottom={0}
        insetStart={-6}
        label={olderLabel}
        onClick={onOlderRuns}
        position="absolute"
        {...buttonProps}
      >
        <FiChevronLeft />
      </IconButton>
    </Box>
  ) : undefined;
};
