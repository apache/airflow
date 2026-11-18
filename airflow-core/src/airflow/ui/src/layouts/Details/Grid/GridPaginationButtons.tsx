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
/**
 * Paging buttons for the Grid's run columns, one component per side.
 *
 * Both sides are pinned to the edges of the Grid's scrollport, so paging stays reachable however
 * far the runs are scrolled. Each is a zero-width sticky box: sticky keeps it at the scrollport's
 * edge instead of the content's, and zero width keeps it out of the flex layout, so the buttons
 * never affect column alignment.
 *
 * Sticky holds a box at its natural position until that position would cross the edge, so each side
 * has to sit where it belongs in the flow: the Grid renders OlderRunsButton before the run columns
 * and NewerRunsButtons after them.
 */
import { Box, Flex } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronLeft, FiChevronRight, FiChevronsRight } from "react-icons/fi";

import { IconButton } from "src/system-components";

const BUTTON_PROPS = { size: "2xs", variant: "ghost" } as const;

type NewerProps = {
  readonly hasNewerRuns: boolean;
  readonly isLoading: boolean;
  readonly latestNotVisible: boolean;
  readonly onJumpToLatest: () => void;
  readonly onNewerRuns: () => void;
};

type OlderProps = {
  readonly hasOlderRuns: boolean;
  readonly isLoading: boolean;
  readonly onOlderRuns: () => void;
  readonly taskNameColumnWidthPx: number;
};

export const NewerRunsButtons = ({
  hasNewerRuns,
  isLoading,
  latestNotVisible,
  onJumpToLatest,
  onNewerRuns,
}: NewerProps) => {
  const { t: translate } = useTranslation("dag");

  if (!latestNotVisible && !hasNewerRuns) {
    return undefined;
  }

  return (
    <Box insetEnd={1} position="sticky" width={0} zIndex={1}>
      <Flex bottom={0} flexDirection="column" gap={1} insetEnd={0} position="absolute">
        {latestNotVisible ? (
          <IconButton
            label={translate("grid.buttons.resetToLatest")}
            loading={isLoading}
            onClick={onJumpToLatest}
            {...BUTTON_PROPS}
          >
            <FiChevronsRight />
          </IconButton>
        ) : undefined}
        {hasNewerRuns ? (
          <IconButton
            label={translate("grid.buttons.newerRuns")}
            loading={isLoading}
            onClick={onNewerRuns}
            {...BUTTON_PROPS}
          >
            <FiChevronRight />
          </IconButton>
        ) : undefined}
      </Flex>
    </Box>
  );
};

export const OlderRunsButton = ({
  hasOlderRuns,
  isLoading,
  onOlderRuns,
  taskNameColumnWidthPx,
}: OlderProps) => {
  const { t: translate } = useTranslation("dag");

  if (!hasOlderRuns) {
    return undefined;
  }

  // Stopping at the task name column's inner edge rather than the scrollport's keeps the button
  // overlapping that column by the same few pixels as when nothing is scrolled.
  return (
    <Box insetStart={`${taskNameColumnWidthPx}px`} position="sticky" width={0} zIndex={1}>
      <IconButton
        bottom={0}
        insetStart={-6}
        label={translate("grid.buttons.olderRuns")}
        loading={isLoading}
        onClick={onOlderRuns}
        position="absolute"
        {...BUTTON_PROPS}
      >
        <FiChevronLeft />
      </IconButton>
    </Box>
  );
};
