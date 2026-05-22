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
import { Flex } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronLeft, FiChevronRight, FiChevronsRight } from "react-icons/fi";

import { IconButton } from "src/components/ui";

type Props = {
  readonly hasNewerRuns: boolean;
  readonly hasOlderRuns: boolean;
  readonly isLoading: boolean;
  readonly latestNotVisible: boolean;
  readonly onJumpToLatest: () => void;
  readonly onNewerRuns: () => void;
  readonly onOlderRuns: () => void;
};

/**
 * Absolutely-positioned navigation buttons that appear at the left and right
 * edges of the grid header bar area.  They do not participate in the flex
 * layout so they never affect column alignment.
 */
export const GridPaginationButtons = ({
  hasNewerRuns,
  hasOlderRuns,
  isLoading,
  latestNotVisible,
  onJumpToLatest,
  onNewerRuns,
  onOlderRuns,
}: Props) => {
  const { t: translate } = useTranslation("dag");

  // Shared props applied to every pagination button.
  const buttonProps = { loading: isLoading, size: "2xs", variant: "ghost" } as const;

  const newerLabel = translate("grid.buttons.newerRuns");
  const resetLabel = translate("grid.buttons.resetToLatest");
  const olderLabel = translate("grid.buttons.olderRuns");

  return (
    <>
      {latestNotVisible || hasNewerRuns ? (
        <Flex bottom={0} flexDirection="column" gap={1} position="absolute" right={-6} zIndex={1}>
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
      ) : undefined}
      {hasOlderRuns ? (
        <IconButton
          bottom={0}
          label={olderLabel}
          left={-6}
          onClick={onOlderRuns}
          position="absolute"
          zIndex={1}
          {...buttonProps}
        >
          <FiChevronLeft />
        </IconButton>
      ) : undefined}
    </>
  );
};
