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
import type { CSSProperties } from "react";

import { Box, type TextProps } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiArrowDownRight, FiArrowUpRight } from "react-icons/fi";
import { MdLoop } from "react-icons/md";

import type { NodeResponse } from "openapi/requests/types.gen";

import { formatNumber } from "src/utils";

export type TaskNameProps = {
  readonly childCount?: number;
  readonly isGroup?: boolean;
  readonly isLoop?: boolean;
  readonly isMapped?: boolean;
  readonly isOpen?: boolean;
  readonly isZoomedOut?: boolean;
  readonly label: string;
  /** Iterations that actually ran. Omitted where only the definition is known. */
  readonly loopIterationsRan?: number | null;
  readonly loopMaxIterations?: number | null;
  readonly setupTeardownType?: NodeResponse["setup_teardown_type"];
} & TextProps;

const iconStyle: CSSProperties = {
  display: "inline",
  position: "relative",
  verticalAlign: "middle",
};

export const TaskName = ({
  childCount,
  isGroup = false,
  isLoop = false,
  isMapped = false,
  isOpen = false,
  isZoomedOut,
  label,
  loopIterationsRan,
  loopMaxIterations,
  setupTeardownType,
  ...rest
}: TaskNameProps) => {
  const { i18n } = useTranslation();
  // The cap alone reads as a count, so a loop that stopped at 4 of 10 would claim 10. Show what
  // ran against what was allowed, and fall back to the cap only where no run is in view.
  const loopIterationsLabel =
    loopIterationsRan === null || loopIterationsRan === undefined
      ? (loopMaxIterations ?? undefined)
      : `${loopIterationsRan}/${loopMaxIterations ?? "?"}`;

  if (isGroup) {
    return (
      <Box
        fontSize="md"
        fontWeight="bold"
        overflow="hidden"
        textOverflow="ellipsis"
        whiteSpace="nowrap"
        {...rest}
      >
        {label}
        {/* A group can be both: a loop whose body is a mapped expansion carries both markers. */}
        {isLoop ? (
          <>
            <MdLoop size={14} style={{ ...iconStyle, marginLeft: 4 }} />
            {loopIterationsLabel === undefined ? undefined : ` ${loopIterationsLabel}`}
          </>
        ) : undefined}
        {isMapped ? " [ ]" : undefined}
      </Box>
    );
  }

  return (
    <Box
      fontSize={isZoomedOut ? "lg" : "md"}
      fontWeight="bold"
      overflow="hidden"
      textOverflow="ellipsis"
      whiteSpace="nowrap"
      {...rest}
    >
      {label}
      {isMapped
        ? ` [${childCount === undefined ? " " : formatNumber(childCount, i18n.language)}]`
        : undefined}
      {setupTeardownType === "setup" && <FiArrowUpRight size={isZoomedOut ? 24 : 15} style={iconStyle} />}
      {setupTeardownType === "teardown" && (
        <FiArrowDownRight size={isZoomedOut ? 24 : 15} style={iconStyle} />
      )}
    </Box>
  );
};
