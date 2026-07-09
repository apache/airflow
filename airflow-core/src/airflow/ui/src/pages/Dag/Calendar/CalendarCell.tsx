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
import { Box } from "@chakra-ui/react";
import { FiAlertTriangle, FiClock } from "react-icons/fi";

import { BasicTooltip } from "src/components/BasicTooltip";

import { CalendarTooltip } from "./CalendarTooltip";
import type { CalendarCellData, CalendarColorMode } from "./types";

type Props = {
  readonly backgroundColor:
    | Record<string, string>
    | string
    | {
        primary: string | { _dark: string; _light: string };
        secondary: string | { _dark: string; _light: string };
      };
  readonly cellData: CalendarCellData | undefined;
  readonly index?: number;
  readonly marginRight?: string;
  readonly viewMode?: CalendarColorMode;
};

export const CalendarCell = ({
  backgroundColor,
  cellData,
  index,
  marginRight,
  viewMode = "total",
}: Props) => {
  const computedMarginRight = marginRight ?? (index !== undefined && index % 7 === 6 ? "8px" : "0");

  const relevantCount =
    viewMode === "failed" ? (cellData?.counts.failed ?? 0) : (cellData?.counts.total ?? 0);
  const hasData = Boolean(cellData && relevantCount > 0);
  const hasTooltip = Boolean(cellData);

  // States present in this cell, computed with the same view-mode-aware logic the
  // tooltip uses (see CalendarTooltip). Exposed as a `data-states` attribute so e2e
  // tests can read run states from the DOM instead of hovering — the tooltip does
  // not open reliably under synthetic pointer events in headless Firefox.
  const runStates = cellData
    ? Object.entries(cellData.counts)
        .filter(
          ([key, value]) => key !== "total" && value > 0 && (viewMode === "failed" ? key === "failed" : true),
        )
        .map(([key]) => key)
    : [];

  const isMixedState =
    typeof backgroundColor === "object" && "secondary" in backgroundColor && "primary" in backgroundColor;

  const { deadlineCounts } = cellData ?? {};
  const hasMissedDeadline = (deadlineCounts?.missed ?? 0) > 0;
  const hasPendingDeadline = (deadlineCounts?.pending ?? 0) > 0;
  const hasDeadline = hasMissedDeadline || hasPendingDeadline;
  const DeadlineIcon = hasMissedDeadline ? FiAlertTriangle : FiClock;

  const deadlineIndicator = hasDeadline ? (
    <Box
      alignItems="center"
      data-testid="deadline-indicator"
      display="flex"
      fontSize="10px"
      height="100%"
      justifyContent="center"
      left="0"
      lineHeight={1}
      position="absolute"
      top="0"
      width="100%"
    >
      <DeadlineIcon />
    </Box>
  ) : undefined;

  const cellBox = isMixedState ? (
    <Box
      _hover={hasData ? { transform: "scale(1.1)" } : {}}
      borderRadius="2px"
      cursor={hasData ? "pointer" : "default"}
      data-has-data={hasData ? "true" : "false"}
      data-states={runStates.join(" ")}
      data-testid="calendar-cell"
      data-view-mode={viewMode}
      height="14px"
      marginRight={computedMarginRight}
      overflow="hidden"
      position="relative"
      width="14px"
    >
      <Box
        bg={backgroundColor.secondary}
        clipPath="polygon(0 100%, 100% 100%, 0 0)"
        height="100%"
        position="absolute"
        width="100%"
      />
      <Box
        bg={backgroundColor.primary}
        clipPath="polygon(100% 0, 100% 100%, 0 0)"
        height="100%"
        position="absolute"
        width="100%"
      />
      {deadlineIndicator}
    </Box>
  ) : (
    <Box
      _hover={hasData ? { transform: "scale(1.1)" } : {}}
      bg={backgroundColor}
      borderRadius="2px"
      cursor={hasData ? "pointer" : "default"}
      data-has-data={hasData ? "true" : "false"}
      data-states={runStates.join(" ")}
      data-testid="calendar-cell"
      data-view-mode={viewMode}
      height="14px"
      marginRight={computedMarginRight}
      position="relative"
      width="14px"
    >
      {deadlineIndicator}
    </Box>
  );

  if (!hasTooltip) {
    return cellBox;
  }

  return (
    <BasicTooltip content={<CalendarTooltip cellData={cellData} viewMode={viewMode} />}>
      {cellBox}
    </BasicTooltip>
  );
};
