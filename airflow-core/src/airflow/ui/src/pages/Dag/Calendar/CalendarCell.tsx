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
import type React from "react";

import { HoverTooltip } from "src/components/HoverTooltip";

import { CalendarTooltip } from "./CalendarTooltip";
import type { CalendarCellData, CalendarColorMode } from "./types";

type Props = {
  readonly backgroundColor:
    | Record<string, string>
    | string
    | {
        actual: string | { _dark: string; _light: string };
        planned: string | { _dark: string; _light: string };
      };
  readonly cellData: CalendarCellData | undefined;
  readonly index?: number;
  readonly marginRight?: string;
  readonly viewMode?: CalendarColorMode;
};

const renderTooltip =
  (cellData: CalendarCellData | undefined, viewMode: CalendarColorMode) =>
  (triggerRef: React.RefObject<HTMLElement>) => (
    <CalendarTooltip cellData={cellData} triggerRef={triggerRef} viewMode={viewMode} />
  );

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

  const isMixedState =
    typeof backgroundColor === "object" && "planned" in backgroundColor && "actual" in backgroundColor;

  const cellBox = isMixedState ? (
    <Box
      _hover={hasData ? { transform: "scale(1.1)" } : {}}
      borderRadius="2px"
      cursor={hasData ? "pointer" : "default"}
      height="14px"
      marginRight={computedMarginRight}
      overflow="hidden"
      position="relative"
      width="14px"
    >
      <Box
        bg={backgroundColor.planned}
        clipPath="polygon(0 100%, 100% 100%, 0 0)"
        height="100%"
        position="absolute"
        width="100%"
      />
      <Box
        bg={backgroundColor.actual}
        clipPath="polygon(100% 0, 100% 100%, 0 0)"
        height="100%"
        position="absolute"
        width="100%"
      />
    </Box>
  ) : (
    <Box
      _hover={hasData ? { transform: "scale(1.1)" } : {}}
      bg={backgroundColor}
      borderRadius="2px"
      cursor={hasData ? "pointer" : "default"}
      height="14px"
      marginRight={computedMarginRight}
      width="14px"
    />
  );

  if (!hasTooltip) {
    return cellBox;
  }

  return <HoverTooltip tooltip={renderTooltip(cellData, viewMode)}>{cellBox}</HoverTooltip>;
};
