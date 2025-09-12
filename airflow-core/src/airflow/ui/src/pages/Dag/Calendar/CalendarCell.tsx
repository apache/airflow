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

import { CalendarTooltip } from "./CalendarTooltip";
import type { CalendarCellData, CalendarColorMode } from "./types";
import { useDelayedTooltip } from "./useDelayedTooltip";

type Props = {
  readonly backgroundColor: Record<string, string> | string;
  readonly cellData: CalendarCellData | null;
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
  const { handleMouseEnter, handleMouseLeave } = useDelayedTooltip();

  const computedMarginRight = marginRight ?? (index !== undefined && index % 7 === 6 ? "8px" : "0");

  const relevantCount =
    viewMode === "failed" ? (cellData?.counts.failed ?? 0) : (cellData?.counts.total ?? 0);
  const hasData = Boolean(cellData && relevantCount > 0);
  const hasTooltip = Boolean(cellData); // Show tooltip if we have cell data (even if no runs)

  return (
    <Box
      onMouseEnter={hasTooltip ? handleMouseEnter : undefined}
      onMouseLeave={hasTooltip ? handleMouseLeave : undefined}
      position="relative"
    >
      <Box
        _hover={hasData ? { transform: "scale(1.1)" } : {}}
        bg={backgroundColor}
        borderRadius="2px"
        cursor={hasData ? "pointer" : "default"}
        height="14px"
        marginRight={computedMarginRight}
        width="14px"
      />
      <CalendarTooltip cellData={cellData} viewMode={viewMode} />
    </Box>
  );
};
