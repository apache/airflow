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
import { CalendarTooltipContent } from "./CalendarTooltipContent";
import type { CalendarCellData } from "./types";
import { useDelayedTooltip } from "./useDelayedTooltip";

type Props = {
  readonly backgroundColor: Record<string, string> | string;
  readonly cellData?: CalendarCellData; // For rich tooltip content
  readonly index?: number;
  readonly marginRight?: string;
};

export const CalendarCell = ({ backgroundColor, cellData, index, marginRight }: Props) => {
  const { handleMouseEnter, handleMouseLeave } = useDelayedTooltip();

  const computedMarginRight = marginRight ?? (index !== undefined && index % 7 === 6 ? "8px" : "0");

  return (
    <Box onMouseEnter={handleMouseEnter} onMouseLeave={handleMouseLeave} position="relative">
      <Box
        _hover={{ transform: "scale(1.1)" }}
        bg={backgroundColor}
        border="1px solid"
        borderColor="border.emphasized"
        borderRadius="2px"
        cursor="pointer"
        height="14px"
        marginRight={computedMarginRight}
        width="14px"
      />
      <CalendarTooltip content={cellData ? <CalendarTooltipContent cellData={cellData} /> : ""} />
    </Box>
  );
};
