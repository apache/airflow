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
import { Box, HStack, Text, VStack } from "@chakra-ui/react";
import { useMemo } from "react";
import type { RefObject } from "react";
import { useTranslation } from "react-i18next";

import type { CalendarCellData, CalendarColorMode } from "./types";

const SQUARE_SIZE = "12px";
const SQUARE_BORDER_RADIUS = "2px";

type Props = {
  readonly cellData: CalendarCellData | undefined;
  readonly triggerRef: RefObject<HTMLElement>;
  readonly viewMode?: CalendarColorMode;
};

const stateColorMap = {
  failed: "failed.solid",
  planned: "stone.solid",
  running: "running.solid",
  success: "success.solid",
};

export const CalendarTooltip = ({ cellData, triggerRef, viewMode = "total" }: Props) => {
  const { t: translate } = useTranslation(["dag", "common"]);

  const tooltipStyle = useMemo(() => {
    if (!triggerRef.current) {
      return { display: "none" };
    }

    const rect = triggerRef.current.getBoundingClientRect();

    return {
      backgroundColor: "var(--chakra-colors-bg-inverted)",
      borderRadius: "4px",
      color: "var(--chakra-colors-fg-inverted)",
      fontSize: "14px",
      left: `${rect.left + globalThis.scrollX + rect.width / 2}px`,
      minWidth: "200px",
      padding: "8px",
      position: "absolute" as const,
      top: `${rect.bottom + globalThis.scrollY + 8}px`,
      transform: "translateX(-50%)",
      whiteSpace: "nowrap" as const,
      zIndex: 1000,
    };
  }, [triggerRef]);

  const arrowStyle = useMemo(
    () => ({
      borderBottom: "4px solid var(--chakra-colors-bg-inverted)",
      borderLeft: "4px solid transparent",
      borderRight: "4px solid transparent",
      content: '""',
      height: 0,
      left: "50%",
      position: "absolute" as const,
      top: "-4px",
      transform: "translateX(-50%)",
      width: 0,
    }),
    [],
  );

  if (!cellData) {
    return undefined;
  }

  const { counts, date } = cellData;

  const relevantCount = viewMode === "failed" ? counts.failed : counts.total;
  const hasRuns = relevantCount > 0;

  // In failed mode, only show failed runs; in total mode, show all non-zero states
  const states = Object.entries(counts)
    .filter(([key, value]) => {
      if (key === "total") {
        return false;
      }
      if (value === 0) {
        return false;
      }
      if (viewMode === "failed") {
        return key === "failed";
      }

      return true;
    })
    .map(([state, count]) => ({
      color: stateColorMap[state as keyof typeof stateColorMap] || "gray.500",
      count,
      state: translate(`common:states.${state}`),
    }));

  return (
    <div style={tooltipStyle}>
      <div style={arrowStyle} />
      {hasRuns ? (
        <VStack align="start" gap={2}>
          <Text fontSize="sm" fontWeight="medium">
            {date}
          </Text>
          <VStack align="start" gap={1.5}>
            {states.map(({ color, count, state }) => (
              <HStack gap={3} key={state}>
                <Box
                  bg={color}
                  border="1px solid"
                  borderColor="border.emphasized"
                  borderRadius={SQUARE_BORDER_RADIUS}
                  height={SQUARE_SIZE}
                  width={SQUARE_SIZE}
                />
                <Text fontSize="xs">
                  {count} {state}
                </Text>
              </HStack>
            ))}
          </VStack>
        </VStack>
      ) : (
        <Text fontSize="sm">
          {/* To do: remove fallback translations */}
          {date}: {viewMode === "failed" ? translate("calendar.noFailedRuns") : translate("calendar.noRuns")}
        </Text>
      )}
    </div>
  );
};
