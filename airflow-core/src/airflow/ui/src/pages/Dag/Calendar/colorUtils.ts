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
import { useToken } from "@chakra-ui/react";

import type { RunCounts } from "./types";

/**
 * Hook to resolve calendar colors from semantic tokens
 * @param counts - The run counts for a calendar cell
 * @param viewMode - The calendar view mode (total or failed)
 * @returns The resolved color value for the calendar cell
 */
export const useCalendarColor = (counts: RunCounts, viewMode: "failed" | "total" = "total") => {
  // Resolve all calendar semantic tokens
  const [
    emptyColor,
    plannedColor,
    totalRunsLevel1,
    totalRunsLevel2,
    totalRunsLevel3,
    totalRunsLevel4,
    failedRunsLevel1,
    failedRunsLevel2,
    failedRunsLevel3,
    failedRunsLevel4,
  ] = useToken("colors", [
    "calendar.empty",
    "calendar.planned",
    "calendar.totalRuns.level1",
    "calendar.totalRuns.level2",
    "calendar.totalRuns.level3",
    "calendar.totalRuns.level4",
    "calendar.failedRuns.level1",
    "calendar.failedRuns.level2",
    "calendar.failedRuns.level3",
    "calendar.failedRuns.level4",
  ]) as Array<string | undefined>;

  // Provide fallback colors if tokens are not resolved
  const safeEmptyColor = emptyColor ?? "transparent";
  const safePlannedColor = plannedColor ?? "gray.400";
  const safeTotalRunsLevel1 = totalRunsLevel1 ?? "blue.200";
  const safeTotalRunsLevel2 = totalRunsLevel2 ?? "blue.400";
  const safeTotalRunsLevel3 = totalRunsLevel3 ?? "blue.600";
  const safeTotalRunsLevel4 = totalRunsLevel4 ?? "blue.800";
  const safeFailedRunsLevel1 = failedRunsLevel1 ?? "red.200";
  const safeFailedRunsLevel2 = failedRunsLevel2 ?? "red.400";
  const safeFailedRunsLevel3 = failedRunsLevel3 ?? "red.600";
  const safeFailedRunsLevel4 = failedRunsLevel4 ?? "red.800";

  // Handle planned tasks
  if (counts.planned > 0) {
    return safePlannedColor;
  }

  // Handle empty cells
  if (counts.total === 0) {
    return safeEmptyColor;
  }

  // Determine target count based on view mode
  const targetCount = viewMode === "total" ? counts.total : counts.failed;

  // Handle empty target count
  if (targetCount === 0) {
    return safeEmptyColor;
  }

  // Get the appropriate color levels
  const colorLevels = viewMode === "total" 
    ? [safeEmptyColor, safeTotalRunsLevel1, safeTotalRunsLevel2, safeTotalRunsLevel3, safeTotalRunsLevel4]
    : [safeEmptyColor, safeFailedRunsLevel1, safeFailedRunsLevel2, safeFailedRunsLevel3, safeFailedRunsLevel4];

  // Simple intensity mapping based on count ranges
  if (targetCount <= 5) {return colorLevels[1] ?? safeEmptyColor;}
  if (targetCount <= 15) {return colorLevels[2] ?? safeEmptyColor;}
  if (targetCount <= 25) {return colorLevels[3] ?? safeEmptyColor;}

  return colorLevels[4] ?? safeEmptyColor;
};
