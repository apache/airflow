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
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiClock } from "react-icons/fi";

import { Tooltip } from "src/components/ui";

import { LegendIcon } from "./LegendIcon";
import { PLANNED_COLOR } from "./calendarUtils";
import type { CalendarScale, CalendarColorMode } from "./types";

type Props = {
  readonly hasDeadlines?: boolean;
  readonly scale: CalendarScale;
  readonly vertical?: boolean;
  readonly viewMode: CalendarColorMode;
};

export const CalendarLegend = ({ hasDeadlines = false, scale, vertical = false, viewMode }: Props) => {
  const { t: translate } = useTranslation("dag");

  const legendTitle =
    viewMode === "failed" ? translate("overview.buttons.failedRun_other") : translate("calendar.totalRuns");

  return (
    <Box>
      <Box mb={4}>
        <Text color="fg.muted" fontSize="sm" fontWeight="medium" mb={3} textAlign="center">
          {legendTitle}
        </Text>
        {scale.type === "empty" ? (
          <Text color="fg.muted" fontSize="xs" textAlign="center">
            {translate("calendar.noRuns")}
          </Text>
        ) : vertical ? (
          <VStack align="center" gap={2}>
            <Text color="fg.muted" fontSize="xs">
              {translate("calendar.legend.more")}
            </Text>
            <VStack gap={0.5}>
              {[...scale.legendItems].reverse().map(({ color, label }) => (
                <Tooltip content={`${label} ${viewMode === "failed" ? "failed" : "runs"}`} key={label}>
                  <Box>
                    <LegendIcon color={color} cursor="pointer" />
                  </Box>
                </Tooltip>
              ))}
            </VStack>
            <Text color="fg.muted" fontSize="xs">
              {translate("calendar.legend.less")}
            </Text>
          </VStack>
        ) : (
          <HStack align="center" gap={2} justify="center">
            <Text color="fg.muted" fontSize="xs">
              {translate("calendar.legend.less")}
            </Text>
            <HStack gap={0.5}>
              {scale.legendItems.map(({ color, label }) => (
                <Tooltip content={`${label} ${viewMode === "failed" ? "failed" : "runs"}`} key={label}>
                  <Box>
                    <LegendIcon color={color} cursor="pointer" />
                  </Box>
                </Tooltip>
              ))}
            </HStack>
            <Text color="fg.muted" fontSize="xs">
              {translate("calendar.legend.more")}
            </Text>
          </HStack>
        )}
      </Box>

      <Box>
        <HStack gap={4} justify="center" wrap="wrap">
          {viewMode === "total" && (
            <>
              <HStack gap={2}>
                <LegendIcon color={{ _dark: "green.700", _light: "green.400" }} />
                <Text color="fg.muted" fontSize="xs">
                  {translate("common:states.success")}
                </Text>
              </HStack>
              <HStack gap={2}>
                <LegendIcon color={{ _dark: "cyan.700", _light: "cyan.400" }} />
                <Text color="fg.muted" fontSize="xs">
                  {translate("common:states.running")}
                </Text>
              </HStack>
            </>
          )}

          <HStack gap={2}>
            <LegendIcon color={{ _dark: "red.700", _light: "red.400" }} />
            <Text color="fg.muted" fontSize="xs">
              {translate("common:states.failed")}
            </Text>
          </HStack>

          <HStack gap={2}>
            <Box bg={PLANNED_COLOR} borderRadius="2px" boxShadow="sm" height="14px" width="14px" />
            <Text color="fg.muted" fontSize="xs">
              {translate("common:states.planned")}
            </Text>
          </HStack>

          <HStack gap={2}>
            <LegendIcon
              color={{
                primary:
                  viewMode === "failed"
                    ? { _dark: "red.700", _light: "red.400" }
                    : { _dark: "green.700", _light: "green.400" },
                secondary: PLANNED_COLOR,
              }}
            />
            <Text color="fg.muted" fontSize="xs">
              {translate("dag:calendar.legend.mixed")}
            </Text>
          </HStack>
        </HStack>
      </Box>

      {Boolean(hasDeadlines) && (
        <Box mt={4}>
          <Text color="fg.muted" fontSize="sm" fontWeight="medium" mb={3} textAlign="center">
            {translate("overview.deadlines.title")}
          </Text>
          <HStack gap={4} justify="center" wrap="wrap">
            <HStack gap={2}>
              <Box fontSize="sm" lineHeight={1}>
                <FiClock />
              </Box>
              <Text color="fg.muted" fontSize="xs">
                {translate("deadlineStatus.upcoming")}
              </Text>
            </HStack>
            <HStack gap={2}>
              <Box fontSize="sm" lineHeight={1}>
                <FiAlertTriangle />
              </Box>
              <Text color="fg.muted" fontSize="xs">
                {translate("deadlineStatus.missed")}
              </Text>
            </HStack>
          </HStack>
        </Box>
      )}
    </Box>
  );
};
