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

import { Tooltip } from "src/components/ui";

import { PLANNED_COLOR } from "./calendarUtils";
import type { CalendarScale, CalendarColorMode } from "./types";

type Props = {
  readonly scale: CalendarScale;
  readonly vertical?: boolean;
  readonly viewMode: CalendarColorMode;
};

export const CalendarLegend = ({ scale, vertical = false, viewMode }: Props) => {
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
                  <Box bg={color} borderRadius="2px" cursor="pointer" height="14px" width="14px" />
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
                  <Box bg={color} borderRadius="2px" cursor="pointer" height="14px" width="14px" />
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
          <HStack gap={2}>
            <Box bg={PLANNED_COLOR} borderRadius="2px" boxShadow="sm" height="14px" width="14px" />
            <Text color="fg.muted" fontSize="xs">
              {translate("common:states.planned")}
            </Text>
          </HStack>
          <HStack gap={2}>
            <Box
              borderRadius="2px"
              boxShadow="sm"
              height="14px"
              overflow="hidden"
              position="relative"
              width="14px"
            >
              <Box
                bg={PLANNED_COLOR}
                clipPath="polygon(0 100%, 100% 100%, 0 0)"
                height="100%"
                position="absolute"
                width="100%"
              />
              <Box
                bg={
                  viewMode === "failed"
                    ? { _dark: "red.700", _light: "red.400" }
                    : { _dark: "green.700", _light: "green.400" }
                }
                clipPath="polygon(100% 0, 100% 100%, 0 0)"
                height="100%"
                position="absolute"
                width="100%"
              />
            </Box>
            <Text color="fg.muted" fontSize="xs">
              {translate("calendar.legend.mixed")}
            </Text>
          </HStack>
        </HStack>
      </Box>
    </Box>
  );
};
