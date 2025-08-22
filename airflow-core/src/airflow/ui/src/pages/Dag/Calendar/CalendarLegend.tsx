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

import type { CalendarColorMode } from "./types";

type Props = {
  readonly colorMode: CalendarColorMode;
  readonly vertical?: boolean;
};

const totalRunsLegendData = [
  { color: { _dark: "gray.700", _light: "gray.100" }, label: "0" },
  { color: { _dark: "green.300", _light: "green.200" }, label: "1-5" },
  { color: { _dark: "green.500", _light: "green.400" }, label: "6-15" },
  { color: { _dark: "green.700", _light: "green.600" }, label: "16-25" },
  { color: { _dark: "green.900", _light: "green.800" }, label: "26+" },
];

const failedRunsLegendData = [
  { color: { _dark: "gray.700", _light: "gray.100" }, label: "0" },
  { color: { _dark: "red.300", _light: "red.200" }, label: "1-2" },
  { color: { _dark: "red.500", _light: "red.400" }, label: "3-5" },
  { color: { _dark: "red.700", _light: "red.600" }, label: "6-10" },
  { color: { _dark: "red.900", _light: "red.800" }, label: "11+" },
];

export const CalendarLegend = ({ colorMode, vertical = false }: Props) => {
  const { t: translate } = useTranslation("dag");

  const legendData = colorMode === "total" ? totalRunsLegendData : failedRunsLegendData;
  const legendTitle =
    colorMode === "total" ? translate("calendar.totalRuns") : translate("overview.buttons.failedRun_other");

  return (
    <Box>
      <Box mb={4}>
        <Text color="fg.muted" fontSize="sm" fontWeight="medium" mb={3} textAlign="center">
          {legendTitle}
        </Text>
        {vertical ? (
          <VStack align="center" gap={2}>
            <Text color="fg.muted" fontSize="xs">
              {translate("calendar.legend.more")}
            </Text>
            <VStack gap={0.5}>
              {[...legendData].reverse().map(({ color, label }) => (
                <Tooltip content={`${label} ${colorMode === "total" ? "runs" : "failed"}`} key={label}>
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
              {legendData.map(({ color, label }) => (
                <Tooltip content={`${label} ${colorMode === "total" ? "runs" : "failed"}`} key={label}>
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
            <Box
              bg={{ _dark: "scheduled.600", _light: "scheduled.200" }}
              borderRadius="2px"
              boxShadow="sm"
              height="14px"
              width="14px"
            />
            <Text color="fg.muted" fontSize="xs">
              {translate("common:states.planned")}
            </Text>
          </HStack>
        </HStack>
      </Box>
    </Box>
  );
};
