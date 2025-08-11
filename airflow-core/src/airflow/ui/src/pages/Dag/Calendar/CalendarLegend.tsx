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

type Props = {
  readonly vertical?: boolean;
};

const spectrumLegendData = [
  {
    color: "success.600",
    size: { height: "20px", width: "32px" },
    tooltipKey: "calendar.legend.tooltips.success100",
  },
  {
    color: "success.500",
    size: { height: "20px", width: "24px" },
    tooltipKey: "calendar.legend.tooltips.successRate80",
  },
  {
    color: "success.400",
    size: { height: "20px", width: "24px" },
    tooltipKey: "calendar.legend.tooltips.successRate60",
  },
  {
    color: "up_for_retry.500",
    size: { height: "20px", width: "24px" },
    tooltipKey: "calendar.legend.tooltips.successRate40",
  },
  {
    color: "upstream_failed.500",
    size: { height: "20px", width: "24px" },
    tooltipKey: "calendar.legend.tooltips.successRate20",
  },
  {
    color: "failed.600",
    size: { height: "20px", width: "32px" },
    tooltipKey: "calendar.legend.tooltips.failed",
  },
];

const stateLegendData = [
  { color: "blue.400", labelKey: "common:states.running" },
  { color: "scheduled.200", labelKey: "common:states.planned" },
  { color: "queued.600", labelKey: "common:states.queued" },
  { color: { _dark: "gray.400", _light: "gray.100" }, labelKey: "common:states.no_status" },
];

export const CalendarLegend = ({ vertical = false }: Props) => {
  const { t: translate } = useTranslation("dag");

  return (
    <Box>
      <Box mb={4}>
        <Text color="fg.muted" fontSize="sm" fontWeight="medium" mb={3} textAlign="center">
          {translate("calendar.legend.successRateSpectrum")}
        </Text>
        {vertical ? (
          <Box>
            <Text color="fg.muted" fontSize="xs" mb={2} textAlign="center">
              {translate("common:states.success")}
            </Text>
            <VStack
              borderRadius="full"
              boxShadow="sm"
              gap={0}
              mx="auto"
              overflow="hidden"
              width="fit-content"
            >
              {spectrumLegendData.map(({ color, size, tooltipKey }) => (
                <Tooltip content={translate(tooltipKey)} key={tooltipKey}>
                  <Box bg={color} cursor="pointer" height={size.width} width={size.height} />
                </Tooltip>
              ))}
            </VStack>
            <Text color="fg.muted" fontSize="xs" mt={2} textAlign="center">
              {translate("common:states.failed")}
            </Text>
          </Box>
        ) : (
          <HStack gap={3} justify="center">
            <Text color="fg.muted" fontSize="xs">
              {translate("common:states.success")}
            </Text>
            <HStack borderRadius="full" boxShadow="sm" gap={0} overflow="hidden">
              {spectrumLegendData.map(({ color, size, tooltipKey }) => (
                <Tooltip content={translate(tooltipKey)} key={tooltipKey}>
                  <Box bg={color} cursor="pointer" height={size.height} width={size.width} />
                </Tooltip>
              ))}
            </HStack>
            <Text color="fg.muted" fontSize="xs">
              {translate("common:states.failed")}
            </Text>
          </HStack>
        )}
      </Box>

      <Box>
        <Text color="fg.muted" fontSize="sm" fontWeight="medium" mb={3} textAlign="center">
          {translate("common:state")}
        </Text>
        <HStack gap={4} justify="center" wrap="wrap">
          {stateLegendData.map(({ color, labelKey }) => (
            <HStack gap={2} key={labelKey}>
              <Box bg={color} borderRadius="full" boxShadow="sm" height="16px" width="16px" />
              <Text color="fg.muted" fontSize="sm">
                {translate(labelKey)}
              </Text>
            </HStack>
          ))}
        </HStack>
      </Box>
    </Box>
  );
};
