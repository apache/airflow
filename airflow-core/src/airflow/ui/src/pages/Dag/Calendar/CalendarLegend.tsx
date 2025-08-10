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
import { Box, HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Tooltip } from "src/components/ui";

import { CALENDAR_STATE_COLORS } from "./constants";

type Props = {
  readonly vertical?: boolean;
};

export const CalendarLegend = ({ vertical = false }: Props) => {
  const { t: translate } = useTranslation("dag");

  return (
    <Box>
      <Box mb={4}>
        <Text color="gray.700" fontSize="sm" fontWeight="medium" mb={3} textAlign="center">
          {translate("calendar.legend.successRateSpectrum")}
        </Text>
        {vertical ? (
          <Box>
            <Text color="gray.600" fontSize="xs" mb={2} textAlign="center">
              {translate("common:states.success")}
            </Text>
            <Box borderRadius="full" boxShadow="sm" mx="auto" overflow="hidden" width="fit-content">
              <Tooltip content={translate("calendar.legend.tooltips.success100")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.success.pure} cursor="pointer" height="32px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate80")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.success.high} cursor="pointer" height="24px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate60")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.success.medium} cursor="pointer" height="24px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate40")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.mixed.moderate} cursor="pointer" height="24px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate20")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.mixed.poor} cursor="pointer" height="24px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.failed")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.failed.pure} cursor="pointer" height="32px" width="20px" />
              </Tooltip>
            </Box>
            <Text color="gray.600" fontSize="xs" mt={2} textAlign="center">
              {translate("common:states.failed")}
            </Text>
          </Box>
        ) : (
          <HStack gap={3} justify="center">
            <Text color="gray.600" fontSize="xs">
              {translate("common:states.success")}
            </Text>
            <HStack borderRadius="full" boxShadow="sm" gap={0} overflow="hidden">
              <Tooltip content={translate("calendar.legend.tooltips.success100")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.success.pure} cursor="pointer" height="20px" width="32px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate80")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.success.high} cursor="pointer" height="20px" width="24px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate60")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.success.medium} cursor="pointer" height="20px" width="24px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate40")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.mixed.moderate} cursor="pointer" height="20px" width="24px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate20")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.mixed.poor} cursor="pointer" height="20px" width="24px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.failed")} openDelay={300}>
                <Box bg={CALENDAR_STATE_COLORS.failed.pure} cursor="pointer" height="20px" width="32px" />
              </Tooltip>
            </HStack>
            <Text color="gray.600" fontSize="xs">
              {translate("common:states.failed")}
            </Text>
          </HStack>
        )}
      </Box>

      <Box>
        <Text color="gray.700" fontSize="sm" fontWeight="medium" mb={3} textAlign="center">
          {translate("common:state")}
        </Text>
        <HStack gap={4} justify="center" wrap="wrap">
          <HStack gap={2}>
            <Box
              bg={CALENDAR_STATE_COLORS.running.pure}
              borderRadius="full"
              boxShadow="sm"
              height="16px"
              width="16px"
            />
            <Text color="gray.600" fontSize="sm">
              {translate("common:states.running")}
            </Text>
          </HStack>
          <HStack gap={2}>
            <Box
              bg={CALENDAR_STATE_COLORS.planned.pure}
              borderRadius="full"
              boxShadow="sm"
              height="16px"
              width="16px"
            />
            <Text color="gray.600" fontSize="sm">
              {translate("common:states.planned")}
            </Text>
          </HStack>
          <HStack gap={2}>
            <Box
              bg={CALENDAR_STATE_COLORS.queued.pure}
              borderRadius="full"
              boxShadow="sm"
              height="16px"
              width="16px"
            />
            <Text color="gray.600" fontSize="sm">
              {translate("common:states.queued")}
            </Text>
          </HStack>
          <HStack gap={2}>
            <Box
              bg={CALENDAR_STATE_COLORS.empty}
              borderRadius="full"
              boxShadow="sm"
              height="16px"
              width="16px"
            />
            <Text color="gray.600" fontSize="sm">
              {translate("common:states.no_status")}
            </Text>
          </HStack>
        </HStack>
      </Box>
    </Box>
  );
};
