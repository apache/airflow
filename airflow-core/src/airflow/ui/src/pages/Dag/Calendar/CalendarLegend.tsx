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

type Props = {
  readonly vertical?: boolean;
};

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
            <Box borderRadius="full" boxShadow="sm" mx="auto" overflow="hidden" width="fit-content">
              <Tooltip content={translate("calendar.legend.tooltips.success100")}>
                <Box bg="success.600" cursor="pointer" height="32px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate80")}>
                <Box bg="success.500" cursor="pointer" height="24px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate60")}>
                <Box bg="success.400" cursor="pointer" height="24px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate40")}>
                <Box bg="up_for_retry.500" cursor="pointer" height="24px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate20")}>
                <Box bg="upstream_failed.500" cursor="pointer" height="24px" width="20px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.failed")}>
                <Box bg="failed.600" cursor="pointer" height="32px" width="20px" />
              </Tooltip>
            </Box>
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
              <Tooltip content={translate("calendar.legend.tooltips.success100")}>
                <Box bg="success.600" cursor="pointer" height="20px" width="32px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate80")}>
                <Box bg="success.500" cursor="pointer" height="20px" width="24px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate60")}>
                <Box bg="success.400" cursor="pointer" height="20px" width="24px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate40")}>
                <Box bg="up_for_retry.500" cursor="pointer" height="20px" width="24px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.successRate20")}>
                <Box bg="upstream_failed.500" cursor="pointer" height="20px" width="24px" />
              </Tooltip>
              <Tooltip content={translate("calendar.legend.tooltips.failed")}>
                <Box bg="failed.600" cursor="pointer" height="20px" width="32px" />
              </Tooltip>
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
          <HStack gap={2}>
            <Box bg="blue.400" borderRadius="full" boxShadow="sm" height="16px" width="16px" />
            <Text color="fg.muted" fontSize="sm">
              {translate("common:states.running")}
            </Text>
          </HStack>
          <HStack gap={2}>
            <Box bg="scheduled.200" borderRadius="full" boxShadow="sm" height="16px" width="16px" />
            <Text color="fg.muted" fontSize="sm">
              {translate("common:states.planned")}
            </Text>
          </HStack>
          <HStack gap={2}>
            <Box bg="queued.600" borderRadius="full" boxShadow="sm" height="16px" width="16px" />
            <Text color="fg.muted" fontSize="sm">
              {translate("common:states.queued")}
            </Text>
          </HStack>
          <HStack gap={2}>
            <Box
              bg={{ _dark: "gray.400", _light: "gray.100" }}
              borderRadius="full"
              boxShadow="sm"
              height="16px"
              width="16px"
            />
            <Text color="fg.muted" fontSize="sm">
              {translate("common:states.no_status")}
            </Text>
          </HStack>
        </HStack>
      </Box>
    </Box>
  );
};
