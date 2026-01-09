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
import { Box, Flex, Text, VStack, Link, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink } from "react-router-dom";

import type { PoolResponse, TaskInstanceState } from "openapi/requests/types.gen";
import { StateIcon } from "src/components/StateIcon";
import { Tooltip } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { type Slots, slotConfigs } from "src/utils/slots";

export const PoolBar = ({
  pool,
  poolsWithSlotType,
  totalSlots,
}: {
  readonly pool: PoolResponse | Slots;
  readonly poolsWithSlotType?: Slots;
  readonly totalSlots: number;
}) => {
  const { t: translate } = useTranslation("common");

  const isDashboard = Boolean(poolsWithSlotType);
  const includeDeferredInBar = "include_deferred" in pool && pool.include_deferred;
  const barSlots = ["running", "queued", "open"];

  if (isDashboard || includeDeferredInBar) {
    barSlots.push("deferred");
  }
  const infoSlots = ["scheduled"];

  if (!isDashboard && !includeDeferredInBar) {
    infoSlots.push("deferred");
  }

  const preparedSlots = slotConfigs.map((config) => {
    const slotType = config.key.replace("_slots", "") as TaskInstanceState;

    return {
      ...config,
      label: translate(`common:states.${slotType}`),
      slotType,
      slotValue: (pool[config.key] as number | undefined) ?? 0,
    };
  });

  return (
    <VStack align="stretch" gap={1} w="100%">
      <Flex bg="bg.muted" borderRadius="md" h="20px" overflow="hidden" w="100%">
        {preparedSlots
          .filter((slot) => barSlots.includes(slot.slotType) && slot.slotValue > 0)
          .map((slot) => {
            const flexValue = slot.slotValue / totalSlots || 0;

            const poolContent = (
              <Tooltip content={slot.label} key={slot.key} showArrow={true}>
                <Flex
                  alignItems="center"
                  bg={`${slot.color}.solid`}
                  color={`${slot.color}.contrast`}
                  gap={1}
                  h="100%"
                  justifyContent="center"
                  overflow="hidden"
                  px={1}
                  w="100%"
                >
                  {slot.icon}
                  <Text fontSize="xs" fontWeight="bold" truncate>
                    {slot.slotValue}
                  </Text>
                </Flex>
              </Tooltip>
            );

            return slot.color !== "success" && "name" in pool ? (
              <Link asChild flex={flexValue} key={slot.key}>
                <RouterLink
                  to={`/task_instances?${SearchParamsKeys.STATE}=${slot.color}&${SearchParamsKeys.POOL}=${pool.name}`}
                >
                  {poolContent}
                </RouterLink>
              </Link>
            ) : (
              <Box flex={flexValue} key={slot.key}>
                {poolContent}
              </Box>
            );
          })}
      </Flex>

      <HStack gap={4} wrap="wrap">
        {preparedSlots
          .filter((slot) => infoSlots.includes(slot.slotType) && slot.slotValue > 0)
          .map((slot) => (
            <HStack gap={1} key={slot.key}>
              <StateIcon size={12} state={slot.slotType} />
              <Text color="fg.muted" fontSize="xs" fontWeight="medium">
                {slot.label}: {slot.slotValue}
              </Text>
            </HStack>
          ))}
      </HStack>
    </VStack>
  );
};
