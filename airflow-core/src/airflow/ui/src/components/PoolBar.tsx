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
import { Box, Flex, Text, VStack, Link } from "@chakra-ui/react";
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
  const activeSlots = ["running", "queued", "open"];

  if ("include_deferred" in pool && pool.include_deferred) {
    activeSlots.push("deferred");
  }

  return (
    <Tooltip
      content={
        <VStack align="start" gap={1} p={1}>
          {slotConfigs.map(({ key }) => {
            const slotValue = pool[key];
            const slotType = key.replace("_slots", "");

            if (slotValue === 0) {
              return undefined;
            }

            return (
              <Flex align="center" gap={2} key={key}>
                <StateIcon size={12} state={slotType as TaskInstanceState} />
                <Text fontSize="xs">
                  {translate(`common:state.${slotType}`)}: {slotValue}
                </Text>
              </Flex>
            );
          })}
          {"include_deferred" in pool && (
            <Text color="fg.muted" fontSize="xs" mt={1}>
              {translate("pools.includeDeferred")}: {pool.include_deferred ? "True" : "False"}
            </Text>
          )}
          {poolsWithSlotType ? (
            <Text color="fg.muted" fontSize="xs" mt={1}>
              {translate("pools.totalPools")}:{" "}
              {Object.values(poolsWithSlotType).reduce((total, count) => total + count, 0)}
            </Text>
          ) : undefined}
        </VStack>
      }
    >
      <Flex bg="bg.muted" borderRadius="md" h="100%" overflow="hidden" w="100%">
        {slotConfigs
          .filter((config) => {
            const slotType = config.key.replace("_slots", "");

            return activeSlots.includes(slotType);
          })
          .map(({ color, icon, key }) => {
            const slotValue = pool[key];
            const flexValue = totalSlots > 0 ? (slotValue / totalSlots) * 100 : 0;

            if (flexValue === 0) {
              return undefined;
            }

            const slotType = key.replace("_slots", "");

            const poolContent = (
              <Flex
                alignItems="center"
                bg={`${color}.solid`}
                color={`${color}.contrast`}
                gap={1}
                h="100%"
                justifyContent="center"
                px={1}
                textAlign="center"
                title={`${slotType}: ${slotValue}`}
                w="100%"
              >
                {icon}
                {flexValue > 5 && (
                  <Text fontSize="xs" fontWeight="bold">
                    {slotValue}
                  </Text>
                )}
              </Flex>
            );

            return color !== "success" && "name" in pool ? (
              <Link asChild key={key} w={`${flexValue}%`}>
                <RouterLink
                  to={`/task_instances?${SearchParamsKeys.STATE}=${color}&${SearchParamsKeys.POOL}=${pool.name}`}
                >
                  {poolContent}
                </RouterLink>
              </Link>
            ) : (
              <Box key={key} w={`${flexValue}%`}>
                {poolContent}
              </Box>
            );
          })}
      </Flex>
    </Tooltip>
  );
};
