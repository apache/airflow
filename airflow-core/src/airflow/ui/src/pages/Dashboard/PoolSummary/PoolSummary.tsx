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
import { Box, Heading, Flex, Skeleton, Link } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { BiTargetLock } from "react-icons/bi";
import { Link as RouterLink } from "react-router-dom";

import { useAuthLinksServiceGetAuthMenus } from "openapi/queries";
import { usePoolServiceGetPools } from "openapi/queries/queries";
import { PoolBar } from "src/components/PoolBar";
import { useAutoRefresh } from "src/utils";
import { type Slots, slotKeys } from "src/utils/slots";

export const PoolSummary = () => {
  const { t: translate } = useTranslation("dashboard");
  const refetchInterval = useAutoRefresh({});
  const { data, isLoading } = usePoolServiceGetPools(undefined, undefined, {
    refetchInterval,
  });
  const { data: authLinks } = useAuthLinksServiceGetAuthMenus();
  const hasPoolsAccess = authLinks?.authorized_menu_items.includes("Pools");

  const pools = data?.pools;
  const totalSlots = pools?.reduce((sum, pool) => sum + pool.slots, 0) ?? 0;
  const aggregatePool: Slots = {
    deferred_slots: 0,
    open_slots: 0,
    queued_slots: 0,
    running_slots: 0,
    scheduled_slots: 0,
  };

  const poolsWithSlotType: Slots = {
    deferred_slots: 0,
    open_slots: 0,
    queued_slots: 0,
    running_slots: 0,
    scheduled_slots: 0,
  };

  pools?.forEach((pool) => {
    slotKeys.forEach((slotKey) => {
      const slotValue = pool[slotKey];

      if (slotValue > 0) {
        aggregatePool[slotKey] += slotValue;
        poolsWithSlotType[slotKey] += 1;
      }
    });
  });

  return (
    <Box w="100%">
      <Flex color="fg.muted" justifyContent="space-between" mb={2} w="100%">
        <Flex alignItems="center">
          <BiTargetLock />
          <Heading ml={1} size="xs">
            {translate("poolSlots")}
          </Heading>
        </Flex>
        {hasPoolsAccess ? (
          <Link asChild color="fg.info" fontSize="xs" h={4}>
            <RouterLink to="/pools">{translate("managePools")}</RouterLink>
          </Link>
        ) : undefined}
      </Flex>

      {isLoading ? (
        <Skeleton borderRadius="full" h={8} w="100%" />
      ) : (
        <Flex bg="bg" borderRadius="full" display="flex" overflow="hidden" w="100%">
          <PoolBar pool={aggregatePool} poolsWithSlotType={poolsWithSlotType} totalSlots={totalSlots} />
        </Flex>
      )}
    </Box>
  );
};
