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
import { Box, Heading, Flex, Skeleton } from "@chakra-ui/react";
import { BiTargetLock } from "react-icons/bi";

import { usePoolServiceGetPools } from "openapi/queries/queries";
import { PoolBar } from "src/pages/PoolBar";
import { useAutoRefresh } from "src/utils";
import type { Slots } from "src/utils/slots";

export const PoolSummary = () => {
  const refetchInterval = useAutoRefresh({});
  const { data, isLoading } = usePoolServiceGetPools(undefined, undefined, {
    refetchInterval,
  });

  const pools = data?.pools;
  const totalSlots = pools?.reduce((sum, pool) => sum + pool.slots, 0) ?? 0;

  const slotTotals: Slots = {
    deferred_slots: 0,
    occupied_slots: 0,
    open_slots: 0,
    queued_slots: 0,
    running_slots: 0,
    scheduled_slots: 0,
  };

  const poolsWithSlotType: Slots = {
    deferred_slots: 0,
    occupied_slots: 0,
    open_slots: 0,
    queued_slots: 0,
    running_slots: 0,
    scheduled_slots: 0,
  };

  pools?.forEach((pool) => {
    Object.keys(slotTotals).forEach((slotKey) => {
      const typedKey = slotKey as keyof Slots;
      const slotValue = pool[typedKey];

      if (slotValue > 0) {
        slotTotals[typedKey] += slotValue;
        poolsWithSlotType[typedKey] += 1;
      }
    });
  });

  return (
    <Box w="100%">
      <Flex color="fg.muted" mb={2} w="100%">
        <BiTargetLock />
        <Heading ml={1} size="xs">
          Pool Slots
        </Heading>
      </Flex>

      {isLoading ? (
        <Skeleton borderRadius="full" h={8} w="100%" />
      ) : (
        <Box>
          <Flex bg="white" borderRadius="full" overflow="hidden" w="100%">
            <PoolBar pool={slotTotals} poolsWithSlotType={poolsWithSlotType} totalSlots={totalSlots} />
          </Flex>
        </Box>
      )}
    </Box>
  );
};
