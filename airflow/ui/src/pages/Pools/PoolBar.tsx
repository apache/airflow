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
import { Box, Flex, Text, VStack } from "@chakra-ui/react";
import { MdHourglassDisabled, MdHourglassFull } from "react-icons/md";

import type { PoolResponse } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";
import { stateColor } from "src/utils/stateColor";

type PoolBarProps = {
  readonly pools: Array<PoolResponse>;
};

const PoolBar = ({ pools }: PoolBarProps) => (
  <Flex direction="column" gap={4}>
    {pools.map((pool) => {
      // Calculate proportions
      const totalSlots = pool.slots;
      const openFlex = pool.open_slots / totalSlots || 0;
      const scheduledFlex = pool.scheduled_slots / totalSlots || 0;
      const runningFlex = pool.running_slots / totalSlots || 0;
      const queuedFlex = pool.queued_slots / totalSlots || 0;
      const occupiedFlex = pool.occupied_slots / totalSlots || 0;
      const deferredFlex = (pool.include_deferred ? pool.deferred_slots / totalSlots : 0) || 0;

      return (
        <Box
          borderColor="border.emphasized"
          borderRadius={8}
          borderWidth={1}
          key={pool.name}
          overflow="hidden"
        >
          <Flex alignItems="center" bg="bg.muted" justifyContent="space-between" p={4}>
            <VStack align="start">
              <Text fontSize="lg" fontWeight="bold">
                {pool.name}
              </Text>
              {Boolean(pool.description) ? (
                <Text color="gray.fg" fontSize="sm">
                  {pool.description}
                </Text>
              ) : undefined}
            </VStack>
            <Tooltip
              content={pool.include_deferred ? "Deferred Slots Included" : "Deferred Slots Not Included"}
            >
              {pool.include_deferred ? <MdHourglassFull size={25} /> : <MdHourglassDisabled size={25} />}
            </Tooltip>
          </Flex>

          <Box margin={4}>
            <Flex bg="gray.100" borderRadius="md" h="20px" overflow="hidden" w="100%">
              {/* Open Slots */}
              <Tooltip content={`Open Slots: ${pool.open_slots}`}>
                <Box bg={stateColor.success} flex={openFlex} h="100%" />
              </Tooltip>

              {/* Scheduled Slots */}
              <Tooltip content={`Scheduled Slots: ${pool.scheduled_slots}`}>
                <Box bg={stateColor.scheduled} flex={scheduledFlex} h="100%" />
              </Tooltip>

              {/* Running Slots */}
              <Tooltip content={`Running Slots: ${pool.running_slots}`}>
                <Box bg={stateColor.running} flex={runningFlex} h="100%" />
              </Tooltip>

              {/* Queued Slots */}
              <Tooltip content={`Queued Slots: ${pool.queued_slots}`}>
                <Box bg={stateColor.queued} flex={queuedFlex} h="100%" />
              </Tooltip>

              {/* Occupied Slots */}
              <Tooltip content={`Occupied Slots: ${pool.occupied_slots}`}>
                <Box bg={stateColor.up_for_retry} flex={occupiedFlex} h="100%" />
              </Tooltip>

              {/* Deferred Slots */}
              {pool.include_deferred && pool.deferred_slots > 0 ? (
                <Tooltip content={`Deferred Slots: ${pool.deferred_slots}`}>
                  <Box bg={stateColor.deferred} flex={deferredFlex} h="100%" />
                </Tooltip>
              ) : undefined}
            </Flex>
          </Box>
        </Box>
      );
    })}
  </Flex>
);

export default PoolBar;
