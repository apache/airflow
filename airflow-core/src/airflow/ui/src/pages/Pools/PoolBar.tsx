/* eslint-disable perfectionist/sort-objects */

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
import { Box, Flex, HStack, Text, VStack } from "@chakra-ui/react";
import { FiXCircle } from "react-icons/fi";

import type { PoolResponse } from "openapi/requests/types.gen";
import { StateIcon } from "src/components/StateIcon";
import { Tooltip } from "src/components/ui";
import { capitalize } from "src/utils";

import DeletePoolButton from "./DeletePoolButton";
import EditPoolButton from "./EditPoolButton";

const slots = {
  open_slots: { color: "success", icon: <StateIcon color="white" state="success" /> },
  occupied_slots: { color: "up_for_retry", icon: <FiXCircle color="white" /> },
  running_slots: { color: "running", icon: <StateIcon color="white" state="running" /> },
  queued_slots: { color: "queued", icon: <StateIcon color="white" state="queued" /> },
  scheduled_slots: { color: "scheduled", icon: <StateIcon color="white" state="scheduled" /> },
  deferred_slots: { color: "deferred", icon: <StateIcon color="white" state="deferred" /> },
};

type PoolBarProps = {
  readonly pool: PoolResponse;
};

const PoolBar = ({ pool }: PoolBarProps) => (
  <Box borderColor="border.emphasized" borderRadius={8} borderWidth={1} mb={2} overflow="hidden">
    <Flex alignItems="center" bg="bg.muted" justifyContent="space-between" p={4}>
      <VStack align="start" flex="1">
        <HStack justifyContent="space-between" width="100%">
          <Text fontSize="lg" fontWeight="bold" whiteSpace="normal" wordBreak="break-word">
            {pool.name} ({pool.slots} slots)
            {pool.include_deferred ? (
              <Tooltip content="Deferred Slots Included">
                <StateIcon size={18} state="deferred" style={{ display: "inline", marginLeft: 6 }} />
              </Tooltip>
            ) : undefined}
          </Text>
          <HStack gap={0}>
            <EditPoolButton pool={pool} />
            {pool.name === "default_pool" ? undefined : <DeletePoolButton poolName={pool.name} />}
          </HStack>
        </HStack>
        {pool.description ?? (
          <Text color="gray.fg" fontSize="sm">
            {pool.description}
          </Text>
        )}
      </VStack>
    </Flex>

    <Box margin={4}>
      <Flex bg="gray.muted" borderRadius="md" h="20px" overflow="hidden" w="100%">
        {Object.entries(slots).map(([slotKey, { color, icon }]) => {
          const rawSlotValue = pool[slotKey as keyof PoolResponse];
          const slotValue = typeof rawSlotValue === "number" ? rawSlotValue : 0;
          const flexValue = slotValue / pool.slots || 0;

          if (flexValue === 0) {
            return undefined; // Skip rendering if no value for this slot
          }

          return (
            <Tooltip content={`${capitalize(slotKey.replace("_", " "))}: ${slotValue}`} key={slotKey}>
              <Flex
                alignItems="center"
                bg={`${color}.solid`}
                flex={flexValue}
                gap={0.5}
                h="100%"
                justifyContent="center"
                position="relative"
              >
                {icon}
                {slotValue}
              </Flex>
            </Tooltip>
          );
        })}
      </Flex>
    </Box>
  </Box>
);

export default PoolBar;
