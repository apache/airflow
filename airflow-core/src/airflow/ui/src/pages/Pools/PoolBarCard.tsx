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
import { useTranslation } from "react-i18next";

import type { PoolResponse } from "openapi/requests/types.gen";
import { PoolBar } from "src/components/PoolBar";
import { StateIcon } from "src/components/StateIcon";
import { Tooltip } from "src/components/ui";

import DeletePoolButton from "./DeletePoolButton";
import EditPoolButton from "./EditPoolButton";

type PoolBarCardProps = {
  readonly pool: PoolResponse;
};

const PoolBarCard = ({ pool }: PoolBarCardProps) => {
  const { t: translate } = useTranslation("admin");

  return (
    <Box borderColor="border.emphasized" borderRadius={8} borderWidth={1} mb={2} overflow="hidden">
      <Flex alignItems="center" bg="bg.muted" justifyContent="space-between" p={4}>
        <VStack align="start" flex="1">
          <HStack justifyContent="space-between" width="100%">
            <Text fontSize="lg" fontWeight="bold" whiteSpace="normal" wordBreak="break-word">
              {pool.name} ({pool.slots} {translate("pools.form.slots")})
              {pool.include_deferred ? (
                <Tooltip content={translate("pools.deferredSlotsIncluded")}>
                  <StateIcon size={18} state="deferred" style={{ display: "inline", marginLeft: 6 }} />
                </Tooltip>
              ) : undefined}
            </Text>
            <HStack gap={0}>
              <EditPoolButton pool={pool} />
              {pool.name === "default_pool" ? undefined : (
                <DeletePoolButton poolName={pool.name} withText={false} />
              )}
            </HStack>
          </HStack>
          {pool.description ?? (
            <Text color="fg.muted" fontSize="sm">
              {pool.description}
            </Text>
          )}
        </VStack>
      </Flex>

      <Box margin={4}>
        <Flex bg="bg.muted" borderRadius="md" h="20px" overflow="hidden" w="100%">
          <PoolBar pool={pool} totalSlots={pool.slots} />
        </Flex>
      </Box>
    </Box>
  );
};

export default PoolBarCard;
