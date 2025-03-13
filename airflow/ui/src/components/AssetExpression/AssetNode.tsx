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
import { Box, Text, VStack, HStack } from "@chakra-ui/react";
import { FiDatabase } from "react-icons/fi";

import type { QueuedEventResponse } from "openapi/requests/types.gen";

import Time from "../Time";
import type { AssetSummary } from "./types";

export const AssetNode = ({
  asset,
  event,
}: {
  readonly asset: AssetSummary["asset"];
  readonly event?: QueuedEventResponse;
}) => (
  <Box
    bg="bg.muted"
    border="1px solid"
    borderRadius="md"
    borderWidth={Boolean(event?.created_at) ? 3 : 1}
    display="inline-block"
    minW="fit-content"
    p={3}
    position="relative"
  >
    <VStack align="start" gap={2}>
      <HStack gap={2}>
        <FiDatabase />
        {/* TODO add events back in when asset_expression contains asset_id */}
        {/* {event?.id === undefined ? ( */}
        <Text fontSize="sm">{asset.uri}</Text>
        {/* ) : (
          <Link asChild color="fg.info" display="block" py={2}>
            <RouterLink to={`/assets/${event.id}`}>{asset.uri}</RouterLink>
          </Link>
        )} */}
      </HStack>
      <Time datetime={event?.created_at} />
    </VStack>
  </Box>
);
