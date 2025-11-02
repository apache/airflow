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
import { Box, Text, HStack, Link } from "@chakra-ui/react";
import { FiDatabase } from "react-icons/fi";
import { PiRectangleDashed } from "react-icons/pi";
import { Link as RouterLink } from "react-router-dom";

import Time from "../Time";
import type { AssetSummary, NextRunEvent } from "./types";

export const AssetNode = ({
  asset,
  event,
}: {
  readonly asset: AssetSummary;
  readonly event?: NextRunEvent;
}) => (
  <Box
    bg="bg.muted"
    border="1px solid"
    borderColor={Boolean(event?.lastUpdate) ? "success.solid" : "border.emphasized"}
    borderRadius="md"
    borderWidth={Boolean(event?.lastUpdate) ? 3 : 1}
    display="inline-block"
    minW="fit-content"
    p={2}
    position="relative"
  >
    <HStack gap={2}>
      {"asset" in asset ? <FiDatabase /> : <PiRectangleDashed />}
      {"alias" in asset ? (
        <Text fontSize="sm">{asset.alias.name}</Text>
      ) : (
        <Link asChild color="fg.info" display="block" py={2}>
          <RouterLink to={`/assets/${asset.asset.id}`}>{asset.asset.name}</RouterLink>
        </Link>
      )}
    </HStack>
    {event?.lastUpdate === undefined ? undefined : (
      <Text color="fg.muted" fontSize="sm">
        <Time datetime={event.lastUpdate} />
      </Text>
    )}
  </Box>
);
