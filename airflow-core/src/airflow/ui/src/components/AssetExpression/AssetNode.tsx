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
import { FiDatabase } from "react-icons/fi";
import { PiRectangleDashed } from "react-icons/pi";

import type { NextRunAssetEventResponse } from "openapi/requests/types.gen";
import { RollupKeyChecklistPopover } from "src/components/RollupKeyChecklist";
import { RouterLink } from "src/components/ui";

import Time from "../Time";
import type { AssetSummary } from "./types";

export const AssetNode = ({
  asset,
  event,
}: {
  readonly asset: AssetSummary;
  readonly event?: NextRunAssetEventResponse;
}) => {
  const isFullyReceived = Boolean(event?.last_update);
  const isPartial =
    !isFullyReceived &&
    (event?.received_count ?? 0) > 0 &&
    (event?.received_count ?? 0) < (event?.required_count ?? 1);
  // In a partitioned Dag with a pending partition, `last_update` is the last
  // asset event, not the pending partition key's arrival — so suppress it for
  // non-rollup partitioned nodes. We detect that case via `required_keys`,
  // which only the partitioned + pending-APDR branch of `next_run_assets`
  // populates. Plain non-partitioned events leave `required_keys` empty and
  // must keep showing their timestamp.
  const isPartitionedNonRollup = event?.is_rollup === false && (event.required_keys?.length ?? 0) > 0;
  const showTime = isFullyReceived && !isPartitionedNonRollup;
  const showRollupChecklist =
    (event?.is_rollup ?? false) && (event?.required_keys?.length ?? 0) > 0 && (isPartial || isFullyReceived);

  return (
    <Box
      bg="bg.muted"
      border="1px solid"
      borderColor={isFullyReceived ? "success.solid" : isPartial ? "warning.solid" : "border.emphasized"}
      borderRadius="md"
      borderWidth={isFullyReceived || isPartial ? 3 : 1}
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
          <RouterLink display="block" py={2} to={`/assets/${asset.asset.id}`}>
            {asset.asset.name}
          </RouterLink>
        )}
      </HStack>
      {showRollupChecklist ? (
        <RollupKeyChecklistPopover
          receivedCount={event?.received_count ?? 0}
          receivedKeys={event?.received_keys ?? []}
          requiredCount={event?.required_count ?? 0}
          requiredKeys={event?.required_keys ?? []}
        />
      ) : showTime ? (
        <Text color="fg.muted" fontSize="sm">
          <Time datetime={event?.last_update ?? null} />
        </Text>
      ) : isPartial ? (
        <Text color="warning.fg" fontSize="sm">
          {event?.received_count} / {event?.required_count}
        </Text>
      ) : undefined}
    </Box>
  );
};
