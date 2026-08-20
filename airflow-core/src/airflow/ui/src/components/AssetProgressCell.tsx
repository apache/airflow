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
import { Button, HStack, VStack } from "@chakra-ui/react";
import { FiDatabase } from "react-icons/fi";

import { usePartitionedDagRunServiceGetPendingPartitionedDagRun } from "openapi/queries";
import type { NextRunAssetEventResponse, PartitionedDagRunAssetResponse } from "openapi/requests/types.gen";
import { AssetExpression } from "src/components/AssetExpression";
import { RollupKeyChecklist } from "src/components/RollupKeyChecklist";
import { Popover, RouterLink } from "src/components/ui";

type Props = {
  readonly dagId: string;
  readonly partitionKey: string;
  readonly totalReceived: number;
  readonly totalRequired: number;
};

export const AssetProgressCell = ({ dagId, partitionKey, totalReceived, totalRequired }: Props) => {
  const { data, isLoading } = usePartitionedDagRunServiceGetPendingPartitionedDagRun({ dagId, partitionKey });

  const assetExpression = data?.asset_expression ?? undefined;
  const assets: Array<PartitionedDagRunAssetResponse> = data?.assets ?? [];

  const hasRollup = assets.some((ak) => ak.is_rollup);

  const events: Array<NextRunAssetEventResponse> = assets
    .filter((ak: PartitionedDagRunAssetResponse) => ak.received_count > 0)
    .map((ak: PartitionedDagRunAssetResponse) => ({
      id: ak.asset_id,
      is_rollup: ak.is_rollup,
      last_update: ak.received ? "received" : null,
      name: ak.asset_name,
      received_count: ak.received_count,
      required_count: ak.required_count,
      uri: ak.asset_uri,
    }));

  return (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount positioning={{ placement: "bottom-end" }} unmountOnExit>
      <Popover.Trigger asChild>
        <Button loading={isLoading} paddingInline={0} variant="ghost">
          <FiDatabase style={{ display: "inline" }} />
          {`${String(totalReceived)} / ${String(totalRequired)}`}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          {hasRollup ? (
            <HStack align="start" gap={4} maxH="300px" overflowX="auto" overflowY="auto">
              {assets
                .filter((ak) => ak.is_rollup)
                .map((ak) => (
                  <VStack align="start" gap={1} key={ak.asset_id}>
                    <RouterLink fontSize="xs" fontWeight="semibold" to={`/assets/${ak.asset_id}`}>
                      {ak.asset_name}
                    </RouterLink>
                    <RollupKeyChecklist receivedKeys={ak.received_keys} requiredKeys={ak.required_keys} />
                  </VStack>
                ))}
            </HStack>
          ) : (
            <AssetExpression events={events} expression={assetExpression} />
          )}
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
