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
import { Button, HStack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiDatabase, FiLock } from "react-icons/fi";

import { usePartitionedDagRunServiceGetPendingPartitionedDagRun } from "openapi/queries";
import type { NextRunAssetEventResponse, PartitionedDagRunAssetResponse } from "openapi/requests/types.gen";

import { Popover, RouterLink } from "src/system-components";

import { AssetExpression } from "src/components/AssetExpression";
import { RollupKeyChecklist } from "src/components/RollupKeyChecklist";

import { formatNumber } from "src/utils";

type Props = {
  readonly dagId: string;
  readonly partitionedDagRunId: number;
  readonly partitionKey: string;
  readonly totalReceived: number;
  readonly totalRequired: number;
};

export const AssetProgressCell = ({
  dagId,
  partitionedDagRunId,
  partitionKey,
  totalReceived,
  totalRequired,
}: Props) => {
  const { i18n, t: translate } = useTranslation();
  const { data, isLoading } = usePartitionedDagRunServiceGetPendingPartitionedDagRun({
    dagId,
    partitionedDagRunId,
    partitionKey,
  });

  const assetExpression = data?.asset_expression ?? undefined;
  const assets: Array<PartitionedDagRunAssetResponse> = data?.assets ?? [];

  const hasRollup = assets.some((ak) => ak.is_rollup);

  // An asset the caller may not read arrives with its id, name and uri blanked, so it has nothing
  // to match an expression leaf on -- and the leaf it would have matched is redacted to `id: null`
  // too. Dropping it here keeps the event list to the assets that can actually line up with one.
  const events: Array<NextRunAssetEventResponse> = assets.flatMap((ak: PartitionedDagRunAssetResponse) =>
    ak.received_count > 0 && ak.asset_id !== null && ak.asset_name !== null && ak.asset_uri !== null
      ? [
          {
            id: ak.asset_id,
            is_rollup: ak.is_rollup,
            last_update: ak.received ? "received" : null,
            name: ak.asset_name,
            received_count: ak.received_count,
            required_count: ak.required_count,
            uri: ak.asset_uri,
          },
        ]
      : [],
  );

  return (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount positioning={{ placement: "bottom-end" }} unmountOnExit>
      <Popover.Trigger asChild>
        <Button loading={isLoading} paddingInline={0} variant="ghost">
          <FiDatabase style={{ display: "inline" }} />
          {`${formatNumber(totalReceived, i18n.language)} / ${formatNumber(totalRequired, i18n.language)}`}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          {hasRollup ? (
            <HStack align="start" gap={4} maxH="300px" overflowX="auto" overflowY="auto">
              {assets
                .filter((ak) => ak.is_rollup)
                .map((ak, index) => (
                  // A hidden asset has no id to key on; the rows come back ordered by the asset's
                  // uri, so the position within one response is stable.
                  <VStack align="start" gap={1} key={ak.asset_id ?? `hidden-${index}`}>
                    {ak.hidden ? (
                      <HStack color="fg.muted" fontSize="xs" fontWeight="semibold" gap={1}>
                        <FiLock />
                        <Text>{translate("expression.hiddenAsset")}</Text>
                      </HStack>
                    ) : (
                      <RouterLink fontSize="xs" fontWeight="semibold" to={`/assets/${ak.asset_id}`}>
                        {ak.asset_name}
                      </RouterLink>
                    )}
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
