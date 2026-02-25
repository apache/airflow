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
import { Button } from "@chakra-ui/react";
import { FiDatabase } from "react-icons/fi";

import { usePartitionedDagRunServiceGetPendingPartitionedDagRun } from "openapi/queries";
import type { PartitionedDagRunAssetResponse } from "openapi/requests/types.gen";
import { AssetExpression, type ExpressionType } from "src/components/AssetExpression";
import type { NextRunEvent } from "src/components/AssetExpression/types";
import { Popover } from "src/components/ui";

type Props = {
  readonly dagId: string;
  readonly partitionKey: string;
  readonly totalReceived: number;
  readonly totalRequired: number;
};

export const AssetProgressCell = ({ dagId, partitionKey, totalReceived, totalRequired }: Props) => {
  const { data, isLoading } = usePartitionedDagRunServiceGetPendingPartitionedDagRun({ dagId, partitionKey });

  const assetExpression = data?.asset_expression as ExpressionType | undefined;
  const assets: Array<PartitionedDagRunAssetResponse> = data?.assets ?? [];

  const events: Array<NextRunEvent> = assets
    .filter((ak: PartitionedDagRunAssetResponse) => ak.received)
    .map((ak: PartitionedDagRunAssetResponse) => ({
      id: ak.asset_id,
      lastUpdate: "received",
      name: ak.asset_name,
      uri: ak.asset_uri,
    }));

  return (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount positioning={{ placement: "bottom-end" }} unmountOnExit>
      <Popover.Trigger asChild>
        <Button loading={isLoading} paddingInline={0} size="sm" variant="ghost">
          <FiDatabase style={{ display: "inline" }} />
          {`${String(totalReceived)} / ${String(totalRequired)}`}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          <AssetExpression events={events} expression={assetExpression} />
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
