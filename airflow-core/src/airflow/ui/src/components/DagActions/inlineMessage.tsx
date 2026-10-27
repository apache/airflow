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
import { Text, Skeleton, VStack } from "@chakra-ui/react";
import type { TFunction } from "i18next";

import type { DryRunBackfillResponse } from "openapi/requests/types.gen";

import { PartitionPreviewTable } from "./PartitionPreviewTable";

type InlineMessageOptions = {
  readonly backfills?: Array<DryRunBackfillResponse>;
  readonly isPartitioned?: boolean;
  readonly isPendingDryRun: boolean;
  readonly totalEntries: number;
  readonly translate: TFunction;
};

export const getInlineMessage = ({
  backfills = [],
  isPartitioned = false,
  isPendingDryRun,
  totalEntries,
  translate,
}: InlineMessageOptions) => {
  if (isPendingDryRun) {
    return <Skeleton height="20px" width="100px" />;
  }

  if (isPartitioned) {
    if (totalEntries === 0) {
      return (
        <Text color="fg.error" fontSize="sm" fontWeight="medium">
          {translate("backfill.partitionsNone")}
        </Text>
      );
    }

    const partitionRows = backfills.filter(
      (backfill): backfill is { partition_key: string } & DryRunBackfillResponse =>
        backfill.partition_key !== null,
    );

    return (
      <VStack alignItems="flex-start" gap={1}>
        <Text color="fg.success" fontSize="sm">
          {translate("backfill.partitionsAffected", { count: totalEntries })}
        </Text>
        <PartitionPreviewTable backfills={partitionRows} />
      </VStack>
    );
  }

  if (totalEntries === 0) {
    return (
      <Text color="fg.error" fontSize="sm" fontWeight="medium">
        {translate("backfill.affectedNone")}
      </Text>
    );
  }

  return (
    <Text color="fg.success" fontSize="sm">
      {translate("backfill.affected", { count: totalEntries })}
    </Text>
  );
};
