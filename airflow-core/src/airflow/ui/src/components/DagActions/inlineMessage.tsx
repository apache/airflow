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

const PARTITION_PREVIEW_LIMIT = 10;

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

    const keys = backfills
      .map((backfill) => backfill.partition_key)
      .filter((key): key is string => key !== null);
    const preview = keys.slice(0, PARTITION_PREVIEW_LIMIT);
    const nullCount = backfills.length - keys.length;
    const remaining = totalEntries - nullCount - preview.length;

    return (
      <VStack alignItems="flex-start" gap={1}>
        <Text color="fg.success" fontSize="sm">
          {translate("backfill.partitionsAffected", { count: totalEntries })}
        </Text>
        {preview.map((key) => (
          <Text fontSize="sm" key={key} pl={2}>
            {key}
          </Text>
        ))}
        {remaining > 0 ? (
          <Text color="fg.muted" fontSize="sm" pl={2}>
            {translate("backfill.andOthers", { count: remaining })}
          </Text>
        ) : undefined}
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
