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
import { Box, HStack, Text, VisuallyHidden } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Tooltip } from "src/system-components";

import { useBackfillProgress } from "src/queries/useBackfillProgress";

type Props = {
  readonly backfillId: number;
  readonly isRunning: boolean;
};

export const BackfillProgressBar = ({ backfillId, isRunning }: Props) => {
  const { t: translate } = useTranslation("components");
  const progress = useBackfillProgress(backfillId, isRunning);

  if (progress === undefined || progress.total === 0) {
    return undefined;
  }

  const { failed, success, total } = progress;
  const remaining = Math.max(total - success - failed, 0);
  const summary = translate("backfill.progressSummary", { failed, remaining, success });

  return (
    <Tooltip content={summary}>
      <HStack gap={2} minWidth="120px">
        <HStack bg="bg.emphasized" borderRadius="full" flex="1" gap={0} height="6px" overflow="hidden">
          <Box bg="success.solid" height="full" width={`${(success / total) * 100}%`} />
          <Box bg="failed.solid" height="full" width={`${(failed / total) * 100}%`} />
        </HStack>
        <Text fontSize="sm" whiteSpace="nowrap">
          {success + failed}/{total}
        </Text>
        <VisuallyHidden>{summary}</VisuallyHidden>
      </HStack>
    </Tooltip>
  );
};
