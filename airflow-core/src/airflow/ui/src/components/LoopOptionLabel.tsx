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
import { HStack, Icon, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdLoop } from "react-icons/md";

import { DurationCell } from "src/components/DurationCell";
import { StateBadge } from "src/components/StateBadge";

import type { LoopOption } from "src/queries/useLoopFilterOptions";

// One renderer for the collapsed pill and the open list, so what you picked reads the same as
// what you picked it from.
export const LoopOptionLabel = ({
  compact = false,
  hideLoopName = false,
  option,
}: {
  readonly compact?: boolean;
  // Set where the loop is already named alongside, so the rows read "All", "0", "1".
  readonly hideLoopName?: boolean;
  readonly option: LoopOption;
}) => {
  const { t: translate } = useTranslation(["common"]);
  const isWholeLoop = option.iteration === undefined;
  const named = isWholeLoop
    ? translate("common:filters.loopAllIterations", { loop: option.loopId })
    : translate("common:filters.loopIteration", { index: option.iteration, loop: option.loopId });
  const bare = isWholeLoop ? translate("common:filters.allIterations") : String(option.iteration);
  const label = hideLoopName ? bare : named;

  return (
    <HStack gap={2} justifyContent="space-between" width="full">
      <Text whiteSpace="nowrap">{label}</Text>
      <HStack gap={1}>
        {isWholeLoop ? (
          option.iterationsRan === undefined ? undefined : (
            <HStack color={option.state === "failed" ? "fg.error" : "fg.muted"} gap={1}>
              <Icon aria-hidden as={MdLoop} boxSize={3.5} />
              <Text fontSize="xs">
                {option.iterationsRan}/{option.maxIterations}
              </Text>
            </HStack>
          )
        ) : (
          <HStack color="fg.muted" fontSize="xs" gap={2}>
            <Text whiteSpace="nowrap">
              {translate("common:filters.iterationTaskCount", { count: option.taskCount ?? 0 })}
            </Text>
            {option.duration === undefined ? undefined : <DurationCell duration={option.duration} />}
          </HStack>
        )}
        {option.state === undefined ? undefined : (
          <StateBadge fontSize="xs" state={option.state}>
            {compact ? undefined : translate(`common:states.${option.state}`)}
          </StateBadge>
        )}
      </HStack>
    </HStack>
  );
};
