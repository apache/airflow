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
import { HStack, Skeleton } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { TaskInstanceState } from "openapi/requests/types.gen";

import { StateBadge } from "src/components/StateBadge";

import { sortStateEntries } from "src/utils";

type Props = {
  readonly isLoading?: boolean;
  readonly stateCounts: Record<string, number> | undefined;
};

/**
 * The task-instance breakdown of a single run, as plain badges.
 *
 * Unlike `RecentTaskStateCounts` on the Dags list these do not link anywhere: the row sits inside
 * a card that is itself a link, and an anchor cannot contain another anchor. The label therefore
 * drops that component's "click to filter" wording.
 */
export const RunTaskStateBadges = ({ isLoading = false, stateCounts }: Props) => {
  const { t: translate } = useTranslation(["dag", "common"]);

  if (isLoading) {
    return (
      <HStack gap={1}>
        {[1, 2, 3].map((index) => (
          <Skeleton borderRadius="full" height="22px" key={index} width="44px" />
        ))}
      </HStack>
    );
  }

  const stateEntries = sortStateEntries(stateCounts);

  if (stateEntries.length === 0) {
    return undefined;
  }

  return (
    <HStack gap={1} wrap="wrap">
      {stateEntries.map(([state, count]) => {
        const describedState = translate("dag:overview.latestRun.taskState", {
          formattedCount: `${count}`,
          state: translate(`common:states.${state}` as const),
        });

        return (
          // Task instances without a state are keyed "no_status"; StateBadge renders them as null.
          <StateBadge
            aria-label={describedState}
            fontSize="xs"
            key={state}
            state={state === "no_status" ? null : (state as TaskInstanceState)}
            title={describedState}
          >
            {count}
          </StateBadge>
        );
      })}
    </HStack>
  );
};
