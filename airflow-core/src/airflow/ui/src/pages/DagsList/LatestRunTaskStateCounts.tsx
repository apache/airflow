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
import { HStack, Portal, Skeleton, Tooltip } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type {
  DAGLatestRunTaskInstanceStateCountsResponse,
  TaskInstanceState,
} from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { RouterLink } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { sortStateEntries } from "src/utils";

type Props = {
  readonly compact?: boolean;
  readonly dagId: string;
  readonly entry: DAGLatestRunTaskInstanceStateCountsResponse | undefined;
  readonly isLoading: boolean;
};

export const LatestRunTaskStateCounts = ({ compact = false, dagId, entry, isLoading }: Props) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const gap = compact ? 0.5 : 1;
  const fontSize = compact ? "xs" : "sm";

  if (isLoading) {
    // Badges are dynamic (only states present in the run), so the final count is
    // unknown while loading; three pills approximate a typical row without reflow.
    return (
      <HStack
        aria-label={translate("latestRunTaskStateCounts.loading")}
        data-testid={`latest-run-task-state-counts-loading-${dagId}`}
        gap={gap}
      >
        {[1, 2, 3].map((idx) => (
          <Skeleton borderRadius="full" height="22px" key={idx} width="44px" />
        ))}
      </HStack>
    );
  }

  if (entry === undefined) {
    return undefined;
  }

  const stateEntries = sortStateEntries(entry.state_counts);
  const countByState = new Map(stateEntries);

  const describeState = (state: string) =>
    translate("latestRunTaskStateCounts.tooltip", {
      formattedCount: `${countByState.get(state) ?? 0}`,
      state: translate(`common:states.${state}` as const),
    });

  // One shared tooltip root per card instead of one per badge, so a page of Dag
  // cards doesn't mount hundreds of tooltip state machines (same pattern as DagRunStateCounts).
  return (
    <Tooltip.Root>
      <HStack data-testid={`latest-run-task-state-counts-${dagId}`} gap={gap} wrap="wrap">
        {stateEntries.map(([state, count]) => {
          // Task instances without a state are keyed "no_status"; the task list
          // filters them with the "none" value and StateBadge renders them as null.
          const filterValue = state === "no_status" ? "none" : state;

          return (
            <Tooltip.Trigger asChild key={state} value={state}>
              <RouterLink
                aria-label={describeState(state)}
                data-testid={`latest-run-task-state-count-${state}-${dagId}`}
                to={`/dags/${dagId}/runs/${entry.run_id}?${SearchParamsKeys.TASK_STATE}=${filterValue}`}
              >
                <StateBadge
                  fontSize={fontSize}
                  state={state === "no_status" ? null : (state as TaskInstanceState)}
                >
                  {count}
                </StateBadge>
              </RouterLink>
            </Tooltip.Trigger>
          );
        })}
      </HStack>
      <Portal disabled>
        <Tooltip.Positioner>
          <Tooltip.Content>
            <Tooltip.Arrow>
              <Tooltip.ArrowTip />
            </Tooltip.Arrow>
            <Tooltip.Context>
              {({ triggerValue }) =>
                typeof triggerValue === "string" && countByState.has(triggerValue)
                  ? describeState(triggerValue)
                  : undefined
              }
            </Tooltip.Context>
          </Tooltip.Content>
        </Tooltip.Positioner>
      </Portal>
    </Tooltip.Root>
  );
};
