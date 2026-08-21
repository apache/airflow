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

import type { DagRunState } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { RouterLink } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";

const DISPLAYED_STATES: ReadonlyArray<DagRunState> = ["success", "failed", "running", "queued"];

type Props = {
  readonly compact?: boolean;
  readonly counts: Record<string, number> | undefined;
  readonly dagId: string;
  readonly isLoading: boolean;
  readonly stateCountLimit: number | undefined;
};

export const DagRunStateCounts = ({ compact = false, counts, dagId, isLoading, stateCountLimit }: Props) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const gap = compact ? 0.5 : 1;
  const fontSize = compact ? "xs" : "sm";

  if (isLoading || counts === undefined) {
    // Render skeletons sized to a typical badge so the row doesn't reflow when
    // counts arrive. Rendering zeros while loading would mislead the operator.
    return (
      <HStack
        aria-label={translate("runStateCounts.loading")}
        data-testid={`run-state-counts-loading-${dagId}`}
        gap={gap}
      >
        {DISPLAYED_STATES.map((state) => (
          <Skeleton borderRadius="full" height="22px" key={state} width="44px" />
        ))}
      </HStack>
    );
  }

  const describeState = (state: DagRunState) => {
    const count = counts[state] ?? 0;
    // A count that reached the API cap is only a lower bound; suffix it with "+".
    const isCapped = stateCountLimit !== undefined && count >= stateCountLimit;
    const formattedCount = `${count}${isCapped ? "+" : ""}`;

    return {
      formattedCount,
      isZero: count === 0,
      label: translate("runStateCounts.tooltip", {
        formattedCount,
        state: translate(`common:states.${state}` as const),
      }),
    };
  };

  // One shared tooltip root per card instead of one per badge, so a page of Dag
  // cards doesn't mount hundreds of tooltip state machines (same pattern as RecentRuns).
  return (
    <Tooltip.Root>
      <HStack data-testid={`run-state-counts-${dagId}`} gap={gap}>
        {DISPLAYED_STATES.map((state) => {
          const { formattedCount, isZero, label } = describeState(state);

          return (
            <Tooltip.Trigger asChild key={state} value={state}>
              <RouterLink
                aria-label={label}
                data-testid={`run-state-count-${state}-${dagId}`}
                to={`/dags/${dagId}/runs?${SearchParamsKeys.STATE}=${state}`}
              >
                <StateBadge fontSize={fontSize} opacity={isZero ? 0.4 : 1} state={state}>
                  {formattedCount}
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
              {({ triggerValue }) => {
                const state = DISPLAYED_STATES.find((displayed) => displayed === triggerValue);

                return state === undefined ? undefined : describeState(state).label;
              }}
            </Tooltip.Context>
          </Tooltip.Content>
        </Tooltip.Positioner>
      </Portal>
    </Tooltip.Root>
  );
};
