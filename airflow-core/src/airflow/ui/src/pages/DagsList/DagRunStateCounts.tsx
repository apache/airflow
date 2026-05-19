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
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import type { DagRunState } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { RouterLink, Tooltip } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";

const DISPLAYED_STATES: ReadonlyArray<DagRunState> = ["success", "failed", "running", "queued"];

type Props = {
  readonly compact?: boolean;
  readonly counts: Record<string, number> | undefined;
  readonly dagId: string;
  readonly isLoading: boolean;
};

export const DagRunStateCounts = ({ compact = false, counts, dagId, isLoading }: Props) => {
  const { i18n, t: translate } = useTranslation(["dags", "common"]);
  const gap = compact ? 0.5 : 1;
  const fontSize = compact ? "xs" : "sm";
  // Compact for the badge ("100K"), exact for the tooltip ("100,000").
  const compactFormatter = useMemo(
    () => new Intl.NumberFormat(i18n.language, { maximumFractionDigits: 1, notation: "compact" }),
    [i18n.language],
  );
  const exactFormatter = useMemo(() => new Intl.NumberFormat(i18n.language), [i18n.language]);

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

  return (
    <HStack data-testid={`run-state-counts-${dagId}`} gap={gap}>
      {DISPLAYED_STATES.map((state) => {
        const count = counts[state] ?? 0;
        const translatedState = translate(`common:states.${state}` as const);
        const tooltipContent = translate("runStateCounts.tooltip", {
          formattedCount: exactFormatter.format(count),
          state: translatedState,
        });

        return (
          <Tooltip content={tooltipContent} key={state}>
            <RouterLink
              aria-label={tooltipContent}
              data-testid={`run-state-count-${state}-${dagId}`}
              to={`/dags/${dagId}/runs?${SearchParamsKeys.STATE}=${state}`}
            >
              <StateBadge fontSize={fontSize} opacity={count === 0 ? 0.4 : 1} state={state}>
                {compactFormatter.format(count)}
              </StateBadge>
            </RouterLink>
          </Tooltip>
        );
      })}
    </HStack>
  );
};
