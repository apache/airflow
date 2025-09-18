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
// airflow/ui/src/components/PoolBar.tsx
import { Flex, Link, Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink } from "react-router-dom";

import type { PoolResponse } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { type Slots, slotConfigs } from "src/utils/slots";

export const PoolBar = ({
  pool,
  poolsWithSlotType,
  totalSlots,
}: {
  readonly pool: PoolResponse | Slots;
  readonly poolsWithSlotType?: Slots;
  readonly totalSlots: number;
}) => {
  const { t: translate } = useTranslation("common");

  // Build segments first so the last one can absorb rounding
  const segments = slotConfigs
    .map(({ color, icon, key }) => {
      const slotValue = pool[key];
      const flexValue = slotValue / totalSlots || 0;

      if (flexValue === 0) {
        return undefined;
      }

      // Hide 'deferred' when the pool is configured NOT to include deferred slots
      const isDeferredKey = key === "deferred_slots";

      if (isDeferredKey && "include_deferred" in pool && !pool.include_deferred) {
        return undefined;
      }

      const slotType = key.replace("_slots", "");
      const poolCount = poolsWithSlotType ? poolsWithSlotType[key] : 0;

      return { color, flexValue, icon, key, poolCount, slotType, slotValue };
    })
    .filter(Boolean) as Array<{
    color: string;
    flexValue: number;
    icon: React.ReactNode;
    key: keyof Slots;
    poolCount: number;
    slotType: string;
    slotValue: number;
  }>;

  // Seamless widths: last segment gets the remainder to hit 100%
  let usedFlex = 0;
  const corrected = segments.map((seg, idx) => {
    const isLast = idx === segments.length - 1;

    if (isLast) {
      const remaining = Math.max(1 - usedFlex, 0);

      return { ...seg, correctedFlex: remaining || seg.flexValue };
    }
    usedFlex += seg.flexValue;

    return { ...seg, correctedFlex: seg.flexValue };
  });

  return (
    <>
      {corrected.map(({ color, correctedFlex, icon, key, poolCount, slotType, slotValue }) => {
        const tooltipContent = `${translate(`pools.${slotType}`)}: ${slotValue} (${poolCount} ${translate("pools.pools", { count: poolCount })})`;

        const inner = (
          <Tooltip content={tooltipContent} key={String(key)}>
            <Flex
              alignItems="center"
              bg={`${color}.solid`}
              color={`${color}.contrast`}
              // No flex here — wrapper owns the width
              gap={1}
              h="100%"
              justifyContent="center"
              minW={0}
              px={1}
              textAlign="center"
              w="100%"
            >
              {icon}
              {slotValue}
            </Flex>
          </Tooltip>
        );

        // Wrapper owns the proportional width, so segments butt together with no gaps
        return color !== "success" && "name" in pool ? (
          <Link asChild display="flex" flex={correctedFlex} key={String(key)}>
            <RouterLink
              to={`/task_instances?${SearchParamsKeys.STATE}=${color}&${SearchParamsKeys.POOL}=${pool.name}`}
            >
              {inner}
            </RouterLink>
          </Link>
        ) : (
          <Box display="flex" flex={correctedFlex} key={String(key)}>
            {inner}
          </Box>
        );
      })}
    </>
  );
};
