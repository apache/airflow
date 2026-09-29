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
import { Box, HStack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useBackfillServiceListBackfillsUi, useDagRunServiceGetDagRuns } from "openapi/queries";
import type { DagRunType } from "openapi/requests/types.gen";

import { RadioCardItem, RadioCardRoot } from "src/system-components";

import type { PausedDagAction } from "./types";

const PAUSED_DAG_OPTIONS = [
  { description: "pausedDag.unpauseDescription", label: "pausedDag.unpause", value: "unpause" },
  { description: "pausedDag.drainDescription", label: "pausedDag.drain", value: "drain" },
  { description: "pausedDag.keepPausedDescription", label: "pausedDag.keepPaused", value: "keepPaused" },
] as const satisfies Array<{ description: string; label: string; value: PausedDagAction }>;

const NON_BACKFILL_RUN_TYPES: Array<Exclude<DagRunType, "backfill">> = [
  "scheduled",
  "manual",
  "operator_triggered",
  "asset_triggered",
  "asset_materialization",
];

type PausedDagOptionsProps = {
  readonly dagId: string;
  readonly onChange: (action: PausedDagAction) => void;
  readonly value: PausedDagAction;
};

const PausedDagOptions = ({ dagId, onChange, value }: PausedDagOptionsProps) => {
  const { t: translate } = useTranslation("components");
  // Unpausing or draining lets every unfinished run proceed, not only the one being created. The cached
  // count can predate runs that have since finished (e.g. the run of an earlier drain), so it is
  // refetched and only trusted once fetched for this form.
  const { data: activeBackfills } = useBackfillServiceListBackfillsUi({ active: true, dagId }, undefined, {
    staleTime: 0,
  });
  // A paused backfill's runs do not start while it stays paused, whichever option is chosen.
  const isBackfillPaused = activeBackfills?.backfills.some((backfill) => backfill.is_paused) ?? false;
  const { data: unfinishedRuns, isFetchedAfterMount } = useDagRunServiceGetDagRuns(
    {
      dagId,
      limit: 1,
      runType: isBackfillPaused ? NON_BACKFILL_RUN_TYPES : undefined,
      state: ["queued", "running"],
    },
    undefined,
    { staleTime: 0 },
  );
  const unfinishedRunCount = isFetchedAfterMount ? (unfinishedRuns?.total_entries ?? 0) : 0;

  return (
    <Box data-testid="paused-dag-options">
      <Text fontSize="md" fontWeight="semibold" mb={3}>
        {translate("pausedDag.title")}
      </Text>
      <RadioCardRoot
        onValueChange={({ value: selected }) => {
          const option = PAUSED_DAG_OPTIONS.find((candidate) => candidate.value === selected);

          if (option !== undefined) {
            onChange(option.value);
          }
        }}
        size="sm"
        value={value}
      >
        <HStack align="stretch">
          {PAUSED_DAG_OPTIONS.map((option) => (
            <RadioCardItem
              description={
                <>
                  {translate(option.description)}
                  {option.value !== "keepPaused" && unfinishedRunCount > 0 ? (
                    <Text color="fg.warning" fontWeight="medium" mt={1}>
                      {translate("pausedDag.unfinishedRunsWillRun", { count: unfinishedRunCount })}
                    </Text>
                  ) : undefined}
                </>
              }
              indicatorPlacement="start"
              key={option.value}
              label={translate(option.label)}
              value={option.value}
            />
          ))}
        </HStack>
      </RadioCardRoot>
    </Box>
  );
};

export default PausedDagOptions;
