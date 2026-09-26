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
import { Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useDagRunServiceGetDagRuns } from "openapi/queries";

import { Accordion, Alert, RadioCardItem, RadioCardRoot } from "src/system-components";

export type PausedDagAction = "drain" | "keepPaused" | "unpause";

type PausedDagOptionsProps = {
  readonly dagId: string;
  readonly onChange: (action: PausedDagAction) => void;
  readonly value: PausedDagAction;
};

const PausedDagOptions = ({ dagId, onChange, value }: PausedDagOptionsProps) => {
  const { t: translate } = useTranslation("components");
  // Draining a paused Dag lets every unfinished run proceed, not only the one being created. The
  // cached count can predate runs that have since finished (e.g. the run of an earlier drain), so
  // it is refetched and only trusted once fetched for this form.
  const { data: unfinishedRuns, isFetchedAfterMount } = useDagRunServiceGetDagRuns(
    { dagId, limit: 1, state: ["queued", "running"] },
    undefined,
    { enabled: value === "drain", staleTime: 0 },
  );
  const unfinishedRunCount = isFetchedAfterMount ? (unfinishedRuns?.total_entries ?? 0) : 0;

  return (
    <VStack alignItems="stretch" gap={2}>
      <Accordion.Root collapsible data-testid="paused-dag-options" size="lg" variant="enclosed">
        <Accordion.Item value="pausedDag">
          <Accordion.ItemTrigger cursor="button">
            {translate("pausedDag.title")}
            {/* The section starts collapsed, so keep the choice that will be applied visible. */}
            <Text color="fg.muted" fontSize="sm" fontWeight="normal">
              {translate(`pausedDag.${value}`)}
            </Text>
          </Accordion.ItemTrigger>
          <Accordion.ItemContent>
            <RadioCardRoot
              onChange={(event) => onChange((event.target as HTMLInputElement).value as PausedDagAction)}
              size="sm"
              value={value}
            >
              <VStack align="stretch" gap={2}>
                <RadioCardItem
                  description={translate("pausedDag.unpauseDescription")}
                  indicatorPlacement="start"
                  label={translate("pausedDag.unpause")}
                  value="unpause"
                />
                <RadioCardItem
                  description={translate("pausedDag.drainDescription")}
                  indicatorPlacement="start"
                  label={translate("pausedDag.drain")}
                  value="drain"
                />
                <RadioCardItem
                  description={translate("pausedDag.keepPausedDescription")}
                  indicatorPlacement="start"
                  label={translate("pausedDag.keepPaused")}
                  value="keepPaused"
                />
              </VStack>
            </RadioCardRoot>
          </Accordion.ItemContent>
        </Accordion.Item>
      </Accordion.Root>
      {value === "drain" && unfinishedRunCount > 0 ? (
        <Alert status="warning">
          {translate("pausedDag.unfinishedRunsWillRun", { count: unfinishedRunCount })}
        </Alert>
      ) : undefined}
    </VStack>
  );
};

export default PausedDagOptions;
