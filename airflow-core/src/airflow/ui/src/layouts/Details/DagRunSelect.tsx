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
import { createListCollection, type SelectValueChangeDetails, Select } from "@chakra-ui/react";
import { forwardRef, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate, useParams } from "react-router-dom";

import type { GridRunsResponse } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { useGridRuns } from "src/queries/useGridRuns.ts";

type DagRunSelected = {
  run: GridRunsResponse;
  value: string;
};

type DagRunSelectProps = {
  readonly limit: number;
};

export const DagRunSelect = forwardRef<HTMLDivElement, DagRunSelectProps>(({ limit }, ref) => {
  const { dagId = "", runId, taskId } = useParams();
  const { t: translate } = useTranslation(["dag", "common"]);
  const navigate = useNavigate();

  const { data: gridRuns, isLoading } = useGridRuns({ limit });
  const runOptions = useMemo(
    () =>
      createListCollection({
        items: (gridRuns ?? []).map((dr: GridRunsResponse) => ({
          run: dr,
          value: dr.run_id,
        })),
      }),
    [gridRuns],
  );

  const selectDagRun = ({ items }: SelectValueChangeDetails<DagRunSelected>) => {
    const runPartialPath = items.length > 0 ? `/runs/${items[0]?.run.run_id}` : "";

    navigate({
      pathname: `/dags/${dagId}${runPartialPath}/${taskId === undefined ? "" : `tasks/${taskId}`}`,
    });
  };

  const selectedRun = (gridRuns ?? []).find((dr) => dr.run_id === runId);

  return (
    <Select.Root
      collection={runOptions}
      data-testid="dag-run-select"
      disabled={isLoading || !gridRuns}
      onValueChange={selectDagRun}
      ref={ref}
      size="sm"
      value={runId === undefined ? [] : [runId]}
      width="250px"
    >
      <Select.Label fontSize="xs">{translate("common:dagRun_one")}</Select.Label>
      <Select.Control>
        <Select.Trigger>
          <Select.ValueText placeholder={translate("dag:allRuns")}>
            {selectedRun ? (
              <>
                <Time datetime={selectedRun.run_after} />
                <StateBadge ml={2} state={selectedRun.state} />
              </>
            ) : undefined}
          </Select.ValueText>
        </Select.Trigger>
        <Select.IndicatorGroup>
          <Select.ClearTrigger />
          <Select.Indicator />
        </Select.IndicatorGroup>
      </Select.Control>
      <Select.Positioner>
        <Select.Content>
          {runOptions.items.map((option) => (
            <Select.Item item={option} key={option.value}>
              <Time datetime={option.run.run_after} />
              <StateBadge ml={2} state={option.run.state} />
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Positioner>
    </Select.Root>
  );
});
