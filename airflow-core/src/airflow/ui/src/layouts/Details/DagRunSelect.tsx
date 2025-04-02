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
import { createListCollection, type SelectValueChangeDetails } from "@chakra-ui/react";
import { forwardRef, type RefObject } from "react";
import { useNavigate, useParams } from "react-router-dom";

import type { GridDAGRunwithTIs } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { Select } from "src/components/ui";
import { useGrid } from "src/queries/useGrid";

type DagRunSelected = {
  run: GridDAGRunwithTIs;
  value: string;
};

type DagRunSelectProps = {
  readonly limit: number;
};

export const DagRunSelect = forwardRef<HTMLDivElement, DagRunSelectProps>(({ limit }, ref) => {
  const { dagId = "", runId, taskId } = useParams();
  const navigate = useNavigate();

  const { data, isLoading } = useGrid(limit);

  const runOptions = createListCollection<DagRunSelected>({
    items: (data?.dag_runs ?? []).map((dr: GridDAGRunwithTIs) => ({
      run: dr,
      value: dr.dag_run_id,
    })),
  });

  const selectDagRun = ({ items }: SelectValueChangeDetails<DagRunSelected>) => {
    const run = items.length > 0 ? `/runs/${items[0]?.run.dag_run_id}` : "";

    navigate({
      pathname: `/dags/${dagId}${run}/${taskId === undefined ? "" : `tasks/${taskId}`}`,
    });
  };

  return (
    <Select.Root
      collection={runOptions}
      data-testid="dag-run-select"
      disabled={isLoading}
      onValueChange={selectDagRun}
      size="sm"
      value={runId === undefined ? [] : [runId]}
      width="250px"
    >
      <Select.Label fontSize="xs">Dag Run</Select.Label>
      <Select.Trigger clearable>
        <Select.ValueText placeholder="All Runs">
          {(items: Array<DagRunSelected>) => (
            <>
              <Time datetime={items[0]?.run.run_after} />
              <StateBadge ml={2} state={items[0]?.run.state} />
            </>
          )}
        </Select.ValueText>
      </Select.Trigger>
      <Select.Content portalRef={ref as RefObject<HTMLElement>} zIndex="popover">
        {runOptions.items.map((option) => (
          <Select.Item item={option} key={option.value}>
            <Time datetime={option.run.run_after} />
            <StateBadge ml={2} state={option.run.state} />
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
});
