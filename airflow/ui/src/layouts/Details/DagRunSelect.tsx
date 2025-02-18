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
import { useNavigate, useParams, useSearchParams } from "react-router-dom";

import { useGridServiceGridData } from "openapi/queries";
import type { GridDAGRunwithTIs } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";

type DagRunSelected = {
  run: GridDAGRunwithTIs;
  value: string;
};

export const DagRunSelect = forwardRef<HTMLDivElement>((_, ref) => {
  const { dagId = "", runId, taskId } = useParams();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();

  const offset = parseInt(searchParams.get("offset") ?? "0", 10);

  const { data, isLoading } = useGridServiceGridData(
    {
      dagId,
      limit: 25,
      offset,
      orderBy: "-run_after",
    },
    undefined,
  );

  const runOptions = createListCollection<DagRunSelected>({
    items: (data?.dag_runs ?? []).map((dr: GridDAGRunwithTIs) => ({
      run: dr,
      value: dr.dag_run_id,
    })),
  });

  const selectDagRun = ({ items }: SelectValueChangeDetails<DagRunSelected>) =>
    navigate({
      pathname: `/dags/${dagId}/runs/${items[0]?.run.dag_run_id}/${taskId === undefined ? "" : `tasks/${taskId}`}`,
      search: searchParams.toString(),
    });

  return (
    <Select.Root
      collection={runOptions}
      colorPalette="blue"
      data-testid="dag-run-select"
      disabled={isLoading}
      maxWidth="500px"
      onValueChange={selectDagRun}
      value={runId === undefined ? [] : [runId]}
      variant="subtle"
    >
      <Select.Trigger>
        <Select.ValueText placeholder="Run">
          {(items: Array<DagRunSelected>) => (
            <StateBadge state={items[0]?.run.state}>{items[0]?.value}</StateBadge>
          )}
        </Select.ValueText>
      </Select.Trigger>
      <Select.Content portalRef={ref as RefObject<HTMLElement>} zIndex="popover">
        {runOptions.items.map((option) => (
          <Select.Item item={option} key={option.value}>
            <StateBadge state={option.run.state}>{option.value}</StateBadge>
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
});
