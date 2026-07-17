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
import type { Table } from "@tanstack/react-table";
import type { TFunction } from "i18next";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import type { MetaColumn } from "src/components/DataTable/types";
import { StateBadge } from "src/components/StateBadge";
import { Checkbox } from "src/components/ui/Checkbox";

// Stable per-row key; dag_run_id keeps the same task distinct across runs (past/future
// expansion), and map_index disambiguates the mapped instances of one task.
export const taskInstanceKey = (ti: TaskInstanceResponse): string =>
  `${ti.dag_run_id}:${ti.task_id}:${ti.map_index}`;

// When provided, rows render a leading checkbox so the user can exclude individual
// task instances from the action. Excluded keys are tracked by the caller.
export type RowSelection = {
  excludedKeys: Set<string>;
  onToggle: (key: string, included: boolean) => void;
};

// Header "select all" checkbox that toggles every task instance in its table at once.
const renderSelectAllHeader = (selection: RowSelection, table: Table<TaskInstanceResponse>) => {
  const keys = table.getRowModel().rows.map((row) => taskInstanceKey(row.original));
  const excludedCount = keys.filter((key) => selection.excludedKeys.has(key)).length;
  const allExcluded = keys.length > 0 && excludedCount === keys.length;
  const someExcluded = excludedCount > 0 && excludedCount < keys.length;

  return (
    <Checkbox
      borderWidth={1}
      checked={someExcluded ? "indeterminate" : !allExcluded}
      onCheckedChange={(event) => {
        const included = Boolean(event.checked);

        keys.forEach((key) => selection.onToggle(key, included));
      }}
    />
  );
};

export const getColumns = (
  translate: TFunction,
  selection?: RowSelection,
): Array<MetaColumn<TaskInstanceResponse>> => [
  ...(selection
    ? [
        {
          accessorKey: "select",
          cell: ({ row: { original } }: { row: { original: TaskInstanceResponse } }) => {
            const key = taskInstanceKey(original);

            return (
              <Checkbox
                borderWidth={1}
                checked={!selection.excludedKeys.has(key)}
                onCheckedChange={(event) => selection.onToggle(key, Boolean(event.checked))}
              />
            );
          },
          enableSorting: false,
          header: ({ table }: { table: Table<TaskInstanceResponse> }) =>
            renderSelectAllHeader(selection, table),
          meta: { skeletonWidth: 10 },
        } satisfies MetaColumn<TaskInstanceResponse>,
      ]
    : []),
  {
    accessorKey: "task_id",
    header: translate("taskId"),
    size: 200,
  },
  {
    accessorKey: "state",
    cell: ({
      row: {
        original: { state },
      },
    }) => (
      <StateBadge state={state}>
        {state ? translate(`common:states.${state}`) : translate("common:states.no_status")}
      </StateBadge>
    ),
    header: translate("state"),
  },
  {
    accessorKey: "map_index",
    header: translate("mapIndex"),
  },
  {
    accessorKey: "dag_run_id",
    header: translate("runId"),
  },
];
