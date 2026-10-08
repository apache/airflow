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
import type { ColumnDef } from "@tanstack/react-table";

import type { DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

import type { DataTableFeatures } from "src/components/DataTable/features";

import type { RecentTasks } from "src/queries/useRecentTaskStateCounts";

import { RecentTaskStateCounts } from "./RecentTaskStateCounts";
import { RecentTaskStateCountsHeader } from "./RecentTaskStateCountsHeader";

/** The Dags table's "Recent tasks" column, or no column when the user turned it off. */
export const buildRecentTaskStateCountsColumns = (
  recentTasks: RecentTasks,
): Array<ColumnDef<DataTableFeatures, DAGWithLatestDagRunsResponse>> =>
  recentTasks.show
    ? [
        {
          accessorKey: "recent_task_state_counts",
          cell: ({ row: { original } }) => (
            <RecentTaskStateCounts
              compact
              dagId={original.dag_id}
              entry={recentTasks.entriesByDag[original.dag_id]}
              isLoading={recentTasks.isLoading}
            />
          ),
          enableSorting: false,
          header: () => <RecentTaskStateCountsHeader />,
        },
      ]
    : [];
