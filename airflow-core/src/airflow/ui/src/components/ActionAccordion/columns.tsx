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
import type { TFunction } from "i18next";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import type { MetaColumn } from "src/components/DataTable/types";
import { StateBadge } from "src/components/StateBadge";

export const getColumns = (translate: TFunction): Array<MetaColumn<TaskInstanceResponse>> => [
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
    accessorKey: "run_id",
    header: translate("runId"),
  },
];
