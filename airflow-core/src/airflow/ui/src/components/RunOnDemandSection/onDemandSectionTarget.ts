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
import type { TaskInstanceResponse, TaskInstanceState } from "openapi/requests/types.gen";

const ON_DEMAND_SECTION_OPERATOR_NAME = "OnDemandSectionOperator";

export type OnDemandSectionTarget = {
  readonly dagId: string;
  readonly dagRunId: string;
  readonly mapIndex: number;
  readonly note: string | null;
  readonly operator?: string | null;
  readonly operatorName?: string | null;
  readonly startDate: string | null;
  readonly state: TaskInstanceState | null;
  readonly taskDisplayName: string;
  readonly taskId: string;
};

export const isRunnableOnDemandSection = (target: OnDemandSectionTarget) =>
  target.state === "success" &&
  target.mapIndex === -1 &&
  (target.operatorName === ON_DEMAND_SECTION_OPERATOR_NAME ||
    target.operator === ON_DEMAND_SECTION_OPERATOR_NAME);

export const getOnDemandSectionTarget = (taskInstance: TaskInstanceResponse): OnDemandSectionTarget => ({
  dagId: taskInstance.dag_id,
  dagRunId: taskInstance.dag_run_id,
  mapIndex: taskInstance.map_index,
  note: taskInstance.note,
  operator: taskInstance.operator,
  operatorName: taskInstance.operator_name,
  startDate: taskInstance.start_date,
  state: taskInstance.state,
  taskDisplayName: taskInstance.task_display_name,
  taskId: taskInstance.task_id,
});
