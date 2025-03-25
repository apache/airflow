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
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

export const getTaskInstanceLink = (ti: TaskInstanceResponse) =>
  `/dags/${ti.dag_id}/runs/${ti.dag_run_id}/tasks/${ti.task_id}${ti.map_index >= 0 ? `/mapped/${ti.map_index}` : ""}`;

export const getTaskInstanceLinkFromObj = ({
  dagId,
  dagRunId,
  mapIndex = -1,
  taskId,
}: {
  dagId: string;
  dagRunId: string;
  mapIndex: number;
  taskId: string;
}) => `/dags/${dagId}/runs/${dagRunId}/tasks/${taskId}${mapIndex >= 0 ? `/mapped/${mapIndex}` : ""}`;
