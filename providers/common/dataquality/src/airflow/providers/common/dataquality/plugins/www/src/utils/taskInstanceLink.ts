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

interface TaskInstanceRef {
  dag_id: string;
  map_index?: number;
  run_id: string;
  task_id: string;
}

/** Link back to a run's own task-instance page, matching the host UI's route shape. */
export function buildTaskInstanceHref(run: TaskInstanceRef): string {
  const base = `/dags/${encodeURIComponent(run.dag_id)}/runs/${encodeURIComponent(
    run.run_id,
  )}/tasks/${encodeURIComponent(run.task_id)}`;

  return run.map_index !== undefined && run.map_index >= 0 ? `${base}/mapped/${run.map_index}` : base;
}
