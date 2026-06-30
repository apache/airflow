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
import { useParams } from "react-router-dom";

import {
  useDagServiceGetDag,
  useTaskInstanceServiceGetMappedTaskInstance,
  useTaskServiceGetTask,
} from "openapi/queries";
import type { AppliesToContext } from "src/utils/pluginAppliesTo";

/**
 * Resolve the records the current route provides, for evaluating plugin `applies_to`.
 *
 * Each query is gated on the route params it needs, so it stays disabled where those
 * params are absent and is a cache hit where the details page already fetched it — no
 * extra requests. Task groups are skipped for the task query, since `groupId` is not a
 * task_id and would 404.
 */
export const usePluginAppliesToContext = (): AppliesToContext => {
  const { dagId = "", groupId, mapIndex = "-1", runId = "", taskId = "" } = useParams();

  const { data: dag, isLoading: isDagLoading } = useDagServiceGetDag({ dagId }, undefined, {
    enabled: Boolean(dagId),
  });

  const { data: task, isLoading: isTaskLoading } = useTaskServiceGetTask({ dagId, taskId }, undefined, {
    enabled: Boolean(dagId) && Boolean(taskId) && groupId === undefined,
  });

  const { data: taskInstance, isLoading: isTaskInstanceLoading } =
    useTaskInstanceServiceGetMappedTaskInstance(
      { dagId, dagRunId: runId, mapIndex: parseInt(mapIndex, 10), taskId },
      undefined,
      { enabled: Boolean(dagId) && Boolean(runId) && Boolean(taskId) && groupId === undefined },
    );

  return {
    dag,
    // `isLoading` (not `isPending`) is deliberate: a disabled query reports
    // `isPending` forever, which would withhold scoped views indefinitely on
    // destinations that legitimately have no task or task instance.
    isLoading: isDagLoading || isTaskLoading || isTaskInstanceLoading,
    task,
    taskInstance,
  };
};
