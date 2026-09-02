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
 * Pass `enabled: false` where no view configures scoping, so none of these run at all.
 * Otherwise each query is gated on the route params it needs, so it stays disabled where
 * those params are absent. Passing no explicit `queryKey` is what keeps these on the same
 * cache entry the surrounding page already populated — `DetailsLayout` for the Dag, the Task
 * and TaskInstance pages for their own records — since the generated hook builds the key from
 * the params via the same `Use*KeyFn` the pages go through. `usePluginAppliesToContext.test.ts`
 * asserts that equality so a param change on either side cannot silently split the cache.
 * Task groups are skipped for the task query, since `groupId` is not a task_id and would 404.
 */
export const usePluginAppliesToContext = (enabled: boolean): AppliesToContext => {
  const { dagId = "", groupId, mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const parsedMapIndex = parseInt(mapIndex, 10);

  const { data: dag, isLoading: isDagLoading } = useDagServiceGetDag({ dagId }, undefined, {
    enabled: enabled && Boolean(dagId),
  });

  const { data: task, isLoading: isTaskLoading } = useTaskServiceGetTask({ dagId, taskId }, undefined, {
    enabled: enabled && Boolean(dagId) && Boolean(taskId) && groupId === undefined,
  });

  const { data: taskInstance, isLoading: isTaskInstanceLoading } =
    useTaskInstanceServiceGetMappedTaskInstance(
      { dagId, dagRunId: runId, mapIndex: parsedMapIndex, taskId },
      undefined,
      {
        enabled:
          enabled &&
          Boolean(dagId) &&
          Boolean(runId) &&
          Boolean(taskId) &&
          groupId === undefined &&
          !isNaN(parsedMapIndex),
      },
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
