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
  useDagRunServiceGetDagRun,
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
 * those params are absent. Passing no explicit `queryKey` builds the key from the params via
 * the same `Use*KeyFn` the pages go through, so these share the surrounding page's cache entry
 * wherever that page passes the same params — `DetailsLayout` for the Dag, the Task and
 * TaskInstance pages for their own records. The task query adds the instance's version number,
 * so it shares with a page that asks for the same version and pays for its own fetch otherwise,
 * rather than going wrong.
 * `usePluginAppliesToContext.test.ts` asserts the equality that holds today, so a param change
 * on either side surfaces here rather than silently splitting the cache.
 * Task groups are skipped for the task query, since `groupId` is not a task_id and would 404.
 */
export const usePluginAppliesToContext = (enabled: boolean): AppliesToContext => {
  const { dagId = "", groupId, mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const parsedMapIndex = parseInt(mapIndex, 10);

  const { data: dag, isLoading: isDagLoading } = useDagServiceGetDag({ dagId }, undefined, {
    enabled: enabled && Boolean(dagId),
  });

  const { data: dagRun, isLoading: isDagRunLoading } = useDagRunServiceGetDagRun(
    { dagId, dagRunId: runId },
    undefined,
    { enabled: enabled && Boolean(dagId) && Boolean(runId) },
  );

  const hasTaskInstance =
    enabled &&
    Boolean(dagId) &&
    Boolean(runId) &&
    Boolean(taskId) &&
    groupId === undefined &&
    !isNaN(parsedMapIndex);

  const { data: taskInstance, isLoading: isTaskInstanceLoading } =
    useTaskInstanceServiceGetMappedTaskInstance(
      { dagId, dagRunId: runId, mapIndex: parsedMapIndex, taskId },
      undefined,
      { enabled: hasTaskInstance },
    );

  // Pinned to the version this instance ran, so `task.*` paths judge it by the definition it
  // used rather than the Dag's current one. Waiting for the instance costs a round trip on task
  // instance routes, but asking before then would fetch the latest task and have to discard it.
  const { data: task, isLoading: isTaskLoading } = useTaskServiceGetTask(
    { dagId, taskId, versionNumber: taskInstance?.dag_version?.version_number },
    undefined,
    {
      enabled:
        enabled &&
        Boolean(dagId) &&
        Boolean(taskId) &&
        groupId === undefined &&
        (!hasTaskInstance || taskInstance !== undefined),
    },
  );

  return {
    dag,
    dagRun,
    // `isLoading` (not `isPending`) is deliberate: a disabled query reports
    // `isPending` forever, which would withhold scoped views indefinitely on
    // destinations that legitimately have no run, task or task instance.
    isLoading: isDagLoading || isDagRunLoading || isTaskLoading || isTaskInstanceLoading,
    task,
    taskInstance,
  };
};
