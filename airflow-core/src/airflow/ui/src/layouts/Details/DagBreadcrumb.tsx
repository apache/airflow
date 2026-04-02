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
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import {
  useDagRunServiceGetDagRun,
  useDagServiceGetDagDetails,
  useTaskInstanceServiceGetMappedTaskInstance,
  useTaskServiceGetTask,
} from "openapi/queries";
import { BreadcrumbStats } from "src/components/BreadcrumbStats";
import { StateBadge } from "src/components/StateBadge";
import { TogglePause } from "src/components/TogglePause";
import { isStatePending, useAutoRefresh } from "src/utils";

export const DagBreadcrumb = () => {
  const { t: translate } = useTranslation();
  const { dagId = "", groupId, mapIndex = "-1", runId, taskId } = useParams();
  const refetchInterval = useAutoRefresh({ dagId });
  const parsedMapIndex = parseInt(mapIndex, 10);

  const { data: dag } = useDagServiceGetDagDetails({
    dagId,
  });

  const { data: dagRun } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId ?? "",
    },
    undefined,
    {
      enabled: Boolean(runId),
      refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false),
    },
  );

  const { data: task } = useTaskServiceGetTask({ dagId, taskId }, undefined, { enabled: Boolean(taskId) });

  const { data: mappedTaskInstance } = useTaskInstanceServiceGetMappedTaskInstance(
    { dagId, dagRunId: runId ?? "", mapIndex: parsedMapIndex, taskId: taskId ?? "" },
    undefined,
    { enabled: Boolean(runId) && Boolean(taskId) && mapIndex !== "-1" && !isNaN(parsedMapIndex) },
  );

  const links: Array<{ label: ReactNode | string; labelExtra?: ReactNode; title?: string; value?: string }> =
    [
      {
        label: dag?.dag_display_name ?? dagId,
        labelExtra: (
          <TogglePause
            dagDisplayName={dag?.dag_display_name}
            dagId={dagId}
            isPaused={Boolean(dag?.is_paused)}
            skipConfirm
          />
        ),
        title: translate("dag_one"),
        value: `/dags/${dagId}`,
      },
    ];

  // Add dag run breadcrumb
  if (runId !== undefined) {
    links.push({
      label: dagRun === undefined ? runId : dagRun.dag_run_id,
      labelExtra: dagRun === undefined ? undefined : <StateBadge fontSize="xs" state={dagRun.state} />,
      title: translate("dagRun_one"),
      value: `/dags/${dagId}/runs/${runId}`,
    });
  }

  // Add group breadcrumb
  if (groupId !== undefined) {
    if (runId === undefined) {
      links.push({
        label: translate("allRuns", { ns: "dag" }),
        title: translate("dagRun_one"),
        value: `/dags/${dagId}/runs`,
      });
    }

    links.push({
      label: groupId,
      title: "Group",
      value: `/dags/${dagId}/groups/${groupId}`,
    });
  }

  // Add task breadcrumb
  if (runId !== undefined && taskId !== undefined) {
    if (task?.is_mapped) {
      links.push({
        label: `${task.task_display_name ?? taskId} [ ]`,
        title: translate("task_one"),
        value: `/dags/${dagId}/runs/${runId}/tasks/${taskId}/mapped`,
      });
    } else {
      links.push({
        label: task?.task_display_name ?? taskId,
        title: translate("task_one"),
      });
    }
  }

  if (runId === undefined && taskId !== undefined) {
    links.push({
      label: translate("allRuns", { ns: "dag" }),
      title: translate("dagRun_one"),
      value: `/dags/${dagId}/runs`,
    });
    links.push({
      label: task?.task_display_name ?? taskId,
      title: translate("task_one"),
    });
  }

  if (mapIndex !== "-1") {
    links.push({
      label: mappedTaskInstance?.rendered_map_index ?? mapIndex,
      title: translate("mapIndex"),
    });
  }

  return <BreadcrumbStats links={links} />;
};
