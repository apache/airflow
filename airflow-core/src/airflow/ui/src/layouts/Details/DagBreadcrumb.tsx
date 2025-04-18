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
import { useParams } from "react-router-dom";

import {
  useDagRunServiceGetDagRun,
  useDagServiceGetDagDetails,
  useTaskServiceGetTask,
} from "openapi/queries";
import { BreadcrumbStats } from "src/components/BreadcrumbStats";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TogglePause } from "src/components/TogglePause";
import { isStatePending, useAutoRefresh } from "src/utils";

export const DagBreadcrumb = () => {
  const { dagId = "", mapIndex = "-1", runId, taskId } = useParams();
  const refetchInterval = useAutoRefresh({ dagId });

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
        title: "Dag",
        value: `/dags/${dagId}`,
      },
    ];

  // Add dag run breadcrumb
  if (runId !== undefined) {
    links.push({
      label: dagRun === undefined ? runId : <Time datetime={dagRun.run_after} />,
      labelExtra: dagRun === undefined ? undefined : <StateBadge fontSize="xs" state={dagRun.state} />,
      title: "Dag Run",
      value: `/dags/${dagId}/runs/${runId}`,
    });
  }

  // Add task breadcrumb
  if (runId !== undefined && taskId !== undefined) {
    if (task?.is_mapped) {
      links.push({
        label: `${task.task_display_name ?? taskId} [ ]`,
        title: "Task",
        value: `/dags/${dagId}/runs/${runId}/tasks/${taskId}/mapped`,
      });
    } else {
      links.push({
        label: task?.task_display_name ?? taskId,
        title: "Task",
      });
    }
  }

  if (runId === undefined && taskId !== undefined) {
    links.push({ label: "All Runs", title: "Dag Run", value: `/dags/${dagId}/runs/` });
    links.push({ label: task?.task_display_name ?? taskId, title: "Task" });
  }

  if (mapIndex !== "-1") {
    links.push({ label: mapIndex, title: "Map Index" });
  }

  return <BreadcrumbStats links={links} />;
};
