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
import { useTranslation } from "react-i18next";
import { useLocation, useParams } from "react-router-dom";

import {
  useDagRunServiceGetDagRun,
  useDagServiceGetDagDetails,
  useTaskInstanceServiceGetMappedTaskInstance,
  useTaskInstanceServiceGetTaskInstance,
  useTaskServiceGetTask,
} from "openapi/queries";
import type { TaskInstanceState } from "openapi/requests/types.gen";
import { BreadcrumbRow, CrumbLink, CrumbStack, CrumbText, type CrumbShape } from "src/components/Breadcrumb";
import { isStatePending, useAutoRefresh } from "src/utils";

import { DagSwitcherButton } from "./DagSwitcherButton";

type Crumb = {
  readonly caption: string;
  /** Set alongside `state` so the badge keeps its room while the value is still loading. */
  readonly hasState?: boolean;
  readonly key: string;
  readonly state?: TaskInstanceState | null;
  readonly to?: string;
  readonly value: string;
};

const BreadcrumbItem = ({
  crumb,
  dagId,
  isLast,
  shape,
}: {
  readonly crumb: Crumb;
  readonly dagId: string;
  readonly isLast: boolean;
  readonly shape: CrumbShape;
}) => {
  const content = (
    <CrumbStack
      caption={crumb.caption}
      hasState={crumb.hasState}
      isCurrent={isLast}
      state={crumb.state}
      value={crumb.value}
    />
  );

  if (crumb.key === "dag") {
    return (
      <DagSwitcherButton dagId={dagId} shape={shape}>
        {content}
      </DagSwitcherButton>
    );
  }

  if (isLast || crumb.to === undefined) {
    return <CrumbText shape={shape}>{content}</CrumbText>;
  }

  return (
    <CrumbLink shape={shape} to={crumb.to}>
      {content}
    </CrumbLink>
  );
};

export const DagBreadcrumb = () => {
  const { t: translate } = useTranslation();
  const { dagId = "", groupId, mapIndex = "-1", runId, taskId } = useParams();
  const { pathname } = useLocation();
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

  // A task inside a mapped task group has expanded instances even though the task itself reports
  // `is_mapped: false`, so the route is the reliable signal: both `/mapped` and `/mapped/:mapIndex`
  // mean a list of instances exists behind this task.
  const hasExpandedInstances = Boolean(task?.is_mapped) || mapIndex !== "-1" || pathname.endsWith("/mapped");

  // Expanded instances are a list rather than one instance, so there is no single state to show —
  // and asking for one without a map index is a guaranteed 404.
  const { data: taskInstance } = useTaskInstanceServiceGetTaskInstance(
    { dagId, dagRunId: runId ?? "", taskId: taskId ?? "" },
    undefined,
    {
      enabled: Boolean(runId) && Boolean(taskId) && !hasExpandedInstances,
      refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false),
    },
  );

  const crumbs: Array<Crumb> = [
    {
      caption: translate("dag_one"),
      key: "dag",
      to: `/dags/${dagId}`,
      value: dag?.dag_display_name ?? dagId,
    },
  ];

  // Add dag run breadcrumb
  if (runId !== undefined) {
    crumbs.push({
      caption: translate("dagRun_one"),
      hasState: true,
      key: "dagRun",
      state: dagRun?.state,
      to: `/dags/${dagId}/runs/${runId}`,
      value: dagRun === undefined ? runId : dagRun.dag_run_id,
    });
  }

  // Add group breadcrumb
  if (groupId !== undefined) {
    if (runId === undefined) {
      crumbs.push({
        caption: translate("dagRun_one"),
        key: "allRuns",
        to: `/dags/${dagId}/runs`,
        value: translate("allRuns", { ns: "dag" }),
      });
    }

    crumbs.push({
      caption: translate("taskGroup_one"),
      key: "group",
      to: `/dags/${dagId}/groups/${groupId}`,
      value: groupId,
    });
  }

  // Add task breadcrumb
  if (runId !== undefined && taskId !== undefined) {
    if (hasExpandedInstances) {
      crumbs.push({
        caption: translate("taskInstance_other"),
        key: "task",
        to: `/dags/${dagId}/runs/${runId}/tasks/${taskId}/mapped`,
        value: `${task?.task_display_name ?? taskId} [ ]`,
      });
    } else {
      crumbs.push({
        caption: translate("taskInstance_one"),
        hasState: true,
        key: "task",
        state: taskInstance?.state,
        value: task?.task_display_name ?? taskId,
      });
    }
  }

  if (runId === undefined && taskId !== undefined) {
    crumbs.push({
      caption: translate("dagRun_one"),
      key: "allRuns",
      to: `/dags/${dagId}/runs`,
      value: translate("allRuns", { ns: "dag" }),
    });
    crumbs.push({
      caption: translate("task_one"),
      key: "task",
      value: task?.task_display_name ?? taskId,
    });
  }

  if (mapIndex !== "-1") {
    crumbs.push({
      caption: translate("mapIndex"),
      hasState: true,
      key: "mapIndex",
      state: mappedTaskInstance?.state,
      value: mappedTaskInstance?.rendered_map_index ?? mapIndex,
    });
  }

  return (
    <BreadcrumbRow aria-label={translate("breadcrumb")} data-testid="dag-breadcrumb">
      {crumbs.map((crumb, index) => (
        <BreadcrumbItem
          crumb={crumb}
          dagId={dagId}
          isLast={index === crumbs.length - 1}
          key={crumb.key}
          shape={{ hasNotch: index > 0, hasPoint: index < crumbs.length - 1 }}
        />
      ))}
    </BreadcrumbRow>
  );
};
