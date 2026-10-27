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
import { useLocation, useParams, useSearchParams } from "react-router-dom";

import {
  useDagRunServiceGetDagRun,
  useDagServiceGetDagDetails,
  useTaskInstanceServiceGetMappedTaskInstance,
  useTaskInstanceServiceGetTaskInstance,
  useTaskServiceGetTask,
} from "openapi/queries";
import type { TaskInstanceState } from "openapi/requests/types.gen";

import { BreadcrumbRow, CrumbLink, CrumbStack, CrumbText, type CrumbShape } from "src/components/Breadcrumb";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useTaskInstanceCoordinates } from "src/hooks/useTaskInstanceCoordinates";
import { useGridTiSummariesStream } from "src/queries/useGridTISummaries.ts";
import { selectActivePass, useLoopFilterOptions, type LoopOption } from "src/queries/useLoopFilterOptions";
import { isStatePending, useAutoRefresh } from "src/utils";

import { DagRunSwitcherButton } from "./DagRunSwitcherButton";
import { DagSwitcherButton } from "./DagSwitcherButton";
import { IterationSwitcherButton } from "./IterationSwitcherButton";
import { MapIndexSwitcherButton } from "./MapIndexSwitcherButton";

/** Levels that stand for a Dag run — both offer the run search. */
const RUN_LEVEL_KEYS = new Set(["allRuns", "dagRun"]);

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
  isGroupLevel,
  isLast,
  isMapped,
  loopOptions,
  shape,
}: {
  readonly crumb: Crumb;
  readonly dagId: string;
  readonly isGroupLevel: boolean;
  readonly isLast: boolean;
  readonly isMapped: boolean;
  readonly loopOptions: Array<LoopOption>;
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

  if (crumb.key === "iteration") {
    return (
      <IterationSwitcherButton includeWholeLoop={isGroupLevel} options={loopOptions} shape={shape}>
        {content}
      </IterationSwitcherButton>
    );
  }

  if (crumb.key === "dag") {
    return (
      <DagSwitcherButton dagId={dagId} shape={shape}>
        {content}
      </DagSwitcherButton>
    );
  }

  if (RUN_LEVEL_KEYS.has(crumb.key) && crumb.to !== undefined) {
    return (
      <DagRunSwitcherButton dagId={dagId} isMapped={isMapped} shape={shape} to={crumb.to}>
        {content}
      </DagRunSwitcherButton>
    );
  }

  if (crumb.key === "mapIndex") {
    return (
      <MapIndexSwitcherButton dagId={dagId} shape={shape}>
        {content}
      </MapIndexSwitcherButton>
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
  const { t: translate } = useTranslation(["common", "dag"]);
  const { dagId = "", groupId, mapIndex = "-1", runId, taskId } = useParams();
  const [searchParams] = useSearchParams();
  // A task inside a loop is named for it, so either level resolves to the same loop.
  const { options: loopOptions } = useLoopFilterOptions({ dagId, groupId: groupId ?? taskId, runId });
  const { pathname } = useLocation();
  const coordinates = useTaskInstanceCoordinates();
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
      refetchInterval: (query) => isStatePending(query.state.data?.state) && refetchInterval,
    },
  );

  const { data: task } = useTaskServiceGetTask({ dagId, taskId }, undefined, { enabled: Boolean(taskId) });

  // A group has no instance of its own, so its state is the aggregate the grid already computes.
  const { summariesByRunId } = useGridTiSummariesStream({
    dagId,
    runIds: groupId !== undefined && runId !== undefined ? [runId] : [],
  });
  const groupSummary =
    groupId === undefined || runId === undefined
      ? undefined
      : summariesByRunId.get(runId)?.task_instances.find((summary) => summary.task_id === groupId);

  const { data: mappedTaskInstance } = useTaskInstanceServiceGetMappedTaskInstance(
    { ...coordinates, dagId, dagRunId: runId ?? "", mapIndex: parsedMapIndex, taskId: taskId ?? "" },
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
    { ...coordinates, dagId, dagRunId: runId ?? "", taskId: taskId ?? "" },
    undefined,
    {
      enabled: Boolean(runId) && Boolean(taskId) && !hasExpandedInstances,
      refetchInterval: (query) => isStatePending(query.state.data?.state) && refetchInterval,
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

  // The run level is always present, even with no run in the URL: it is where runs are switched,
  // so standing on "all runs" is what lets any page of the Dag jump straight to one.
  if (runId === undefined) {
    crumbs.push({
      caption: translate("dagRun_one"),
      key: "allRuns",
      to: `/dags/${dagId}/runs`,
      value: translate("dag:allRuns"),
    });
  } else {
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
    crumbs.push({
      caption: translate("taskGroup_one"),
      hasState: true,
      key: "group",
      state: groupSummary?.state,
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
      caption: translate("task_one"),
      key: "task",
      value: task?.task_display_name ?? taskId,
    });
  }

  // Only a level a loop encloses has a pass to name, so the crumb follows the options rather
  // than the route: no loop in scope means no options and no crumb.
  // A pass is a level inside a group or task; the run above them has no single loop to name.
  if (loopOptions.length > 0 && (groupId !== undefined || taskId !== undefined)) {
    // A group with no pass pinned is being looked at whole, which is a state only it can be in:
    // a task instance is always one row, so it falls back to the newest pass instead.
    const pinnedPass = searchParams.get(SearchParamsKeys.ITERATION);
    const isWholeLoop = taskId === undefined && (pinnedPass === null || pinnedPass === "");
    // A loaded row knows its own pass; only a group has to read it off the url.
    const resolvedPass = (mappedTaskInstance ?? taskInstance)?.loop_iteration?.iteration;
    const selected = isWholeLoop ? undefined : selectActivePass(loopOptions, searchParams, resolvedPass);

    if (isWholeLoop || selected !== undefined) {
      crumbs.push({
        caption: translate("common:filters.iteration"),
        hasState: true,
        key: "iteration",
        // Looking at the loop whole means its own outcome; looking at one pass means that pass's.
        state: isWholeLoop ? loopOptions[0]?.state : selected?.state,
        // How far the loop got belongs to the header's stat; the crumb names the pass alone.
        value: isWholeLoop ? translate("common:filters.allIterations") : String(selected?.iteration),
      });
    }
  }

  // The map index level stays on the bar for the whole of an expanded task, standing on "all
  // instances" when none is picked, so the switch between instances is in the same place whether
  // you are looking at one of them or at the summary of all of them.
  if (hasExpandedInstances && runId !== undefined && taskId !== undefined) {
    crumbs.push(
      mapIndex === "-1"
        ? { caption: translate("mapIndex"), key: "mapIndex", value: translate("dag:allMapIndexes") }
        : {
            caption: translate("mapIndex"),
            hasState: true,
            key: "mapIndex",
            state: mappedTaskInstance?.state,
            value: mappedTaskInstance?.rendered_map_index ?? mapIndex,
          },
    );
  }

  return (
    <BreadcrumbRow aria-label={translate("breadcrumb")} data-testid="dag-breadcrumb">
      {crumbs.map((crumb, index) => (
        <BreadcrumbItem
          crumb={crumb}
          dagId={dagId}
          isGroupLevel={taskId === undefined}
          isLast={index === crumbs.length - 1}
          isMapped={hasExpandedInstances}
          key={crumb.key}
          loopOptions={loopOptions}
          shape={{ hasNotch: index > 0, hasPoint: index < crumbs.length - 1 }}
        />
      ))}
    </BreadcrumbRow>
  );
};
