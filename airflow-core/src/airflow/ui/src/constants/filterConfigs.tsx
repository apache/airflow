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
import { Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { BiTargetLock } from "react-icons/bi";
import { FiBarChart, FiDatabase, FiTag, FiUser, FiUsers } from "react-icons/fi";
import { LuBrackets } from "react-icons/lu";
import {
  MdBuild,
  MdCheckCircle,
  MdCode,
  MdComputer,
  MdDateRange,
  MdHistory,
  MdHourglassEmpty,
  MdPause,
  MdPendingActions,
  MdPlayArrow,
  MdSchedule,
  MdSearch,
  MdStar,
} from "react-icons/md";
import { PiQueue } from "react-icons/pi";

import { useTeamsServiceListTeams } from "openapi/queries";
import type { DagRunState, DagRunType, TaskInstanceState } from "openapi/requests/types.gen";

import type { FilterConfig } from "src/components/FilterBar";
import { TagsFilter } from "src/components/FilterBar/filters/TagsFilter";
import { TimetableTypeFilter } from "src/components/FilterBar/filters/TimetableTypeFilter";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { StateBadge } from "src/components/StateBadge";

import { DagIcon } from "src/assets/DagIcon";
import { TaskIcon } from "src/assets/TaskIcon";
import {
  dagRunStateOptions,
  dagRunTypeOptions,
  jobStateOptions,
  jobTypeOptions,
  taskInstanceStateOptions,
} from "src/constants/stateOptions";
import { useConfig } from "src/queries/useConfig";

import { SearchParamsKeys } from "./searchParams";

export enum FilterTypes {
  BOOLEAN = "boolean",
  DATE = "date",
  DATERANGE = "daterange",
  MULTISELECT = "multiselect",
  NUMBER = "number",
  SELECT = "select",
  TEXT = "text",
}

/**
 * Drops the "all" entry from an option list. Selecting it matches everything, which is the same
 * as not filtering — and since a filter left unset is removed, the pill's absence already says
 * so. Offering it too gives two ways to spell one thing, one of which looks like a filter.
 */
const withoutAllOption = <TOption extends { value: string }>(options: ReadonlyArray<TOption>) =>
  options.filter((option) => option.value !== "all");

export const useFilterConfigs = () => {
  const { t: translate } = useTranslation([
    "assets",
    "browse",
    "common",
    "components",
    "admin",
    "dags",
    "hitl",
  ]);
  const multiTeamEnabled = Boolean(useConfig("multi_team"));
  const { data: teamsData } = useTeamsServiceListTeams({ orderBy: ["name"] }, undefined, {
    enabled: multiTeamEnabled,
  });

  const runStateOptions = withoutAllOption(dagRunStateOptions.items).map((option) => ({
    label: <StateBadge state={option.value as DagRunState}>{translate(option.label)}</StateBadge>,
    value: option.value,
  }));

  const filterConfigMap = {
    [SearchParamsKeys.ASSET_EVENT_DATE_RANGE]: {
      endKey: SearchParamsKeys.END_DATE,
      icon: <MdDateRange />,
      label: translate("components:backfill.dateRange"),
      startKey: SearchParamsKeys.START_DATE,
      type: FilterTypes.DATERANGE,
    },
    [SearchParamsKeys.BODY_SEARCH]: {
      hotkeyDisabled: true,
      icon: <MdSearch />,
      label: translate("hitl:filters.body"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.BUNDLE_VERSION]: {
      hotkeyDisabled: true,
      icon: <MdCode />,
      label: translate("components:versionDetails.bundleVersion"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.CONF_CONTAINS]: {
      hotkeyDisabled: true,
      icon: <MdCode />,
      label: translate("common:dagRun.conf"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.CONSUMING_ASSET_PATTERN]: {
      hotkeyDisabled: true,
      icon: <FiDatabase />,
      label: translate("common:consumingAsset"),
      placeholder: translate("common:filters.searchAsset"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.CREATED_AT_RANGE]: {
      endKey: SearchParamsKeys.CREATED_AT_LTE,
      icon: <MdDateRange />,
      label: translate("hitl:filters.createdAt"),
      startKey: SearchParamsKeys.CREATED_AT_GTE,
      type: FilterTypes.DATERANGE,
    },
    [SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN]: {
      hotkeyDisabled: true,
      icon: <DagIcon />,
      label: translate("common:dagId"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.DAG_ID]: {
      hotkeyDisabled: true,
      icon: <DagIcon />,
      label: translate("common:dagId"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.DAG_ID_PATTERN]: {
      hotkeyDisabled: true,
      icon: <DagIcon />,
      label: translate("common:dagId"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.DAG_RUN_STATE]: {
      icon: <MdCheckCircle />,
      label: translate("dags:filters.anyRunState"),
      options: runStateOptions,
      placeholder: translate("dags:filters.anyRunStatePlaceholder"),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.DAG_VERSION]: {
      hotkeyDisabled: true,
      icon: <MdHistory />,
      label: translate("common:dagRun.dagVersions"),
      min: 1,
      type: FilterTypes.NUMBER,
    },
    [SearchParamsKeys.DEADLINE_TIME_RANGE]: {
      endKey: SearchParamsKeys.DEADLINE_TIME_LTE,
      icon: <MdDateRange />,
      label: translate("browse:deadlines.columns.deadlineTime"),
      startKey: SearchParamsKeys.DEADLINE_TIME_GTE,
      type: FilterTypes.DATERANGE,
    },
    [SearchParamsKeys.DURATION_GTE]: {
      icon: <MdHourglassEmpty />,
      label: translate("common:filters.durationFrom"),
      min: 0,
      type: FilterTypes.NUMBER,
    },
    [SearchParamsKeys.DURATION_LTE]: {
      icon: <MdHourglassEmpty />,
      label: translate("common:filters.durationTo"),
      min: 0,
      type: FilterTypes.NUMBER,
    },
    [SearchParamsKeys.END_DATE_RANGE]: {
      endKey: SearchParamsKeys.END_DATE_LTE,
      icon: <MdDateRange />,
      label: translate("common:endDate"),
      startKey: SearchParamsKeys.END_DATE_GTE,
      type: FilterTypes.DATERANGE,
    },
    [SearchParamsKeys.EVENT_DATE_RANGE]: {
      endKey: SearchParamsKeys.BEFORE,
      icon: <MdDateRange />,
      label: translate("common:logicalDate"),
      startKey: SearchParamsKeys.AFTER,
      type: FilterTypes.DATERANGE,
    },
    [SearchParamsKeys.EVENT_TYPE]: {
      label: translate("browse:auditLog.filters.eventType"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.EXECUTOR_CLASS]: {
      hotkeyDisabled: true,
      icon: <MdBuild />,
      label: translate("admin:jobs.columns.executorClass"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.FAVORITE]: {
      icon: <MdStar />,
      label: translate("dags:filters.favoriteState"),
      options: [
        { label: translate("dags:filters.favorite.favorite"), value: "true" },
        { label: translate("dags:filters.favorite.unfavorite"), value: "false" },
      ],
      placeholder: translate("dags:filters.favoriteStatePlaceholder"),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.GROUP_PATTERN]: {
      hotkeyDisabled: true,
      icon: <FiDatabase />,
      label: translate("assets:group"),
      placeholder: translate("assets:filters.groupPlaceholder"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.HAS_EVENTS]: {
      icon: <MdCheckCircle />,
      label: translate("assets:filters.hasEvents"),
      options: [
        { label: translate("assets:filters.hasEventsOptions.yes"), value: "true" },
        { label: translate("assets:filters.hasEventsOptions.no"), value: "false" },
      ],
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.HOSTNAME]: {
      hotkeyDisabled: true,
      icon: <MdComputer />,
      label: translate("admin:jobs.columns.hostname"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.JOB_STATE]: {
      icon: <MdCheckCircle />,
      label: translate("common:state"),
      options: withoutAllOption(jobStateOptions.items).map((option) => ({
        label: translate(option.label),
        value: option.value,
      })),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.JOB_TYPE]: {
      icon: <MdBuild />,
      label: translate("admin:jobs.columns.jobType"),
      options: withoutAllOption(jobTypeOptions.items).map((option) => ({
        label: translate(option.label),
        value: option.value,
      })),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.KEY_PATTERN]: {
      icon: <MdSearch />,
      label: translate("admin:columns.key"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.LAST_ASSET_EVENT_TIMESTAMP_RANGE]: {
      endKey: SearchParamsKeys.LAST_ASSET_EVENT_TIMESTAMP_LTE,
      icon: <MdDateRange />,
      label: translate("assets:filters.lastEventDateRange"),
      startKey: SearchParamsKeys.LAST_ASSET_EVENT_TIMESTAMP_GTE,
      type: FilterTypes.DATERANGE,
    },
    [SearchParamsKeys.LAST_DAG_RUN_STATE]: {
      icon: <MdCheckCircle />,
      label: translate("dags:filters.lastRunState"),
      options: runStateOptions,
      placeholder: translate("dags:filters.lastRunStatePlaceholder"),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.LOGICAL_DATE_RANGE]: {
      endKey: SearchParamsKeys.LOGICAL_DATE_LTE,
      icon: <MdDateRange />,
      label: translate("common:logicalDate"),
      startKey: SearchParamsKeys.LOGICAL_DATE_GTE,
      type: FilterTypes.DATERANGE,
    },
    [SearchParamsKeys.MAP_INDEX]: {
      icon: <LuBrackets />,
      label: translate("common:mapIndex"),
      min: -1,
      type: FilterTypes.NUMBER,
    },
    [SearchParamsKeys.MISSED]: {
      icon: <MdCheckCircle />,
      label: translate("browse:deadlines.filters.status"),
      options: [
        { label: translate("browse:deadlines.filters.statusOptions.pending"), value: "false" },
        { label: translate("browse:deadlines.filters.statusOptions.missed"), value: "true" },
      ],
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.NAME_PATTERN]: {
      hotkeyDisabled: true,
      icon: <TaskIcon />,
      label: translate("common:taskId"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.NEEDS_REVIEW]: {
      icon: <MdPendingActions />,
      label: translate("dags:filters.requiresHitlAction"),
      type: FilterTypes.BOOLEAN,
    },
    [SearchParamsKeys.OPERATOR_NAME_PATTERN]: {
      hotkeyDisabled: true,
      icon: <MdBuild />,
      label: translate("common:task.operator"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.OWNERS]: {
      icon: <FiUser />,
      isCreatable: true,
      label: translate("common:dagDetails.owner"),
      options: [],
      placeholder: translate("common:table.ownerPlaceholder"),
      type: FilterTypes.MULTISELECT,
    },
    [SearchParamsKeys.PARTITION_KEY_PATTERN]: {
      hotkeyDisabled: true,
      icon: <MdSearch />,
      label: translate("common:dagRun.partitionKey"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.PAUSED]: {
      icon: <MdPause />,
      label: translate("dags:filters.pausedState"),
      options: [
        { label: translate("dags:filters.paused.active"), value: "false" },
        { label: translate("dags:filters.paused.paused"), value: "true" },
      ],
      placeholder: translate("dags:filters.pausedPlaceholder"),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.POOL_NAME_PATTERN]: {
      hotkeyDisabled: true,
      icon: <BiTargetLock />,
      label: translate("common:taskInstance.pool"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.QUEUE_NAME_PATTERN]: {
      hotkeyDisabled: true,
      icon: <PiQueue />,
      label: translate("common:taskInstance.queue"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.RENDERED_MAP_INDEX]: {
      hotkeyDisabled: true,
      icon: <MdSearch />,
      label: translate("common:taskInstance.renderedMapIndex"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.RESPONDED_BY_USER_NAME]: {
      hotkeyDisabled: true,
      icon: <FiUser />,
      label: translate("hitl:response.responded_by_user_name"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.RESPONSE_RECEIVED]: {
      icon: <FiUser />,
      label: translate("hitl:requiredActionState"),
      options: [
        {
          label: <StateBadge state="awaiting_input">{translate("hitl:filters.response.pending")}</StateBadge>,
          value: "false",
        },
        {
          label: <StateBadge state="success">{translate("hitl:filters.response.received")}</StateBadge>,
          value: "true",
        },
      ],
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.RUN_AFTER_RANGE]: {
      endKey: SearchParamsKeys.RUN_AFTER_LTE,
      icon: <MdDateRange />,
      label: translate("common:dagRun.runAfter"),
      startKey: SearchParamsKeys.RUN_AFTER_GTE,
      type: FilterTypes.DATERANGE,
    },
    [SearchParamsKeys.RUN_ID]: {
      hotkeyDisabled: true,
      icon: <FiBarChart />,
      label: translate("common:runId"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.RUN_ID_PATTERN]: {
      hotkeyDisabled: true,
      icon: <FiBarChart />,
      label: translate("common:runId"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.RUN_TYPE]: {
      icon: <MdPlayArrow />,
      label: translate("common:dagRun.runType"),
      options: withoutAllOption(dagRunTypeOptions.items).map((option) => ({
        label: (
          <Box alignItems="center" display="inline-flex" gap={1}>
            <RunTypeIcon runType={option.value as DagRunType} />
            {translate(option.label)}
          </Box>
        ),
        value: option.value,
      })),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.START_DATE_RANGE]: {
      endKey: SearchParamsKeys.START_DATE_LTE,
      icon: <MdDateRange />,
      label: translate("common:startDate"),
      startKey: SearchParamsKeys.START_DATE_GTE,
      type: FilterTypes.DATERANGE,
    },
    [SearchParamsKeys.STATE]: {
      icon: <MdCheckCircle />,
      label: translate("common:state"),
      options: runStateOptions,
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.SUBJECT_SEARCH]: {
      icon: <MdSearch />,
      label: translate("hitl:subject"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.TAGS]: {
      EditorComponent: TagsFilter,
      icon: <FiTag />,
      label: translate("common:dagDetails.tags"),
      matchModeKey: SearchParamsKeys.TAGS_MATCH_MODE,
      placeholder: translate("common:table.tagPlaceholder"),
      type: FilterTypes.MULTISELECT,
    },
    [SearchParamsKeys.TASK_ID]: {
      hotkeyDisabled: true,
      icon: <TaskIcon />,
      label: translate("common:taskId"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.TASK_ID_PATTERN]: {
      hotkeyDisabled: true,
      icon: <TaskIcon />,
      label: translate("common:taskId"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.TASK_STATE]: {
      icon: <MdCheckCircle />,
      label: translate("common:state"),
      options: withoutAllOption(taskInstanceStateOptions.items).map((option) => ({
        label: <StateBadge state={option.value as TaskInstanceState}>{translate(option.label)}</StateBadge>,
        value: option.value,
      })),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.TEAMS]: {
      icon: <FiUsers />,
      label: translate("common:dagDetails.team"),
      options: (teamsData?.teams ?? []).map((team) => ({ label: team.name, value: team.name })),
      type: FilterTypes.MULTISELECT,
    },
    [SearchParamsKeys.TIMETABLE_TYPE]: {
      EditorComponent: TimetableTypeFilter,
      icon: <MdSchedule />,
      label: translate("dags:filters.timetableType"),
      type: FilterTypes.MULTISELECT,
    },
    [SearchParamsKeys.TRIGGERING_USER_NAME_PATTERN]: {
      hotkeyDisabled: true,
      icon: <FiUser />,
      label: translate("common:dagRun.triggeringUser"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.TRY_NUMBER]: {
      label: translate("common:tryNumber"),
      min: 1,
      type: FilterTypes.NUMBER,
    },
    [SearchParamsKeys.USER]: {
      icon: <FiUser />,
      label: translate("common:user"),
      supportsAdvancedSearch: true,
      type: FilterTypes.TEXT,
    },
  };

  const getFilterConfig = (key: keyof typeof filterConfigMap): FilterConfig => ({
    key,
    ...filterConfigMap[key],
  });

  return { getFilterConfig };
};
