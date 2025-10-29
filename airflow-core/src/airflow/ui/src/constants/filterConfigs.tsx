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
import { Flex } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiBarChart, FiUser } from "react-icons/fi";
import { LuBrackets } from "react-icons/lu";
import {
  MdDateRange,
  MdSearch,
  MdHistory,
  MdHourglassEmpty,
  MdCode,
  MdPlayArrow,
  MdCheckCircle,
} from "react-icons/md";

import type { DagRunState, DagRunType } from "openapi/requests/types.gen";
import { DagIcon } from "src/assets/DagIcon";
import { TaskIcon } from "src/assets/TaskIcon";
import type { FilterConfig } from "src/components/FilterBar";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { StateBadge } from "src/components/StateBadge";
import { dagRunStateOptions, dagRunTypeOptions } from "src/constants/stateOptions";

import { SearchParamsKeys } from "./searchParams";

export enum FilterTypes {
  DATE = "date",
  NUMBER = "number",
  SELECT = "select",
  TEXT = "text",
}

export const useFilterConfigs = () => {
  const { t: translate } = useTranslation(["browse", "common", "admin", "hitl"]);

  const filterConfigMap = {
    [SearchParamsKeys.AFTER]: {
      icon: <MdDateRange />,
      label: translate("common:table.from"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.BEFORE]: {
      icon: <MdDateRange />,
      label: translate("common:table.to"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.BODY_SEARCH]: {
      hotkeyDisabled: true,
      icon: <MdSearch />,
      label: translate("hitl:filters.body"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.CONF_CONTAINS]: {
      hotkeyDisabled: true,
      icon: <MdCode />,
      label: translate("common:dagRun.conf"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.CREATED_AT_GTE]: {
      icon: <MdDateRange />,
      label: translate("hitl:filters.createdAtFrom"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.CREATED_AT_LTE]: {
      icon: <MdDateRange />,
      label: translate("hitl:filters.createdAtTo"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN]: {
      hotkeyDisabled: true,
      icon: <DagIcon />,
      label: translate("common:dagId"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.DAG_ID]: {
      hotkeyDisabled: true,
      icon: <DagIcon />,
      label: translate("common:dagId"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.DAG_ID_PATTERN]: {
      hotkeyDisabled: true,
      icon: <DagIcon />,
      label: translate("common:dagId"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.DAG_VERSION]: {
      hotkeyDisabled: true,
      icon: <MdHistory />,
      label: translate("common:dagRun.dagVersions"),
      min: 1,
      type: FilterTypes.NUMBER,
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
    [SearchParamsKeys.END_DATE]: {
      icon: <MdDateRange />,
      label: translate("common:table.to"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.EVENT_TYPE]: {
      label: translate("browse:auditLog.filters.eventType"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.KEY_PATTERN]: {
      icon: <MdSearch />,
      label: translate("admin:columns.key"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.LOGICAL_DATE_GTE]: {
      icon: <MdDateRange />,
      label: translate("common:filters.logicalDateFrom"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.LOGICAL_DATE_LTE]: {
      icon: <MdDateRange />,
      label: translate("common:filters.logicalDateTo"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.MAP_INDEX]: {
      icon: <LuBrackets />,
      label: translate("common:mapIndex"),
      min: -1,
      type: FilterTypes.NUMBER,
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
        { label: translate("hitl:filters.response.all"), value: "all" },
        {
          label: <StateBadge state="deferred">{translate("hitl:filters.response.pending")}</StateBadge>,
          value: "false",
        },
        {
          label: <StateBadge state="success">{translate("hitl:filters.response.received")}</StateBadge>,
          value: "true",
        },
      ],
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.RUN_AFTER_GTE]: {
      icon: <MdDateRange />,
      label: translate("common:filters.runAfterFrom"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.RUN_AFTER_LTE]: {
      icon: <MdDateRange />,
      label: translate("common:filters.runAfterTo"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.RUN_ID]: {
      hotkeyDisabled: true,
      icon: <FiBarChart />,
      label: translate("common:runId"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.RUN_ID_PATTERN]: {
      hotkeyDisabled: true,
      icon: <FiBarChart />,
      label: translate("common:runId"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.RUN_TYPE]: {
      icon: <MdPlayArrow />,
      label: translate("common:dagRun.runType"),
      options: dagRunTypeOptions.items.map((option) => ({
        label:
          option.value === "all" ? (
            translate(option.label)
          ) : (
            <Flex alignItems="center" gap={1}>
              <RunTypeIcon runType={option.value as DagRunType} />
              {translate(option.label)}
            </Flex>
          ),
        value: option.value,
      })),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.START_DATE]: {
      icon: <MdDateRange />,
      label: translate("common:table.from"),
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.STATE]: {
      icon: <MdCheckCircle />,
      label: translate("common:state"),
      options: dagRunStateOptions.items.map((option) => ({
        label:
          option.value === "all" ? (
            translate(option.label)
          ) : (
            <StateBadge state={option.value as DagRunState}>{translate(option.label)}</StateBadge>
          ),
        value: option.value,
      })),
      type: FilterTypes.SELECT,
    },
    [SearchParamsKeys.SUBJECT_SEARCH]: {
      icon: <MdSearch />,
      label: translate("hitl:subject"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.TASK_ID]: {
      hotkeyDisabled: true,
      icon: <TaskIcon />,
      label: translate("common:taskId"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.TASK_ID_PATTERN]: {
      hotkeyDisabled: true,
      icon: <TaskIcon />,
      label: translate("common:taskId"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.TRIGGERING_USER_NAME_PATTERN]: {
      hotkeyDisabled: true,
      icon: <FiUser />,
      label: translate("common:dagRun.triggeringUser"),
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
      type: FilterTypes.TEXT,
    },
  };

  const getFilterConfig = (key: keyof typeof filterConfigMap): FilterConfig => ({
    key,
    ...filterConfigMap[key],
  });

  return { getFilterConfig };
};
