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
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { FiBarChart } from "react-icons/fi";
import { LuBrackets } from "react-icons/lu";
import { MdDateRange, MdSearch } from "react-icons/md";

import { DagIcon } from "src/assets/DagIcon";
import { TaskIcon } from "src/assets/TaskIcon";
import type { FilterConfig } from "src/components/FilterBar";

import { SearchParamsKeys } from "./searchParams";

/**
 * Hook to get filter configurations with translations
 */
export const useFilterConfigs = () => {
  const { t: translate } = useTranslation(["browse", "common", "admin"]);

  const filterConfigMap = useMemo(
    () => ({
      [SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN]: {
        hotkeyDisabled: true,
        icon: <DagIcon />,
        label: translate("common:dagName"),
        placeholder: translate("common:filters.dagDisplayNamePlaceholder"),
        type: "text" as const,
      },
      [SearchParamsKeys.KEY_PATTERN]: {
        icon: <MdSearch />,
        label: translate("admin:columns.key"),
        placeholder: translate("common:filters.keyPlaceholder"),
        type: "text" as const,
      },
      [SearchParamsKeys.LOGICAL_DATE_GTE]: {
        icon: <MdDateRange />,
        label: translate("common:filters.logicalDateFromPlaceholder"),
        placeholder: translate("common:filters.logicalDateFromPlaceholder"),
        type: "date" as const,
      },
      [SearchParamsKeys.LOGICAL_DATE_LTE]: {
        icon: <MdDateRange />,
        label: translate("common:filters.logicalDateToPlaceholder"),
        placeholder: translate("common:filters.logicalDateToPlaceholder"),
        type: "date" as const,
      },
      [SearchParamsKeys.MAP_INDEX]: {
        icon: <LuBrackets />,
        label: translate("common:mapIndex"),
        min: -1,
        placeholder: translate("common:filters.mapIndexPlaceholder"),
        type: "number" as const,
      },
      [SearchParamsKeys.RUN_AFTER_GTE]: {
        icon: <MdDateRange />,
        label: translate("common:filters.runAfterFromPlaceholder"),
        placeholder: translate("common:filters.runAfterFromPlaceholder"),
        type: "date" as const,
      },
      [SearchParamsKeys.RUN_AFTER_LTE]: {
        icon: <MdDateRange />,
        label: translate("common:filters.runAfterToPlaceholder"),
        placeholder: translate("common:filters.runAfterToPlaceholder"),
        type: "date" as const,
      },
      [SearchParamsKeys.RUN_ID_PATTERN]: {
        hotkeyDisabled: true,
        icon: <FiBarChart />,
        label: translate("common:runId"),
        placeholder: translate("common:filters.runIdPlaceholder"),
        type: "text" as const,
      },
      [SearchParamsKeys.TASK_ID_PATTERN]: {
        hotkeyDisabled: true,
        icon: <TaskIcon />,
        label: translate("common:taskId"),
        placeholder: translate("common:filters.taskIdPlaceholder"),
        type: "text" as const,
      },
    }),
    [translate],
  );

  const getFilterConfig = useMemo(
    () =>
      (key: keyof typeof filterConfigMap): FilterConfig => ({
        key,
        ...filterConfigMap[key],
      }),
    [filterConfigMap],
  );

  return { getFilterConfig };
};
