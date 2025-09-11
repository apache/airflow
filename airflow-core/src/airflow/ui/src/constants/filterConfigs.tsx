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
import { FiBarChart } from "react-icons/fi";
import { LuBrackets } from "react-icons/lu";
import { MdDateRange, MdSearch } from "react-icons/md";

import { DagIcon } from "src/assets/DagIcon";
import { TaskIcon } from "src/assets/TaskIcon";
import type { FilterConfig } from "src/components/FilterBar";

import { SearchParamsKeys } from "./searchParams";

export enum FilterTypes {
  DATE = "date",
  NUMBER = "number",
  TEXT = "text",
}

export const useFilterConfigs = () => {
  const { t: translate } = useTranslation(["browse", "common", "admin"]);

  const filterConfigMap = {
    [SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN]: {
      hotkeyDisabled: true,
      icon: <DagIcon />,
      label: translate("common:dagId"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.KEY_PATTERN]: {
      icon: <MdSearch />,
      label: translate("admin:columns.key"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.LOGICAL_DATE_GTE]: {
      icon: <MdDateRange />,
      label: translate("common:filters.logicalDateFromLabel", "Logical date from"), // TODO: delete the fallback after the translation freeze
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.LOGICAL_DATE_LTE]: {
      icon: <MdDateRange />,
      label: translate("common:filters.logicalDateToLabel", "Logical date to"), // TODO: delete the fallback after the translation freeze
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.MAP_INDEX]: {
      icon: <LuBrackets />,
      label: translate("common:mapIndex"),
      min: -1,
      type: FilterTypes.NUMBER,
    },
    [SearchParamsKeys.RUN_AFTER_GTE]: {
      icon: <MdDateRange />,
      label: translate("common:filters.runAfterFromLabel", "Run after from"), // TODO: delete the fallback after the translation freeze
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.RUN_AFTER_LTE]: {
      icon: <MdDateRange />,
      label: translate("common:filters.runAfterToLabel", "Run after to"), // TODO: delete the fallback after the translation freeze
      type: FilterTypes.DATE,
    },
    [SearchParamsKeys.RUN_ID_PATTERN]: {
      hotkeyDisabled: true,
      icon: <FiBarChart />,
      label: translate("common:runId"),
      type: FilterTypes.TEXT,
    },
    [SearchParamsKeys.TASK_ID_PATTERN]: {
      hotkeyDisabled: true,
      icon: <TaskIcon />,
      label: translate("common:taskId"),
      type: FilterTypes.TEXT,
    },
  };

  const getFilterConfig = (key: keyof typeof filterConfigMap): FilterConfig => ({
    key,
    ...filterConfigMap[key],
  });

  return { getFilterConfig };
};
