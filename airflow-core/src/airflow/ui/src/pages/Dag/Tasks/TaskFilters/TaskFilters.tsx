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
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import type { TaskCollectionResponse } from "openapi/requests";
import { FilterBar, type FilterValue } from "src/components/FilterBar";
import type { FilterConfig } from "src/components/FilterBar/types";
import { SearchParamsKeys } from "src/constants/searchParams.ts";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

export const TaskFilters = ({ tasksData }: { readonly tasksData: TaskCollectionResponse | undefined }) => {
  const { MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE } = SearchParamsKeys;
  const { t: translate } = useTranslation("tasks");

  const searchParamKeys = useMemo(
    (): Array<FilterableSearchParamsKeys> => [NAME_PATTERN, OPERATOR, TRIGGER_RULE, RETRIES, MAPPED],
    [MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE],
  );

  const { handleFiltersChange, searchParams } = useFiltersHandler(searchParamKeys);

  // Build options from payload
  const allOperatorNames: Array<string> = [
    ...new Set(tasksData?.tasks.map((task) => task.operator_name).filter((item) => item !== null) ?? []),
  ];
  const allTriggerRules: Array<string> = [
    ...new Set(tasksData?.tasks.map((task) => task.trigger_rule).filter((item) => item !== null) ?? []),
  ];
  const allRetryValues: Array<string> = [
    ...new Set(
      tasksData?.tasks.map((task) => task.retries?.toString()).filter((item) => item !== undefined) ?? [],
    ),
  ];
  const allMappedValues = [
    { key: "true", label: translate("mapped") },
    { key: "false", label: translate("notMapped") },
  ];

  const operatorOptions = allOperatorNames
    .map((value) => ({ label: value, value }))
    .sort((left, right) => left.label.localeCompare(right.label));
  const triggerRuleOptions = allTriggerRules
    .map((value) => ({ label: value, value }))
    .sort((left, right) => left.label.localeCompare(right.label));
  const retryValueOptions = allRetryValues
    .map((value) => ({ label: value, value }))
    .sort((left, right) => left.label.localeCompare(right.label));
  const mappedOptions = allMappedValues.map(({ key, label }) => ({ label, value: key }));

  // IMPORTANT: always include configs for keys that may exist in the URL,
  // even when options are empty on first render (prevents "config not found for key" crash).
  const configs: Array<FilterConfig> = [
    {
      hotkeyDisabled: true,
      key: NAME_PATTERN,
      label: translate("searchTasks"),
      placeholder: translate("searchTasks"),
      type: "text",
    },
    {
      key: OPERATOR,
      label: translate("selectOperator", { defaultValue: "Operators" }),
      multiple: true,
      options: operatorOptions,
      type: "select",
    },
    {
      key: TRIGGER_RULE,
      label: translate("selectTriggerRules", { defaultValue: "Trigger rules" }),
      multiple: true,
      options: triggerRuleOptions,
      type: "select",
    },
    {
      key: RETRIES,
      label: translate("retries", { defaultValue: "Retry values" }),
      multiple: true,
      options: retryValueOptions,
      type: "select",
    },
    {
      key: MAPPED,
      label: translate("mapped", { defaultValue: "Mapped" }),
      multiple: false,
      options: mappedOptions,
      type: "select",
    },
  ];

  const initialValues = useMemo(() => {
    const values: Record<string, FilterValue> = {};

    const name = searchParams.get(NAME_PATTERN);

    if (name !== null && name !== "") {
      values[NAME_PATTERN] = name;
    }

    const mapped = searchParams.get(MAPPED);

    if (mapped !== null && mapped !== "") {
      values[MAPPED] = mapped;
    }

    const operators = searchParams.getAll(OPERATOR);

    if (operators.length > 0) {
      values[OPERATOR] = operators;
    }

    const triggerRules = searchParams.getAll(TRIGGER_RULE);

    if (triggerRules.length > 0) {
      values[TRIGGER_RULE] = triggerRules;
    }

    const retries = searchParams.getAll(RETRIES);

    if (retries.length > 0) {
      values[RETRIES] = retries;
    }

    return values;
  }, [MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE, searchParams]);

  return (
    <Box p={2}>
      <FilterBar
        configs={configs}
        initialValues={initialValues}
        maxVisibleFilters={10}
        onFiltersChange={handleFiltersChange}
      />
    </Box>
  );
};

export default TaskFilters;
