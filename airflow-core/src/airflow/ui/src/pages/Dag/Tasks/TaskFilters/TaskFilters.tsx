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
import { useSearchParams } from "react-router-dom";

import type { TaskCollectionResponse } from "openapi/requests";
import { FilterBar } from "src/components/FilterBar/FilterBar";
import type { FilterConfig } from "src/components/FilterBar/types";
import { SearchParamsKeys } from "src/constants/searchParams.ts";

export const TaskFilters = ({ tasksData }: { readonly tasksData: TaskCollectionResponse | undefined }) => {
  const { MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE } = SearchParamsKeys;
  const { t: translate } = useTranslation("tasks");
  const [searchParams, setSearchParams] = useSearchParams();

  // Keep previous local names derived from URL (for minimal churn)
  const selectedOperators = searchParams.getAll(OPERATOR);
  const selectedTriggerRules = searchParams.getAll(TRIGGER_RULE);
  const selectedRetries = searchParams.getAll(RETRIES);
  const selectedMapped = searchParams.get(MAPPED) ?? undefined;
  const taskNamePattern = searchParams.get(NAME_PATTERN) ?? "";

  // Build options from payload using the same intermediate names
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

  // Convert to FilterBar select option shape (label/value)
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

  // FilterBar configs (keys unchanged; labels stick to prior wording)
  const configsUnfiltered: Array<FilterConfig | false> = [
    {
      hotkeyDisabled: true,
      key: NAME_PATTERN,
      label: translate("searchTasks"),
      placeholder: translate("searchTasks"),
      type: "text",
    },
    operatorOptions.length > 0 && {
      key: OPERATOR,
      label: translate("selectOperator", { defaultValue: "Operators" }),
      multiple: true,
      options: operatorOptions,
      type: "select",
    },
    triggerRuleOptions.length > 0 && {
      key: TRIGGER_RULE,
      label: translate("selectTriggerRules", { defaultValue: "Trigger rules" }),
      multiple: true,
      options: triggerRuleOptions,
      type: "select",
    },
    retryValueOptions.length > 0 && {
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

  const configs: Array<FilterConfig> = configsUnfiltered.filter(Boolean) as Array<FilterConfig>;

  // Initial values mirror previous local names
  const initialValues = {
    [MAPPED]: selectedMapped,
    [NAME_PATTERN]: taskNamePattern || undefined,
    [OPERATOR]: selectedOperators.length ? selectedOperators : undefined,
    [RETRIES]: selectedRetries.length ? selectedRetries : undefined,
    [TRIGGER_RULE]: selectedTriggerRules.length ? selectedTriggerRules : undefined,
  };

  // Single place to write the URL params (replaces the old individual handlers)
  const onFiltersChange = (record: Record<string, unknown>): void => {
    const next = new URLSearchParams(searchParams);

    // clear the filter-related keys before writing
    [NAME_PATTERN, OPERATOR, TRIGGER_RULE, RETRIES, MAPPED, "page"].forEach((key) => next.delete(key));

    const name = typeof record[NAME_PATTERN] === "string" ? record[NAME_PATTERN] : "";

    if (name && String(name).trim() !== "") {
      next.set(NAME_PATTERN, String(name));
    }

    const operators = Array.isArray(record[OPERATOR]) ? (record[OPERATOR] as Array<string>) : [];

    operators.forEach((val) => next.append(OPERATOR, val));

    const triggers = Array.isArray(record[TRIGGER_RULE]) ? (record[TRIGGER_RULE] as Array<string>) : [];

    triggers.forEach((val) => next.append(TRIGGER_RULE, val));

    const retries = Array.isArray(record[RETRIES]) ? (record[RETRIES] as Array<string>) : [];

    retries.forEach((val) => next.append(RETRIES, val));

    const mapped = typeof record[MAPPED] === "string" ? record[MAPPED] : "";

    if (mapped && String(mapped).trim() !== "") {
      next.set(MAPPED, String(mapped));
    }

    // keep the existing behavior used by tests: reset to page 1 on any filter change
    next.set("page", "1");

    setSearchParams(next);
  };

  return (
    <Box p={2}>
      <FilterBar
        configs={configs}
        initialValues={initialValues}
        maxVisibleFilters={10}
        onFiltersChange={onFiltersChange}
      />
    </Box>
  );
};

export default TaskFilters;
