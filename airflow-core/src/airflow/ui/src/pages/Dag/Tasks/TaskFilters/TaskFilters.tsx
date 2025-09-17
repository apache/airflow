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
import { useEffect, useMemo, useRef } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import type { TaskResponse } from "openapi/requests/types.gen";
import { FilterBar } from "src/components/FilterBar/FilterBar";
import type { FilterConfig } from "src/components/FilterBar/types";
import { SearchParamsKeys } from "src/constants/searchParams";

type Props = {
  readonly tasksData:
    | {
        readonly tasks: ReadonlyArray<TaskResponse>;
        readonly total_entries?: number;
      }
    | undefined;
};

const uniqueNonEmpty = (items: ReadonlyArray<string>): Array<string> =>
  [...new Set(items)].filter((valueItem) => valueItem !== "");

const isEmpty = (value: unknown): boolean =>
  value === null ||
  value === undefined ||
  (typeof value === "string" && value.trim() === "") ||
  (Array.isArray(value) && value.length === 0);

const arrayOrUndefined = (list: ReadonlyArray<string>): Array<string> | undefined =>
  list.length > 0 ? [...list] : undefined;

const hasOptions = (opts?: ReadonlyArray<unknown>): boolean => Array.isArray(opts) && opts.length > 0;

export const TaskFilters = ({ tasksData }: Props): JSX.Element => {
  const { t: translate } = useTranslation(["common", "dag"]);
  const [searchParams, setSearchParams] = useSearchParams();

  const { MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE } = SearchParamsKeys;

  // Clear any existing filters on first mount (fresh view after reload).
  // Add ?keep_filters=1 to the URL to preserve filters across reload.
  const clearedOnceRef = useRef(false);

  useEffect(() => {
    if (clearedOnceRef.current) {return;}
    clearedOnceRef.current = true;

    const preserve = searchParams.get("keep_filters");

    if (preserve === "1") {return;}

    const FILTER_PARAM_KEYS = [NAME_PATTERN, OPERATOR, TRIGGER_RULE, RETRIES, MAPPED, "page"] as const;
    const hasAny = FILTER_PARAM_KEYS.some((key) => searchParams.has(key));

    if (!hasAny) {return;}

    const next = new URLSearchParams(searchParams);

    FILTER_PARAM_KEYS.forEach((key) => next.delete(key));
    setSearchParams(next, { replace: true });
  }, [MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE, searchParams, setSearchParams]);

  const allTasks: ReadonlyArray<TaskResponse> = useMemo(() => tasksData?.tasks ?? [], [tasksData]);

  const operatorOptions = useMemo(
    () =>
      uniqueNonEmpty(
        allTasks
          .map((task) => task.operator_name)
          .filter((name): name is string => typeof name === "string" && name.trim() !== ""),
      )
        .map((value) => ({ label: value, value }))
        .sort((left, right) => left.label.localeCompare(right.label)),
    [allTasks],
  );

  const triggerRuleOptions = useMemo(
    () =>
      uniqueNonEmpty(
        allTasks
          .map((task) => task.trigger_rule)
          .filter((rule): rule is string => typeof rule === "string" && rule.trim() !== ""),
      )
        .map((value) => ({ label: value, value }))
        .sort((left, right) => left.label.localeCompare(right.label)),
    [allTasks],
  );

  const retryValueOptions = useMemo(
    () =>
      uniqueNonEmpty(
        allTasks
          .map((task) => (typeof task.retries === "number" ? String(task.retries) : ""))
          .filter((value) => value !== ""),
      )
        .map((value) => ({ label: value, value }))
        .sort((left, right) => left.label.localeCompare(right.label)),
    [allTasks],
  );

  const mappedOptions = useMemo(
    () => [
      { label: translate("common:mapped", { defaultValue: "Mapped" }), value: "true" },
      { label: translate("common:unmapped", { defaultValue: "Unmapped" }), value: "false" },
    ],
    [translate],
  );

  const configs: ReadonlyArray<FilterConfig> = useMemo(() => {
    const list: Array<FilterConfig | false> = [
      {
        hotkeyDisabled: true,
        key: NAME_PATTERN,
        label: translate("dag:tasks.searchLabel", { defaultValue: "Search tasks" }),
        placeholder: translate("dag:tasks.searchPlaceholder", { defaultValue: "Name contains…" }),
        type: "text",
      },
      hasOptions(operatorOptions) && {
        key: OPERATOR,
        label: translate("dag:tasks.operators", { defaultValue: "Operators" }),
        multiple: true,
        options: operatorOptions,
        type: "select",
      },
      hasOptions(triggerRuleOptions) && {
        key: TRIGGER_RULE,
        label: translate("dag:tasks.triggerRules", { defaultValue: "Trigger rules" }),
        multiple: true,
        options: triggerRuleOptions,
        type: "select",
      },
      hasOptions(retryValueOptions) && {
        key: RETRIES,
        label: translate("dag:tasks.retryValues", { defaultValue: "Retry values" }),
        multiple: true,
        options: retryValueOptions,
        type: "select",
      },
      {
        key: MAPPED,
        label: translate("dag:tasks.mapped", { defaultValue: "Mapped" }),
        multiple: false,
        options: mappedOptions,
        type: "select",
      },
    ];

    return list.filter(Boolean) as ReadonlyArray<FilterConfig>;
  }, [
    NAME_PATTERN,
    OPERATOR,
    TRIGGER_RULE,
    RETRIES,
    MAPPED,
    operatorOptions,
    triggerRuleOptions,
    retryValueOptions,
    mappedOptions,
    translate,
  ]);

  const initialValues = useMemo(
    () => ({
      [MAPPED]: (() => {
        const param = searchParams.get(MAPPED);

        return isEmpty(param) ? undefined : (param as string);
      })(),
      [NAME_PATTERN]: (() => {
        const param = searchParams.get(NAME_PATTERN);

        return isEmpty(param) ? undefined : (param as string);
      })(),
      [OPERATOR]: arrayOrUndefined(searchParams.getAll(OPERATOR)),
      [RETRIES]: arrayOrUndefined(searchParams.getAll(RETRIES)),
      [TRIGGER_RULE]: arrayOrUndefined(searchParams.getAll(TRIGGER_RULE)),
    }),
    [MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE, searchParams],
  );

  const onFiltersChange = (record: Record<string, unknown>): void => {
    const next = new URLSearchParams(searchParams);

    [NAME_PATTERN, OPERATOR, TRIGGER_RULE, RETRIES, MAPPED, "page"].forEach((key) => next.delete(key));

    const namePattern = typeof record[NAME_PATTERN] === "string" ? record[NAME_PATTERN] : "";

    if (!isEmpty(namePattern)) {next.set(NAME_PATTERN, String(namePattern));}

    const operatorValues = Array.isArray(record[OPERATOR]) ? (record[OPERATOR] as Array<string>) : [];

    operatorValues.forEach((value) => next.append(OPERATOR, value));

    const triggerValues = Array.isArray(record[TRIGGER_RULE]) ? (record[TRIGGER_RULE] as Array<string>) : [];

    triggerValues.forEach((value) => next.append(TRIGGER_RULE, value));

    const retryValues = Array.isArray(record[RETRIES]) ? (record[RETRIES] as Array<string>) : [];

    retryValues.forEach((value) => next.append(RETRIES, value));

    const mappedValue = typeof record[MAPPED] === "string" ? record[MAPPED] : "";

    if (!isEmpty(mappedValue)) {next.set(MAPPED, String(mappedValue));}

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
