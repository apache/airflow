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
import { Box, Button, HStack, Text, VStack } from "@chakra-ui/react";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { LuX } from "react-icons/lu";
import { useSearchParams, useParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { DateTimeInput } from "src/components/DateTimeInput";
import { SearchBar } from "src/components/SearchBar";
import { NumberInputField, NumberInputRoot } from "src/components/ui/NumberInput";
import { SearchParamsKeys } from "src/constants/searchParams";

const FILTERS = [
  {
    hotkeyDisabled: false,
    key: SearchParamsKeys.KEY_PATTERN,
    translationKey: "keyPlaceholder",
    type: "search",
  },
  {
    hotkeyDisabled: true,
    key: SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN,
    translationKey: "dagDisplayNamePlaceholder",
    type: "search",
  },
  {
    hotkeyDisabled: true,
    key: SearchParamsKeys.RUN_ID_PATTERN,
    translationKey: "runIdPlaceholder",
    type: "search",
  },
  {
    hotkeyDisabled: true,
    key: SearchParamsKeys.TASK_ID_PATTERN,
    translationKey: "taskIdPlaceholder",
    type: "search",
  },
  {
    hotkeyDisabled: true,
    key: SearchParamsKeys.MAP_INDEX,
    translationKey: "mapIndexPlaceholder",
    type: "number",
  },
  {
    key: SearchParamsKeys.LOGICAL_DATE_GTE,
    translationKey: "logicalDateFromPlaceholder",
    type: "datetime",
  },
  {
    key: SearchParamsKeys.LOGICAL_DATE_LTE,
    translationKey: "logicalDateToPlaceholder",
    type: "datetime",
  },
  {
    key: SearchParamsKeys.RUN_AFTER_GTE,
    translationKey: "runAfterFromPlaceholder",
    type: "datetime",
  },
  {
    key: SearchParamsKeys.RUN_AFTER_LTE,
    translationKey: "runAfterToPlaceholder",
    type: "datetime",
  },
] as const satisfies ReadonlyArray<{
  readonly hotkeyDisabled?: boolean;
  readonly key: string;
  readonly translationKey: string;
  readonly type: "datetime" | "number" | "search";
}>;

export const XComFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { dagId = "~", mapIndex = "-1", runId = "~", taskId = "~" } = useParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const { t: translate } = useTranslation(["browse", "common"]);
  const [resetKey, setResetKey] = useState(0);

  const visibleFilters = useMemo(
    () =>
      FILTERS.filter((filter) => {
        switch (filter.key) {
          case SearchParamsKeys.DAG_DISPLAY_NAME_PATTERN:
            return dagId === "~";
          case SearchParamsKeys.KEY_PATTERN:
          case SearchParamsKeys.LOGICAL_DATE_GTE:
          case SearchParamsKeys.LOGICAL_DATE_LTE:
          case SearchParamsKeys.RUN_AFTER_GTE:
          case SearchParamsKeys.RUN_AFTER_LTE:
            return true;
          case SearchParamsKeys.MAP_INDEX:
            return mapIndex === "-1";
          case SearchParamsKeys.RUN_ID_PATTERN:
            return runId === "~";
          case SearchParamsKeys.TASK_ID_PATTERN:
            return taskId === "~";
          default:
            return true;
        }
      }),
    [dagId, mapIndex, runId, taskId],
  );

  const handleFilterChange = useCallback(
    (paramKey: string) => (value: string) => {
      if (value === "") {
        searchParams.delete(paramKey);
      } else {
        searchParams.set(paramKey, value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const filterCount = useMemo(
    () =>
      visibleFilters.filter((filter) => {
        const value = searchParams.get(filter.key);

        return value !== null && value !== "";
      }).length,
    [searchParams, visibleFilters],
  );

  const handleResetFilters = useCallback(() => {
    visibleFilters.forEach((filter) => {
      searchParams.delete(filter.key);
    });
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setSearchParams(searchParams);
    setResetKey((prev) => prev + 1);
  }, [pagination, searchParams, setSearchParams, setTableURLState, sorting, visibleFilters]);

  const renderFilterInput = (filter: (typeof FILTERS)[number]) => {
    const { key, translationKey, type } = filter;

    return (
      <Box key={key} w="200px">
        <Box marginBottom={1} minHeight="1.2em">
          {type !== "search" && <Text fontSize="xs">{translate(`common:filters.${translationKey}`)}</Text>}
        </Box>
        {type === "search" ? (
          (() => {
            const { hotkeyDisabled } = filter;

            return (
              <SearchBar
                defaultValue={searchParams.get(key) ?? ""}
                hideAdvanced
                hotkeyDisabled={hotkeyDisabled}
                key={`${key}-${resetKey}`}
                onChange={handleFilterChange(key)}
                placeHolder={translate(`common:filters.${translationKey}`)}
              />
            );
          })()
        ) : type === "datetime" ? (
          <DateTimeInput
            key={`${key}-${resetKey}`}
            onChange={(event) => handleFilterChange(key)(event.target.value)}
            value={searchParams.get(key) ?? ""}
          />
        ) : (
          <NumberInputRoot
            key={`${key}-${resetKey}`}
            min={-1}
            onValueChange={(details) => handleFilterChange(key)(details.value)}
            value={searchParams.get(key) ?? ""}
          >
            <NumberInputField placeholder={translate(`common:filters.${translationKey}`)} />
          </NumberInputRoot>
        )}
      </Box>
    );
  };

  return (
    <VStack align="start" gap={4} paddingY="4px">
      <HStack flexWrap="wrap" gap={4}>
        {visibleFilters.map(renderFilterInput)}
        <Box>
          <Text fontSize="xs" marginBottom={1}>
            &nbsp;
          </Text>
          {filterCount > 0 && (
            <Button onClick={handleResetFilters} size="md" variant="outline">
              <LuX />
              {translate("common:table.filterReset", { count: filterCount })}
            </Button>
          )}
        </Box>
      </HStack>
    </VStack>
  );
};
