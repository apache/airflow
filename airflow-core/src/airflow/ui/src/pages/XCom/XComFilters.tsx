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
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { DateTimeInput } from "src/components/DateTimeInput";
import { SearchBar } from "src/components/SearchBar";
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
] as const;

export const XComFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const { t: translate } = useTranslation(["browse", "common"]);
  const [resetKey, setResetKey] = useState(0);

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
      FILTERS.filter((filter) => {
        const value = searchParams.get(filter.key);

        return value !== null && value !== "";
      }).length,
    [searchParams],
  );

  const handleResetFilters = useCallback(() => {
    FILTERS.forEach((filter) => {
      searchParams.delete(filter.key);
    });
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setSearchParams(searchParams);
    setResetKey((prev) => prev + 1);
  }, [pagination, searchParams, setSearchParams, setTableURLState, sorting]);

  const renderFilterInput = (filter: (typeof FILTERS)[number]) => {
    const { key, translationKey, type } = filter;

    return (
      <Box key={key} w="200px">
        <Text fontSize="xs" marginBottom={1}>
          {type === "search" ? "\u00A0" : translate(`common:filters.${translationKey}`)}
        </Text>
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
        ) : (
          <DateTimeInput
            key={`${key}-${resetKey}`}
            onChange={(event) => handleFilterChange(key)(event.target.value)}
            value={searchParams.get(key) ?? ""}
          />
        )}
      </Box>
    );
  };

  return (
    <VStack align="start" gap={4} paddingY="4px">
      <HStack flexWrap="wrap" gap={4}>
        {FILTERS.map(renderFilterInput)}
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
