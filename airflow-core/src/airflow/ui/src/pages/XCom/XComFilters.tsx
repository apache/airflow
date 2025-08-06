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
import { Box, HStack, Text, VStack } from "@chakra-ui/react";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { DateTimeInput } from "src/components/DateTimeInput";
import { SearchBar } from "src/components/SearchBar";
import { SearchParamsKeys } from "src/constants/searchParams";

const FILTERS = [
  { hotkeyDisabled: false, key: SearchParamsKeys.KEY_PATTERN, translationKey: "keyFilter" },
  { hotkeyDisabled: true, key: SearchParamsKeys.DAG_ID_PATTERN, translationKey: "dagFilter" },
  { hotkeyDisabled: true, key: SearchParamsKeys.RUN_ID_PATTERN, translationKey: "runIdFilter" },
  { hotkeyDisabled: true, key: SearchParamsKeys.TASK_ID_PATTERN, translationKey: "taskIdFilter" },
] as const;

export const XComFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const { t: translate } = useTranslation(["browse"]);

  const createFilterHandler = useCallback(
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

  const createDateTimeFilterHandler = useCallback(
    (paramKey: string) => (event: React.ChangeEvent<HTMLInputElement>) => {
      const { value } = event.target;

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

  return (
    <VStack align="start" gap={4} paddingY="4px">
      <HStack flexWrap="wrap" gap={4}>
        {FILTERS.map(({ hotkeyDisabled, key, translationKey }) => (
          <Box key={key} minW="200px">
            <Text fontSize="xs" marginBottom={1}>
              &nbsp;
            </Text>
            <SearchBar
              defaultValue={searchParams.get(key) ?? ""}
              hideAdvanced
              hotkeyDisabled={hotkeyDisabled}
              onChange={createFilterHandler(key)}
              placeHolder={translate(`xcom.filters.${translationKey}`)}
            />
          </Box>
        ))}
        <Box minW="200px">
          <Text fontSize="xs" marginBottom={1}>
            {translate("xcom.filters.logicalDateFrom")}
          </Text>
          <DateTimeInput
            onChange={createDateTimeFilterHandler(SearchParamsKeys.LOGICAL_DATE_GTE as string)}
            value={searchParams.get(SearchParamsKeys.LOGICAL_DATE_GTE) ?? ""}
          />
        </Box>
        <Box minW="200px">
          <Text fontSize="xs" marginBottom={1}>
            {translate("xcom.filters.logicalDateTo")}
          </Text>
          <DateTimeInput
            onChange={createDateTimeFilterHandler(SearchParamsKeys.LOGICAL_DATE_LTE as string)}
            value={searchParams.get(SearchParamsKeys.LOGICAL_DATE_LTE) ?? ""}
          />
        </Box>
        <Box minW="200px">
          <Text fontSize="xs" marginBottom={1}>
            {translate("xcom.filters.runAfterFrom")}
          </Text>
          <DateTimeInput
            onChange={createDateTimeFilterHandler(SearchParamsKeys.RUN_AFTER_GTE as string)}
            value={searchParams.get(SearchParamsKeys.RUN_AFTER_GTE) ?? ""}
          />
        </Box>
        <Box minW="200px">
          <Text fontSize="xs" marginBottom={1}>
            {translate("xcom.filters.runAfterTo")}
          </Text>
          <DateTimeInput
            onChange={createDateTimeFilterHandler(SearchParamsKeys.RUN_AFTER_LTE as string)}
            value={searchParams.get(SearchParamsKeys.RUN_AFTER_LTE) ?? ""}
          />
        </Box>
      </HStack>
    </VStack>
  );
};
