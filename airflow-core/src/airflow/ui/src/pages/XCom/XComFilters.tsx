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
import { Box, HStack } from "@chakra-ui/react";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
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

  return (
    <HStack flexWrap="wrap" gap={4} paddingY="4px">
      {FILTERS.map(({ hotkeyDisabled, key, translationKey }) => (
        <Box key={key} minW="200px">
          <SearchBar
            defaultValue={searchParams.get(key) ?? ""}
            hideAdvanced
            hotkeyDisabled={hotkeyDisabled}
            onChange={createFilterHandler(key)}
            placeHolder={translate(`xcom.filters.${translationKey}`)}
          />
        </Box>
      ))}
    </HStack>
  );
};
