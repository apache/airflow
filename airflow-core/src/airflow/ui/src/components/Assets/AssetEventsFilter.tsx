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
import { VStack, HStack, Box, Text, Button } from "@chakra-ui/react";
import { useCallback, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { LuX } from "react-icons/lu";
import { useSearchParams } from "react-router-dom";

import { DateTimeInput } from "src/components/DateTimeInput";
import { SearchBar } from "src/components/SearchBar";
import { SearchParamsKeys } from "src/constants/searchParams";

const { DAG_ID, END_DATE, START_DATE, TASK_ID } = SearchParamsKeys;
const filterKeys = [START_DATE, END_DATE, DAG_ID, TASK_ID];

export const AssetEventsFilter = () => {
  const { t: translate } = useTranslation("common");
  const [searchParams, setSearchParams] = useSearchParams();
  const startDate = searchParams.get(START_DATE) ?? "";
  const endDate = searchParams.get(END_DATE) ?? "";
  const dagId = searchParams.get(DAG_ID) ?? "";
  const taskId = searchParams.get(TASK_ID) ?? "";
  const [resetKey, setResetKey] = useState(0);
  const handleFilterChange = useCallback(
    (paramKey: string) => (value: string) => {
      if (value === "") {
        searchParams.delete(paramKey);
      } else {
        searchParams.set(paramKey, value);
      }
      setSearchParams(searchParams);
    },
    [searchParams, setSearchParams],
  );
  const filterCount = useMemo(
    () => filterKeys.reduce((acc, key) => (searchParams.get(key) === null ? acc : acc + 1), 0),
    [searchParams],
  );
  const handleResetFilters = useCallback(() => {
    filterKeys.forEach((key) => searchParams.delete(key));
    setSearchParams(searchParams);
    setResetKey((prev) => prev + 1);
  }, [searchParams, setSearchParams]);

  return (
    <VStack align="start" gap={4} paddingY="4px">
      <HStack flexWrap="wrap" gap={4}>
        <Box w="200px">
          <Text fontSize="xs">{translate("common:table.from")}</Text>
          <DateTimeInput
            onChange={(event) => handleFilterChange(START_DATE)(event.target.value)}
            value={startDate}
          />
        </Box>
        <Box w="200px">
          <Text fontSize="xs">{translate("common:table.to")}</Text>
          <DateTimeInput
            onChange={(event) => handleFilterChange(END_DATE)(event.target.value)}
            value={endDate}
          />
        </Box>
        <Box w="200px">
          <Text fontSize="xs">{translate("common:filters.dagDisplayNamePlaceholder")}</Text>
          <SearchBar
            defaultValue={dagId}
            hideAdvanced
            hotkeyDisabled={true}
            key={`dag-id-${resetKey}`}
            onChange={handleFilterChange(DAG_ID)}
            placeHolder={translate("common:filters.dagDisplayNamePlaceholder")}
          />
        </Box>
        <Box w="200px">
          <Text fontSize="xs">{translate("common:filters.taskIdPlaceholder")}</Text>
          <SearchBar
            defaultValue={taskId}
            hideAdvanced
            hotkeyDisabled={true}
            key={`task-id-${resetKey}`}
            onChange={handleFilterChange(TASK_ID)}
            placeHolder={translate("common:filters.taskIdPlaceholder")}
          />
        </Box>
        <Box alignSelf="end">
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
