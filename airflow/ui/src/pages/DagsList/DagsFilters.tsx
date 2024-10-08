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
import { HStack, Select, Text, Box } from "@chakra-ui/react";
import { Select as ReactSelect } from "chakra-react-select";
import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { QuickFilterButton } from "src/components/QuickFilterButton";

const STATE_PARAM = "last_dag_run_state";
import { searchParamsKeys } from "src/constants/searchParams";

export const DagsFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const { PAUSED: PAUSED_PARAM } = searchParamsKeys;
  const showPaused = searchParams.get(PAUSED_PARAM);
  const state = searchParams.get(STATE_PARAM);
  const isAll = state === null;
  const isRunning = state === "running";
  const isFailed = state === "failed";
  const isSuccess = state === "success";

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  const handlePausedChange: React.ChangeEventHandler<HTMLSelectElement> =
    useCallback(
      ({ target: { value } }) => {
        if (value === "All") {
          searchParams.delete(PAUSED_PARAM);
        } else {
          searchParams.set(PAUSED_PARAM, value);
        }
        setSearchParams(searchParams);
        setTableURLState({
          pagination: { ...pagination, pageIndex: 0 },
          sorting,
        });
      },
      [pagination, searchParams, setSearchParams, setTableURLState, sorting],
    );

  const handleStateChange: React.MouseEventHandler<HTMLButtonElement> =
    useCallback(
      ({ currentTarget: { value } }) => {
        if (value === "all") {
          searchParams.delete(STATE_PARAM);
        } else {
          searchParams.set(STATE_PARAM, value);
        }
        setSearchParams(searchParams);
        setTableURLState({
          pagination: { ...pagination, pageIndex: 0 },
          sorting,
        });
      },
      [pagination, searchParams, setSearchParams, setTableURLState, sorting],
    );

  return (
    <HStack justifyContent="space-between">
      <HStack spacing={4}>
        <Box>
          <Text fontSize="sm" fontWeight={200} mb={1}>
            State:
          </Text>
          <HStack>
            <QuickFilterButton
              isActive={isAll}
              onClick={handleStateChange}
              value="all"
            >
              All
            </QuickFilterButton>
            <QuickFilterButton
              isActive={isFailed}
              onClick={handleStateChange}
              value="failed"
            >
              Failed
            </QuickFilterButton>
            <QuickFilterButton
              isActive={isRunning}
              onClick={handleStateChange}
              value="running"
            >
              Running
            </QuickFilterButton>
            <QuickFilterButton
              isActive={isSuccess}
              onClick={handleStateChange}
              value="success"
            >
              Successful
            </QuickFilterButton>
          </HStack>
        </Box>
        <Box>
          <Text fontSize="sm" fontWeight={200} mb={1}>
            Active:
          </Text>
          <Select
            onChange={handlePausedChange}
            value={showPaused ?? undefined}
            variant="flushed"
          >
            <option>All</option>
            <option value="false">Enabled DAGs</option>
            <option value="true">Disabled DAGs</option>
          </Select>
        </Box>
      </HStack>
      <ReactSelect isDisabled placeholder="Filter by tag" />
    </HStack>
  );
};
