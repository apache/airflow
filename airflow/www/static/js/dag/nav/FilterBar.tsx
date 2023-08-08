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

/* global moment */

import { Box, Button, Flex, Input, Select } from "@chakra-ui/react";
import React from "react";
import type { DagRun, RunState, TaskState } from "src/types";
import AutoRefresh from "src/components/AutoRefresh";

import { useTimezone } from "src/context/timezone";
import { isoFormatWithoutTZ } from "src/datetime_utils";
import useFilters from "src/dag/useFilters";

declare const filtersOptions: {
  dagStates: RunState[];
  numRuns: number[];
  runTypes: DagRun["runType"][];
  taskStates: TaskState[];
};

const FilterBar = () => {
  const {
    filters,
    onBaseDateChange,
    onNumRunsChange,
    onRunTypeChange,
    onRunStateChange,
    clearFilters,
  } = useFilters();

  const { timezone } = useTimezone();
  // @ts-ignore
  const time = moment(filters.baseDate);
  // @ts-ignore
  const formattedTime = time.tz(timezone).format(isoFormatWithoutTZ);

  const inputStyles = { backgroundColor: "white", size: "lg" };

  return (
    <Flex
      backgroundColor="blackAlpha.200"
      mt={4}
      p={4}
      justifyContent="space-between"
    >
      <Flex>
        <Box px={2}>
          <Input
            {...inputStyles}
            type="datetime-local"
            value={formattedTime || ""}
            onChange={(e) => onBaseDateChange(e.target.value)}
          />
        </Box>
        <Box px={2}>
          <Select
            {...inputStyles}
            placeholder="Runs"
            value={filters.numRuns || ""}
            onChange={(e) => onNumRunsChange(e.target.value)}
          >
            {filtersOptions.numRuns.map((value) => (
              <option value={value} key={value}>
                {value}
              </option>
            ))}
          </Select>
        </Box>
        <Box px={2}>
          <Select
            {...inputStyles}
            value={filters.runType || ""}
            onChange={(e) => onRunTypeChange(e.target.value)}
          >
            <option value="" key="all">
              All Run Types
            </option>
            {filtersOptions.runTypes.map((value) => (
              <option value={value.toString()} key={value}>
                {value}
              </option>
            ))}
          </Select>
        </Box>
        <Box />
        <Box px={2}>
          <Select
            {...inputStyles}
            value={filters.runState || ""}
            onChange={(e) => onRunStateChange(e.target.value)}
          >
            <option value="" key="all">
              All Run States
            </option>
            {filtersOptions.dagStates.map((value) => (
              <option value={value} key={value}>
                {value}
              </option>
            ))}
          </Select>
        </Box>
        <Box px={2}>
          <Button
            colorScheme="cyan"
            aria-label="Reset filters"
            background="white"
            variant="outline"
            onClick={clearFilters}
            size="lg"
          >
            Clear Filters
          </Button>
        </Box>
      </Flex>
      <AutoRefresh />
    </Flex>
  );
};

export default FilterBar;
