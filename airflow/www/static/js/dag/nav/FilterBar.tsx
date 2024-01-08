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
import MultiSelect from "src/components/MultiSelect";
import React from "react";
import type { DagRun, RunState, TaskState } from "src/types";
import AutoRefresh from "src/components/AutoRefresh";
import type { Size } from "chakra-react-select";
import { useChakraSelectProps } from "chakra-react-select";

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
    transformArrayToMultiSelectOptions,
  } = useFilters();

  const { timezone } = useTimezone();
  // @ts-ignore
  const time = moment(filters.baseDate);
  // @ts-ignore
  const formattedTime = time.tz(timezone).format(isoFormatWithoutTZ);

  const inputStyles: { backgroundColor: string; size: Size } = {
    backgroundColor: "white",
    size: "lg",
  };

  const multiSelectBoxStyle = { minWidth: "160px", zIndex: 3 };
  const multiSelectStyles = useChakraSelectProps({
    ...inputStyles,
    isMulti: true,
    tagVariant: "solid",
    hideSelectedOptions: false,
    isClearable: false,
    selectedOptionStyle: "check",
    chakraStyles: {
      container: (provided) => ({
        ...provided,
        bg: "white",
      }),
    },
  });

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
        <Box px={2} style={multiSelectBoxStyle}>
          <MultiSelect
            {...multiSelectStyles}
            value={transformArrayToMultiSelectOptions(filters.runType)}
            onChange={(typeOptions) => {
              if (
                Array.isArray(typeOptions) &&
                typeOptions.every((typeOption) => "value" in typeOption)
              ) {
                onRunTypeChange(
                  typeOptions.map((typeOption) => typeOption.value)
                );
              }
            }}
            options={transformArrayToMultiSelectOptions(filters.runTypeOptions)}
            placeholder="All Run Types"
          />
        </Box>
        <Box />
        <Box px={2} style={multiSelectBoxStyle}>
          <MultiSelect
            {...multiSelectStyles}
            value={transformArrayToMultiSelectOptions(filters.runState)}
            onChange={(stateOptions) => {
              if (
                Array.isArray(stateOptions) &&
                stateOptions.every((stateOption) => "value" in stateOption)
              ) {
                onRunStateChange(
                  stateOptions.map((stateOption) => stateOption.value)
                );
              }
            }}
            options={transformArrayToMultiSelectOptions(
              filters.runStateOptions
            )}
            placeholder="All Run States"
          />
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
