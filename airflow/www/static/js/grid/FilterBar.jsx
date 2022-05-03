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

/* global filtersOptions */

import {
  Box,
  Button,
  Flex, Input, Select,
} from '@chakra-ui/react';
import React from 'react';

import useFilters from './utils/useFilters';

const FilterBar = () => {
  const {
    filters,
    onBaseDateChange,
    onNumRunsChange,
    onRunTypeChange,
    onRunStateChange,
    onTaskStateChange,
    clearFilters,
  } = useFilters();

  const inputStyles = { backgroundColor: 'white', size: 'lg' };

  return (
    <Flex backgroundColor="gray.100" mt={0} mb={2} p={4}>
      <Box px={2}>
        <Input
          {...inputStyles}
          type="datetime-local"
          value={filters.baseDate || ''}
          onChange={onBaseDateChange}
        />
      </Box>
      <Box px={2}>
        <Select
          {...inputStyles}
          placeholder="Runs"
          value={filters.numRuns || ''}
          onChange={onNumRunsChange}
        >
          {filtersOptions.numRuns.map((value) => (
            <option value={value} key={value}>{value}</option>
          ))}
        </Select>
      </Box>
      <Box px={2}>
        <Select
          {...inputStyles}
          placeholder="Dag Run Type"
          value={filters.runType || ''}
          onChange={onRunTypeChange}
        >
          {filtersOptions.runTypes.map((value) => (
            <option value={value} key={value}>{value}</option>
          ))}
        </Select>
      </Box>
      <Box />
      <Box px={2}>
        <Select
          {...inputStyles}
          placeholder="Run State"
          value={filters.runState || ''}
          onChange={onRunStateChange}
        >
          {filtersOptions.dagStates.map((value) => (
            <option value={value} key={value}>{value}</option>
          ))}
        </Select>
      </Box>
      <Box px={2}>
        <Select
          {...inputStyles}
          placeholder="Task State"
          value={filters.taskState || ''}
          onChange={onTaskStateChange}
        >
          {filtersOptions.taskStates.map((value) => (
            <option value={value} key={value}>{value}</option>
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
  );
};

export default FilterBar;
