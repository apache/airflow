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

import { Box, Button, Flex, Input } from "@chakra-ui/react";
import React from "react";

import { useTimezone } from "src/context/timezone";
import { isoFormatWithoutTZ } from "src/datetime_utils";
import useFilters from "src/dashboard/useFilters";

const FilterBar = () => {
  const { filters, onStartDateChange, onEndDateChange, clearFilters } =
    useFilters();

  const { timezone } = useTimezone();
  const startDate = moment(filters.startDate);
  const endDate = moment(filters.endDate);
  const formattedStartDate = startDate.tz(timezone).format(isoFormatWithoutTZ);
  const formattedEndDate = endDate.tz(timezone).format(isoFormatWithoutTZ);

  const inputStyles = { backgroundColor: "white", size: "lg" };

  return (
    <Flex backgroundColor="#f0f0f0" mb={4} p={4} justifyContent="space-between">
      <Flex justifyContent="space-between">
        <Box px={2}>
          <Input
            {...inputStyles}
            type="datetime-local"
            value={formattedStartDate || ""}
            onChange={(e) => onStartDateChange(e.target.value)}
          />
        </Box>
        <Box px={2}>
          <Input
            {...inputStyles}
            type="datetime-local"
            value={formattedEndDate || ""}
            onChange={(e) => onEndDateChange(e.target.value)}
          />
        </Box>
      </Flex>
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
