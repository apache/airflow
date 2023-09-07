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

import { Box, Button, Flex, Input, Text } from "@chakra-ui/react";
import React from "react";

import { useTimezone } from "src/context/timezone";
import {
  isoFormatWithoutTZ,
  formatDuration,
  getDuration,
} from "src/datetime_utils";
import useFilters from "src/cluster-activity/useFilters";

const FilterBar = () => {
  const { filters, onStartDateChange, onEndDateChange, clearFilters } =
    useFilters();

  const { timezone } = useTimezone();
  // @ts-ignore
  const startDate = moment(filters.startDate);
  // @ts-ignore
  const endDate = moment(filters.endDate);
  // @ts-ignore
  const formattedStartDate = startDate.tz(timezone).format(isoFormatWithoutTZ);
  // @ts-ignore
  const formattedEndDate = endDate.tz(timezone).format(isoFormatWithoutTZ);

  const inputStyles = { backgroundColor: "white", size: "lg" };

  return (
    <Flex
      backgroundColor="blackAlpha.200"
      mb={4}
      px={4}
      py={5}
      justifyContent="space-between"
      flexWrap="wrap"
      gap={4}
    >
      <Flex justifyContent="space-between" flexWrap="wrap" gap={4}>
        <Box px={2}>
          <Text fontSize="sm" as="b" position="absolute" mt="-14px" ml={1}>
            Start Date
          </Text>
          <Input
            {...inputStyles}
            type="datetime-local"
            value={formattedStartDate || ""}
            onChange={(e) => onStartDateChange(e.target.value)}
          />
        </Box>
        <Box px={2}>
          <Text fontSize="sm" as="b" position="absolute" mt="-14px" ml={1}>
            End Date
          </Text>
          <Input
            {...inputStyles}
            type="datetime-local"
            value={formattedEndDate || ""}
            onChange={(e) => onEndDateChange(e.target.value)}
          />
        </Box>
        <Flex alignItems="center">
          <Text whiteSpace="nowrap">
            for a period of{" "}
            {formatDuration(getDuration(formattedStartDate, formattedEndDate))}
          </Text>
        </Flex>
      </Flex>
      <Flex px={2}>
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
      </Flex>
    </Flex>
  );
};

export default FilterBar;
