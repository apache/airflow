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
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { MdCalendarToday, MdClose } from "react-icons/md";

import { Popover } from "src/components/ui";
import { useDateRangeFilter } from "src/hooks/useDateRangeFilter";

import { FilterPill } from "../FilterPill";
import type { DateRangeValue, FilterPluginProps } from "../types";
import { isValidDateValue, isValidFilterValue } from "../utils";
import { DateRangeCalendar } from "./DateRangeCalendar";
import { DateRangeInputs } from "./DateRangeInputs";

export const DateRangeFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const { t: translate } = useTranslation(["common"]);
  const value =
    filter.value !== null && filter.value !== undefined && typeof filter.value === "object"
      ? (filter.value as DateRangeValue)
      : { endDate: undefined, startDate: undefined };

  const startDateValue = isValidDateValue(value.startDate) ? dayjs(value.startDate) : undefined;
  const endDateValue = isValidDateValue(value.endDate) ? dayjs(value.endDate) : undefined;
  const hasValue = isValidFilterValue(filter.config.type, filter.value);

  const { editingState, formatDisplayValue, handleDateClick, handleInputChange, setEditingState } =
    useDateRangeFilter({ onChange, value });

  return (
    <FilterPill
      displayValue={formatDisplayValue()}
      filter={filter}
      hasValue={hasValue}
      onChange={onChange}
      onRemove={onRemove}
    >
      <Popover.Root defaultOpen={!hasValue} key={filter.id} lazyMount unmountOnExit>
        <Popover.Trigger asChild>
          <Box
            alignItems="center"
            bg={hasValue ? "blue.muted" : "gray.muted"}
            border="0.5px solid"
            borderColor="border"
            borderRadius="full"
            color="colorPalette.fg"
            colorPalette={hasValue ? "blue" : "gray"}
            cursor="pointer"
            display="flex"
            h="10"
            overflow="hidden"
            width="360px"
          >
            <HStack
              alignItems="center"
              bg={hasValue ? "blue.muted" : "gray.muted"}
              borderLeftRadius="full"
              fontSize="sm"
              fontWeight="medium"
              gap={2}
              h="full"
              px={3}
              py={1.5}
              whiteSpace="nowrap"
            >
              <MdCalendarToday />
              <Text>{filter.config.label}:</Text>
            </HStack>
            <HStack flex="1" gap={2} px={2.5} py={0.5}>
              <Text color={hasValue ? "inherit" : "gray.500"} fontSize="sm">
                {hasValue ? formatDisplayValue() : translate("common:filters.selectDateRange")}
              </Text>
              <Box
                _hover={{
                  bg: "gray.100",
                  color: "gray.600",
                }}
                alignItems="center"
                aria-label={`Remove ${filter.config.label} filter`}
                bg="transparent"
                borderRadius="full"
                color="gray.400"
                cursor="pointer"
                display="flex"
                h={6}
                justifyContent="center"
                ml="auto"
                onClick={(event) => {
                  event.stopPropagation();
                  onRemove();
                }}
                transition="all 0.2s"
                w={6}
              >
                <MdClose size={16} />
              </Box>
            </HStack>
          </Box>
        </Popover.Trigger>
        <Popover.Content p={3} w="300px">
          <VStack gap={4} w="full">
            <DateRangeInputs
              editingState={editingState}
              endDateValue={endDateValue}
              onChange={handleInputChange}
              onClearEnd={() => onChange({ ...value, endDate: undefined })}
              onClearStart={() => onChange({ ...value, startDate: undefined })}
              setEditingState={setEditingState}
              setSelectionTarget={(target) =>
                setEditingState((prev) => ({ ...prev, selectionTarget: target }))
              }
              startDateValue={startDateValue}
            />
            <DateRangeCalendar
              currentMonth={editingState.currentMonth}
              onDateClick={handleDateClick}
              onMonthChange={(month) => setEditingState((prev) => ({ ...prev, currentMonth: month }))}
              value={value}
            />
          </VStack>
        </Popover.Content>
      </Popover.Root>
    </FilterPill>
  );
};
