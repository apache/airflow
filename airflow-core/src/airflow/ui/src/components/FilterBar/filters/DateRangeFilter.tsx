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
import { Box, HStack, IconButton, Input, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { MdAccessTime, MdCalendarToday, MdClose } from "react-icons/md";

import { Popover } from "src/components/ui";
import { useTimezone } from "src/context/timezone";
import { useDateRangeFilter, DATE_INPUT_FORMAT, TIME_INPUT_FORMAT } from "src/hooks/useDateRangeFilter";

import { FilterPill } from "../FilterPill";
import type { DateRangeValue, FilterPluginProps } from "../types";
import { isValidDateValue, isValidFilterValue } from "../utils";
import { DateRangeCalendar } from "./DateRangeCalendar";

export const DateRangeFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const { t: translate } = useTranslation(["common"]);
  const { selectedTimezone } = useTimezone();
  const value =
    filter.value !== null && filter.value !== undefined && typeof filter.value === "object"
      ? (filter.value as DateRangeValue)
      : { endDate: undefined, startDate: undefined };

  const startDateValue = isValidDateValue(value.startDate) ? dayjs(value.startDate) : undefined;
  const endDateValue = isValidDateValue(value.endDate) ? dayjs(value.endDate) : undefined;
  const hasValue = isValidFilterValue(filter.config.type, filter.value);

  const { editingState, formatDisplayValue, handleDateClick, handleInputChange, setEditingState } =
    useDateRangeFilter({
      onChange,
      value,
      translate
    });

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
        <Popover.Content p={3} w="320px">
          <VStack gap={4} w="full">
            <VStack gap={3} w="full">
              <HStack justify="flex-start" w="full">
                <HStack gap={1}>
                  <MdAccessTime size={14} />
                  <Text color="fg.muted" fontSize="xs">
                    {selectedTimezone}
                  </Text>
                </HStack>
              </HStack>

              <HStack gap={2} w="full">
                <Box flex="1" position="relative">
                  <Text color="fg.muted" fontSize="xs" mb={0.5}>
                    {translate("common:table.from")}
                  </Text>
                  <Input
                    _focus={{ borderColor: "brand.focusRing" }}
                    borderColor={editingState.selectionTarget === "start" ? "brand.focusRing" : "border"}
                    fontSize="sm"
                    fontWeight="medium"
                    onBlur={() => {
                      if (
                        startDateValue &&
                        editingState.inputs.start &&
                        !dayjs(editingState.inputs.start, DATE_INPUT_FORMAT, true).isValid()
                      ) {
                        setEditingState((prev) => ({
                          ...prev,
                          inputs: { ...prev.inputs, start: startDateValue.format(DATE_INPUT_FORMAT) },
                        }));
                      }
                    }}
                    onChange={handleInputChange("start", "date")}
                    onFocus={() => setEditingState((prev) => ({ ...prev, selectionTarget: "start" }))}
                    placeholder={DATE_INPUT_FORMAT}
                    value={editingState.inputs.start}
                  />
                  {Boolean(editingState.inputs.start) && (
                    <IconButton
                      aria-label="Clear start date"
                      onClick={() => onChange({ ...value, startDate: undefined })}
                      position="absolute"
                      right={1}
                      size="2xs"
                      top="50%"
                      variant="ghost"
                    >
                      <MdClose size={12} />
                    </IconButton>
                  )}
                </Box>

                <Box flex="1" position="relative">
                  <Text color="fg.muted" fontSize="xs" mb={0.5}>
                    {translate("common:table.to")}
                  </Text>
                  <Input
                    _focus={{ borderColor: "brand.focusRing" }}
                    borderColor={editingState.selectionTarget === "end" ? "brand.focusRing" : "border"}
                    fontSize="sm"
                    fontWeight="medium"
                    onBlur={() => {
                      if (
                        endDateValue &&
                        editingState.inputs.end &&
                        !dayjs(editingState.inputs.end, DATE_INPUT_FORMAT, true).isValid()
                      ) {
                        setEditingState((prev) => ({
                          ...prev,
                          inputs: { ...prev.inputs, end: endDateValue.format(DATE_INPUT_FORMAT) },
                        }));
                      }
                    }}
                    onChange={handleInputChange("end", "date")}
                    onFocus={() => setEditingState((prev) => ({ ...prev, selectionTarget: "end" }))}
                    placeholder={DATE_INPUT_FORMAT}
                    value={editingState.inputs.end}
                  />
                  {Boolean(editingState.inputs.end) && (
                    <IconButton
                      aria-label="Clear end date"
                      onClick={() => onChange({ ...value, endDate: undefined })}
                      position="absolute"
                      right={1}
                      size="2xs"
                      top="50%"
                      variant="ghost"
                    >
                      <MdClose size={12} />
                    </IconButton>
                  )}
                </Box>
              </HStack>

              <HStack gap={2} w="full">
                <Box flex="1">
                  <Text color="fg.muted" fontSize="xs" mb={0.5}>
                    {translate("common:filters.startTime")}
                  </Text>
                  <Input
                    fontSize="sm"
                    onChange={handleInputChange("start", "time")}
                    placeholder={TIME_INPUT_FORMAT}
                    value={editingState.inputs.startTime}
                    w="full"
                  />
                </Box>

                <Box flex="1">
                  <Text color="fg.muted" fontSize="xs" mb={0.5}>
                    {translate("common:filters.endTime")}
                  </Text>
                  <Input
                    fontSize="sm"
                    onChange={handleInputChange("end", "time")}
                    placeholder={TIME_INPUT_FORMAT}
                    value={editingState.inputs.endTime}
                    w="full"
                  />
                </Box>
              </HStack>
            </VStack>
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
