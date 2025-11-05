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
import { useDateRangeFilter, DATE_INPUT_FORMAT, TIME_INPUT_FORMAT, type ValidationError } from "src/hooks/useDateRangeFilter";

import { FilterPill } from "../FilterPill";
import type { DateRangeValue, FilterPluginProps } from "../types";
import { isValidDateValue, isValidFilterValue } from "../utils";
import { DateRangeCalendar } from "./DateRangeCalendar";

export const DateRangeFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const { t: translate } = useTranslation(["common", "components"]);
  const { selectedTimezone } = useTimezone();
  const value =
    filter.value !== null && filter.value !== undefined && typeof filter.value === "object"
      ? (filter.value as DateRangeValue)
      : { endDate: undefined, startDate: undefined };

  const hasValue = isValidFilterValue(filter.config.type, filter.value);

  const { editingState, endDateValue, formatDisplayValue, getFieldError, handleDateClick, handleInputChange, hasValidationErrors, setEditingState, startDateValue } =
    useDateRangeFilter({
      onChange,
      translate,
      value
    });

  const getBorderColor = (fieldName: ValidationError['field']) => {
    if (getFieldError(fieldName)) {
      return "danger.solid";
    }
    if (editingState.selectionTarget === fieldName) {
      return "brand.focusRing";
    }

    return "border";
  };

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
          <VStack gap={2} w="full">
            <VStack gap={1} w="full">
              <HStack justify="flex-start" w="full">
                <HStack gap={1}>
                  <MdAccessTime size={14} />
                  <Text color="fg.muted" fontSize="xs">
                    {selectedTimezone}
                  </Text>
                </HStack>
              </HStack>

              <HStack alignItems="flex-start" gap={2} w="full">
                <Box flex="1">
                  <Text color="fg.muted" fontSize="xs" mb={0.5}>
                    {translate("common:table.from")}
                  </Text>
                  <Box position="relative">
                    <Input
                      _focus={{ borderColor: "brand.focusRing" }}
                      borderColor={getBorderColor("start")}
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
                        onClick={(event) => {
                          event.stopPropagation();
                          onChange({ ...value, startDate: undefined });
                          setEditingState((prev) => ({
                            ...prev,
                            inputs: { ...prev.inputs, start: "", startTime: "" },
                            validationErrors: prev.validationErrors.filter(
                              (error) => error.field !== "start" && error.field !== "startTime" && error.field !== "range"
                            ),
                          }));
                        }}
                        position="absolute"
                        right={1}
                        size="2xs"
                        top="50%"
                        transform="translateY(-50%)"
                        variant="ghost"
                      >
                        <MdClose size={12} />
                      </IconButton>
                    )}
                  </Box>
                  <Box alignItems="flex-start" display="flex" minH="16px">
                    {getFieldError("start") && (
                      <Text color="danger.fg" fontSize="xs" mt={1}>
                        {getFieldError("start")?.message}
                      </Text>
                    )}
                  </Box>
                </Box>

                <Box flex="1">
                  <Text color="fg.muted" fontSize="xs" mb={0.5}>
                    {translate("common:table.to")}
                  </Text>
                  <Box position="relative">
                    <Input
                      _focus={{ borderColor: "brand.focusRing" }}
                      borderColor={getBorderColor("end")}
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
                        onClick={(event) => {
                          event.stopPropagation();
                          onChange({ ...value, endDate: undefined });
                          setEditingState((prev) => ({
                            ...prev,
                            inputs: { ...prev.inputs, end: "", endTime: "" },
                            validationErrors: prev.validationErrors.filter(
                              (error) => error.field !== "end" && error.field !== "endTime" && error.field !== "range"
                            ),
                          }));
                        }}
                        position="absolute"
                        right={1}
                        size="2xs"
                        top="50%"
                        transform="translateY(-50%)"
                        variant="ghost"
                      >
                        <MdClose size={12} />
                      </IconButton>
                    )}
                  </Box>
                  <Box alignItems="flex-start" display="flex" minH="16px">
                    {getFieldError("end") && (
                      <Text color="danger.fg" fontSize="xs" mt={1}>
                        {getFieldError("end")?.message}
                      </Text>
                    )}
                  </Box>
                </Box>
              </HStack>

              <HStack alignItems="flex-start" gap={2} w="full">
                <Box flex="1">
                  <Text color="fg.muted" fontSize="xs" mb={0.5}>
                    {translate("common:filters.startTime")}
                  </Text>
                  <Box position="relative">
                    <Input
                      borderColor={getBorderColor("startTime")}
                      fontSize="sm"
                      onChange={handleInputChange("start", "time")}
                      placeholder={TIME_INPUT_FORMAT}
                      value={editingState.inputs.startTime}
                      w="full"
                    />
                    {Boolean(editingState.inputs.startTime) && (
                      <IconButton
                        aria-label="Clear start time"
                        onClick={(event) => {
                          event.stopPropagation();
                          handleInputChange("start", "time")({ target: { value: "" } } as React.ChangeEvent<HTMLInputElement>);
                        }}
                        position="absolute"
                        right={1}
                        size="2xs"
                        top="50%"
                        transform="translateY(-50%)"
                        variant="ghost"
                      >
                        <MdClose size={12} />
                      </IconButton>
                    )}
                  </Box>
                  <Box alignItems="flex-start" display="flex" minH="16px">
                    {getFieldError("startTime") && (
                      <Text color="danger.fg" fontSize="xs" mt={1}>
                        {getFieldError("startTime")?.message}
                      </Text>
                    )}
                  </Box>
                </Box>

                <Box flex="1">
                  <Text color="fg.muted" fontSize="xs" mb={0.5}>
                    {translate("common:filters.endTime")}
                  </Text>
                  <Box position="relative">
                    <Input
                      borderColor={getBorderColor("endTime")}
                      fontSize="sm"
                      onChange={handleInputChange("end", "time")}
                      placeholder={TIME_INPUT_FORMAT}
                      value={editingState.inputs.endTime}
                      w="full"
                    />
                    {Boolean(editingState.inputs.endTime) && (
                      <IconButton
                        aria-label="Clear end time"
                        onClick={(event) => {
                          event.stopPropagation();
                          handleInputChange("end", "time")({ target: { value: "" } } as React.ChangeEvent<HTMLInputElement>);
                        }}
                        position="absolute"
                        right={1}
                        size="2xs"
                        top="50%"
                        transform="translateY(-50%)"
                        variant="ghost"
                      >
                        <MdClose size={12} />
                      </IconButton>
                    )}
                  </Box>
                  <Box alignItems="flex-start" display="flex" minH="16px">
                    {getFieldError("endTime") && (
                      <Text color="danger.fg" fontSize="xs" mt={1}>
                        {getFieldError("endTime")?.message}
                      </Text>
                    )}
                  </Box>
                </Box>
              </HStack>
              {getFieldError("range") && (
                <Text color="danger.fg" fontSize="xs" textAlign="center">
                  {getFieldError("range")?.message}
                </Text>
              )}
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
