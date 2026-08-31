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
import { Box, HStack, Text, VStack, type InputProps } from "@chakra-ui/react";
import dayjs, { type Dayjs } from "dayjs";
import timezone from "dayjs/plugin/timezone";
import { forwardRef, useEffect, useState, type ChangeEvent, type HTMLAttributes } from "react";
import { useTranslation } from "react-i18next";
import { MdAccessTime, MdCalendarToday } from "react-icons/md";

import { DateInput } from "src/components/FilterBar/filters/DateInput";
import { DateRangeCalendar } from "src/components/FilterBar/filters/DateRangeCalendar";
import { isValidDateValue } from "src/components/FilterBar/utils";
import { useTimezone } from "src/context/timezone";
import type { ValidationError } from "src/hooks/useDateRangeFilter";
import {
  combineDateAndTime,
  DATE_INPUT_FORMAT,
  TIME_INPUT_FORMAT,
  validateDateInput,
  validateTimeInput,
} from "src/hooks/useDateRangeFilter";
import { Popover } from "src/system-components";

dayjs.extend(timezone);

// A single datetime picker built exactly like the date-range filter: a trigger showing the selected
// value opens a popover with the selected timezone, a text date input, a text time input, and a
// calendar. Text inputs (not a native `datetime-local`) keep it consistent with the range picker and
// avoid the Firefox/Safari problem where picking a date without a time yields no value (#54429).
type Props = {
  // Default the time to the end of the day (instead of the start) when only a date is entered — used
  // for a range's upper bound so it stays inclusive.
  readonly endOfDay?: boolean;
  readonly value: string;
} & Omit<InputProps, "onBlur" | "onFocus" | "onKeyDown"> &
  Pick<HTMLAttributes<HTMLDivElement>, "onBlur" | "onFocus" | "onKeyDown">;

const DISPLAY_FORMAT = "MMM DD, YYYY HH:mm";

const splitValue = (value: string, tz: string) => {
  const parsed = isValidDateValue(value) ? dayjs(value).tz(tz) : undefined;

  return {
    date: parsed?.format(DATE_INPUT_FORMAT) ?? "",
    time: parsed?.format(TIME_INPUT_FORMAT) ?? "",
  };
};

export const DateTimeInput = forwardRef<HTMLDivElement, Props>(
  ({ disabled, endOfDay = false, onBlur, onChange, onFocus, onKeyDown, value }, ref) => {
    const { t: translate } = useTranslation(["components", "common"]);
    const { selectedTimezone } = useTimezone();
    const selected = isValidDateValue(value) ? dayjs(value).tz(selectedTimezone) : undefined;

    const [inputs, setInputs] = useState(() => splitValue(value, selectedTimezone));
    const [currentMonth, setCurrentMonth] = useState<Dayjs>(() => selected ?? dayjs());

    // Reflect external value changes (form reset, calendar/input edits) without clobbering
    // in-progress typing: an incomplete date never emits, so `value` stays put and this leaves it be.
    useEffect(() => {
      setInputs(splitValue(value, selectedTimezone));
    }, [value, selectedTimezone]);

    const emit = (emitted: string) => {
      onChange?.({ target: { value: emitted } } as ChangeEvent<HTMLInputElement>);
    };

    const commit = (next: { date: string; time: string }) => {
      if (next.date === "") {
        emit("");

        return;
      }
      if (validateDateInput(next.date) && (next.time === "" || validateTimeInput(next.time))) {
        const combined = combineDateAndTime(next.date, next.time, { endOfDay, timezone: selectedTimezone });

        if (combined !== "") {
          emit(combined);
        }
      }
    };

    const applyChange = (inputType: "date" | "time", nextValue: string) => {
      const next = inputType === "date" ? { ...inputs, date: nextValue } : { ...inputs, time: nextValue };

      setInputs(next);
      commit(next);
    };

    const handleDateClick = (day: Dayjs) => {
      const next = { ...inputs, date: day.format(DATE_INPUT_FORMAT) };

      setInputs(next);
      setCurrentMonth(day);
      commit(next);
    };

    const getFieldError = (fieldName: ValidationError["field"]): ValidationError | undefined => {
      if (fieldName === "start" && inputs.date !== "" && !validateDateInput(inputs.date)) {
        return { field: "start", message: translate("dateRangeFilter.validation.invalidDateFormat") };
      }
      if (fieldName === "startTime" && inputs.time !== "" && !validateTimeInput(inputs.time)) {
        return { field: "startTime", message: translate("dateRangeFilter.validation.invalidTimeFormat") };
      }

      return undefined;
    };

    const getBorderColor = (fieldName: ValidationError["field"]) =>
      getFieldError(fieldName) ? "danger.solid" : "border";

    const handleInputChange =
      (_field: "end" | "start", inputType: "date" | "time") => (event: ChangeEvent<HTMLInputElement>) =>
        applyChange(inputType, event.target.value);

    const isoValue = value === "" ? undefined : value;
    const calendarValue = { endDate: isoValue, startDate: isoValue };

    return (
      <Popover.Root lazyMount positioning={{ placement: "bottom-start" }} unmountOnExit>
        <Popover.Trigger asChild disabled={disabled}>
          <Box
            _hover={Boolean(disabled) ? undefined : { borderColor: "border.emphasized" }}
            alignItems="center"
            borderColor="border"
            borderRadius="md"
            borderWidth="1px"
            cursor={Boolean(disabled) ? "not-allowed" : "pointer"}
            data-testid="datetime-input"
            display="flex"
            gap={2}
            justifyContent="space-between"
            onBlur={onBlur}
            onFocus={onFocus}
            onKeyDown={onKeyDown}
            opacity={Boolean(disabled) ? 0.5 : 1}
            px={3}
            py={2}
            ref={ref}
            w="full"
          >
            <Text color={selected ? "fg" : "fg.muted"} fontSize="sm" truncate>
              {selected ? selected.format(DISPLAY_FORMAT) : translate("common:filters.selectDateTime")}
            </Text>
            <MdCalendarToday />
          </Box>
        </Popover.Trigger>
        <Popover.Content p={3} w="320px">
          <VStack gap={2} w="full">
            <HStack gap={1} justify="flex-start" w="full">
              <MdAccessTime size={14} />
              <Text color="fg.muted" fontSize="xs">
                {selectedTimezone}
              </Text>
            </HStack>

            <HStack alignItems="flex-start" gap={2} w="full">
              <DateInput
                field="start"
                getBorderColor={getBorderColor}
                getFieldError={getFieldError}
                handleInputChange={handleInputChange}
                inputType="date"
                inputValue={inputs.date}
                label={translate("common:filters.date")}
                onClear={() => applyChange("date", "")}
                placeholder={DATE_INPUT_FORMAT}
              />
              <DateInput
                field="start"
                getBorderColor={getBorderColor}
                getFieldError={getFieldError}
                handleInputChange={handleInputChange}
                inputType="time"
                inputValue={inputs.time}
                label={translate("common:filters.time")}
                onClear={() => applyChange("time", "")}
                placeholder={TIME_INPUT_FORMAT}
              />
            </HStack>

            <DateRangeCalendar
              currentMonth={currentMonth}
              onDateClick={handleDateClick}
              onMonthChange={setCurrentMonth}
              value={calendarValue}
            />
          </VStack>
        </Popover.Content>
      </Popover.Root>
    );
  },
);
