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
import { Box, CloseButton, HStack, Input, InputGroup, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { MdAccessTime, MdCalendarToday } from "react-icons/md";

import { DateRangeCalendar } from "src/components/FilterBar/filters/DateRangeCalendar";
import { Popover } from "src/components/ui";
import { useTimezone } from "src/context/timezone";

dayjs.extend(timezone);

const DATE_FORMAT = "YYYY/MM/DD";
const TIME_FORMAT = "HH:mm";

type SingleDatePickerProps = {
  readonly onChange: (value: string | undefined) => void;
  readonly value: string | undefined;
};

export const SingleDatePicker = ({ onChange, value }: SingleDatePickerProps) => {
  const { t: translate } = useTranslation("common");
  const { selectedTimezone } = useTimezone();
  const parsedValue =
    value !== undefined && value !== "" && dayjs(value).isValid() ? dayjs(value) : undefined;
  const displayInTz = parsedValue?.tz(selectedTimezone);

  const [currentMonth, setCurrentMonth] = useState<dayjs.Dayjs>(displayInTz ?? dayjs());
  const [dateInput, setDateInput] = useState(displayInTz?.format(DATE_FORMAT) ?? "");
  const [timeInput, setTimeInput] = useState(displayInTz?.format(TIME_FORMAT) ?? "");

  const applyDateTime = (dateStr: string, timeStr: string) => {
    const date = dayjs(dateStr, DATE_FORMAT, true);

    if (!date.isValid()) {
      return;
    }

    let combined = date.startOf("day");
    const time = dayjs(`2000-01-01 ${timeStr}`, `YYYY-MM-DD ${TIME_FORMAT}`, true);

    if (time.isValid()) {
      combined = combined.hour(time.hour()).minute(time.minute());
    }

    onChange(combined.tz(selectedTimezone, true).toISOString());
  };

  const handleDateClick = (clickedDate: dayjs.Dayjs) => {
    const newDateStr = clickedDate.format(DATE_FORMAT);
    const effectiveTime = timeInput || dayjs().tz(selectedTimezone).format(TIME_FORMAT);

    setDateInput(newDateStr);
    setTimeInput(effectiveTime);
    setCurrentMonth(clickedDate);
    applyDateTime(newDateStr, effectiveTime);
  };

  const handleDateInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const val = event.target.value;

    setDateInput(val);
    const parsed = dayjs(val, DATE_FORMAT, true);

    if (parsed.isValid()) {
      setCurrentMonth(parsed);
      applyDateTime(val, timeInput);
    }
  };

  const handleTimeInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const val = event.target.value;

    setTimeInput(val);
    applyDateTime(dateInput, val);
  };

  const handleClear = (event: React.MouseEvent) => {
    event.stopPropagation();
    setDateInput("");
    setTimeInput("");
    onChange(undefined);
  };

  const displayText = displayInTz
    ? `${displayInTz.format(DATE_FORMAT)} ${displayInTz.format(TIME_FORMAT)}`
    : "";

  const calendarValue = displayInTz
    ? { endDate: displayInTz.toISOString(), startDate: displayInTz.toISOString() }
    : { endDate: undefined, startDate: undefined };

  return (
    <Popover.Root lazyMount positioning={{ placement: "bottom-start" }} unmountOnExit>
      <Popover.Trigger asChild>
        <InputGroup
          endElement={
            value === undefined ? undefined : (
              <CloseButton aria-label="Clear date" onClick={handleClear} size="xs" />
            )
          }
          startElement={<MdCalendarToday />}
        >
          <Input cursor="pointer" placeholder={DATE_FORMAT} readOnly size="sm" value={displayText} />
        </InputGroup>
      </Popover.Trigger>
      <Popover.Content p={3} w="280px">
        <VStack gap={2} w="full">
          <HStack gap={1} justify="flex-start" w="full">
            <MdAccessTime size={14} />
            <Text color="fg.muted" fontSize="xs">
              {selectedTimezone}
            </Text>
          </HStack>
          <HStack gap={2} w="full">
            <Box flex="1">
              <Text color="fg.muted" fontSize="xs" mb={0.25}>
                {translate("date")}
              </Text>
              <Input
                fontSize="sm"
                onChange={handleDateInputChange}
                placeholder={DATE_FORMAT}
                size="sm"
                value={dateInput}
              />
            </Box>
            <Box flex="1">
              <Text color="fg.muted" fontSize="xs" mb={0.25}>
                {translate("time")}
              </Text>
              <Input
                fontSize="sm"
                onChange={handleTimeInputChange}
                placeholder={TIME_FORMAT}
                size="sm"
                value={timeInput}
              />
            </Box>
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
};
