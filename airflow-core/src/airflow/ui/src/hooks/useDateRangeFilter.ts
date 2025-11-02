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
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import type { TFunction } from "i18next";
import { useEffect, useMemo, useState } from "react";

import type { DateRangeValue } from "src/components/FilterBar/types";
import { isValidDateValue } from "src/components/FilterBar/utils";
import { useTimezone } from "src/context/timezone";

dayjs.extend(timezone);

export const DATE_INPUT_FORMAT = "YYYY/MM/DD";
export const TIME_INPUT_FORMAT = "HH:mm";
export const DATETIME_INPUT_FORMAT = `${DATE_INPUT_FORMAT} ${TIME_INPUT_FORMAT}`;

export type DateSelection = "end" | "start" | undefined;

export type DateRangeEditingState = {
  currentMonth: dayjs.Dayjs;
  inputs: {
    end: string;
    endTime: string;
    start: string;
    startTime: string;
  };
  selectionTarget: DateSelection;
};

type UseDateRangeFilterArgs = {
  onChange: (next: DateRangeValue) => void;
  translate: TFunction;
  value: DateRangeValue;
};

const combineDateAndTime = (dateStr: string, timeStr: string, tz: string): string => {
  const date = dayjs(dateStr, DATE_INPUT_FORMAT, true);

  if (!date.isValid()) {
    return "";
  }

  if (!timeStr.trim()) {
    // If no time is provided, set to 00:00
    return date.startOf("day").tz(tz, true).toISOString();
  }

  const time = dayjs(`2000-01-01 ${timeStr}`, `YYYY-MM-DD ${TIME_INPUT_FORMAT}`, true);

  if (!time.isValid()) {
    return date.startOf("day").tz(tz, true).toISOString();
  }

  const combined = date.hour(time.hour()).minute(time.minute()).second(0).millisecond(0);

  return combined.tz(tz, true).toISOString();
};

export const useDateRangeFilter = ({ onChange, translate,value }: UseDateRangeFilterArgs) => {
  const { selectedTimezone } = useTimezone();
  const startDateValue = useMemo(
    () => (isValidDateValue(value.startDate) ? dayjs(value.startDate) : undefined),
    [value.startDate],
  );
  const endDateValue = useMemo(
    () => (isValidDateValue(value.endDate) ? dayjs(value.endDate) : undefined),
    [value.endDate],
  );

  const [editingState, setEditingState] = useState<DateRangeEditingState>(() => ({
    currentMonth: startDateValue ?? endDateValue ?? dayjs(),
    inputs: {
      end: endDateValue?.format(DATE_INPUT_FORMAT) ?? "",
      endTime: endDateValue?.format(TIME_INPUT_FORMAT) ?? "",
      start: startDateValue?.format(DATE_INPUT_FORMAT) ?? "",
      startTime: startDateValue?.format(TIME_INPUT_FORMAT) ?? "",
    },
    selectionTarget: undefined,
  }));

  useEffect(() => {
    setEditingState((prev) => ({
      ...prev,
      inputs: {
        end: endDateValue?.format(DATE_INPUT_FORMAT) ?? "",
        endTime: endDateValue?.format(TIME_INPUT_FORMAT) ?? "",
        start: startDateValue?.format(DATE_INPUT_FORMAT) ?? "",
        startTime: startDateValue?.format(TIME_INPUT_FORMAT) ?? "",
      },
    }));
  }, [startDateValue, endDateValue]);

  const handleDateClick = (clickedDate: dayjs.Dayjs) => {
    const currentTarget = editingState.selectionTarget;
    let nextTarget: DateSelection = "end";
    let newStartDate = value.startDate;
    let newEndDate = value.endDate;

    if (currentTarget === "start" || (!startDateValue && !endDateValue)) {
      // Set start date with start of day time
      newStartDate = clickedDate.startOf("day").toISOString();

      if (endDateValue && clickedDate.isAfter(endDateValue, "day")) {
        newEndDate = undefined;
      }
    } else {
      // Set end date with end of day time
      newEndDate = clickedDate.endOf("day").toISOString();

      if (startDateValue && clickedDate.isBefore(startDateValue, "day")) {
        newStartDate = clickedDate.startOf("day").toISOString();
        newEndDate = clickedDate.endOf("day").toISOString();
      }
      nextTarget = undefined;
    }

    onChange({ endDate: newEndDate, startDate: newStartDate });
    setEditingState((prev) => ({
      ...prev,
      currentMonth: clickedDate,
      selectionTarget: nextTarget,
    }));
  };

  const handleInputChange =
    (field: "end" | "start", inputType: "date" | "time") => (event: React.ChangeEvent<HTMLInputElement>) => {
      const inputValue = event.target.value;
      const inputKey = inputType === "date" ? field : (`${field}Time` as const);

      setEditingState((prev) => ({
        ...prev,
        inputs: { ...prev.inputs, [inputKey]: inputValue },
      }));

      const currentInputs = { ...editingState.inputs, [inputKey]: inputValue };
      const dateStr = field === "start" ? currentInputs.start : currentInputs.end;
      const timeStr = field === "start" ? currentInputs.startTime : currentInputs.endTime;

      if (dayjs(dateStr, DATE_INPUT_FORMAT, true).isValid()) {
        const combinedDateTime = combineDateAndTime(dateStr, timeStr, selectedTimezone);

        if (combinedDateTime) {
          onChange({
            ...value,
            [field === "start" ? "startDate" : "endDate"]: combinedDateTime,
          });
        }
      }
    };

  const formatDateTime = (date: dayjs.Dayjs) => {
    const dateStr = date.tz(selectedTimezone).format("MMM DD, YYYY");

    return `${dateStr} ${date.tz(selectedTimezone).format("HH:mm")}`;
  };

  const formatDisplayValue = () => {
    if (!startDateValue && !endDateValue) {
      return "";
    }

    if (startDateValue && endDateValue) {
      // simplified format for same-day ranges
      if (startDateValue.isSame(endDateValue, "day")) {
        const dateStr = startDateValue.tz(selectedTimezone).format("MMM DD, YYYY");
        const startTime = startDateValue.tz(selectedTimezone).format("HH:mm");
        const endTime = endDateValue.tz(selectedTimezone).format("HH:mm");

        return `${dateStr} ${startTime} - ${endTime}`;
      }

      return `${formatDateTime(startDateValue)} - ${formatDateTime(endDateValue)}`;
    }

    if (startDateValue) {
      return `${translate("common:filters.from")} ${formatDateTime(startDateValue)}`;
    }

    return endDateValue ? `${translate("common:filters.to")} ${formatDateTime(endDateValue)}` : "";
  };

  return {
    editingState,
    endDateValue,
    formatDisplayValue,
    handleDateClick,
    handleInputChange,
    setEditingState,
    startDateValue,
  };
};
