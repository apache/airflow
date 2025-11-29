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

export type ValidationError = {
  field: "end" | "endTime" | "range" | "start" | "startTime";
  message: string;
};

export type DateRangeEditingState = {
  currentMonth: dayjs.Dayjs;
  inputs: {
    end: string;
    endTime: string;
    start: string;
    startTime: string;
  };
  selectionTarget: DateSelection;
  validationErrors: Array<ValidationError>;
};

type UseDateRangeFilterArgs = {
  onChange: (next: DateRangeValue) => void;
  translate: TFunction;
  value: DateRangeValue;
};

const validateDateInput = (dateStr: string): boolean => {
  if (!dateStr.trim()) {
    return true; // Empty is valid
  }

  const parsed = dayjs(dateStr, DATE_INPUT_FORMAT, true);

  if (!parsed.isValid()) {
    return false;
  }

  // Check if the parsed date exactly matches the input format
  // This prevents dayjs from auto-correcting invalid dates
  return parsed.format(DATE_INPUT_FORMAT) === dateStr;
};

const validateTimeInput = (timeStr: string): boolean => {
  if (!timeStr.trim()) {
    return true; // Empty is valid
  }

  const parsed = dayjs(`2000-01-01 ${timeStr}`, `YYYY-MM-DD ${TIME_INPUT_FORMAT}`, true);

  if (!parsed.isValid()) {
    return false;
  }

  // Check if the parsed time exactly matches the input format
  // This prevents dayjs from auto-correcting invalid times
  return parsed.format(TIME_INPUT_FORMAT) === timeStr;
};

const validateDateRange = (startDate?: string, endDate?: string): boolean => {
  if (!Boolean(startDate) || !Boolean(endDate)) {
    return true; // Incomplete ranges are valid
  }

  const start = dayjs(startDate);
  const end = dayjs(endDate);

  return start.isBefore(end) || start.isSame(end);
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

export const useDateRangeFilter = ({ onChange, translate, value }: UseDateRangeFilterArgs) => {
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
    validationErrors: [],
  }));

  const validateInputs = (inputs: DateRangeEditingState["inputs"]): Array<ValidationError> => {
    const errors: Array<ValidationError> = [];

    if (inputs.start && !validateDateInput(inputs.start)) {
      errors.push({
        field: "start",
        message: translate("components:dateRangeFilter.validation.invalidDateFormat"),
      });
    }

    if (inputs.end && !validateDateInput(inputs.end)) {
      errors.push({
        field: "end",
        message: translate("components:dateRangeFilter.validation.invalidDateFormat"),
      });
    }

    if (inputs.startTime && !validateTimeInput(inputs.startTime)) {
      errors.push({
        field: "startTime",
        message: translate("components:dateRangeFilter.validation.invalidTimeFormat"),
      });
    }

    if (inputs.endTime && !validateTimeInput(inputs.endTime)) {
      errors.push({
        field: "endTime",
        message: translate("components:dateRangeFilter.validation.invalidTimeFormat"),
      });
    }

    if (
      Boolean(inputs.start) &&
      Boolean(inputs.end) &&
      validateDateInput(inputs.start) &&
      validateDateInput(inputs.end)
    ) {
      const startDateTime = combineDateAndTime(inputs.start, inputs.startTime, selectedTimezone);
      const endDateTime = combineDateAndTime(inputs.end, inputs.endTime, selectedTimezone);

      if (Boolean(startDateTime) && Boolean(endDateTime) && !validateDateRange(startDateTime, endDateTime)) {
        errors.push({
          field: "range",
          message: translate("components:dateRangeFilter.validation.startBeforeEnd"),
        });
      }
    }

    return errors;
  };

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
    let newStartDate: string | undefined = value.startDate;
    let newEndDate: string | undefined = value.endDate;

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
    setEditingState((prev) => {
      // Update inputs to reflect the new date values
      const newInputs = {
        ...prev.inputs,
        end: newEndDate === undefined ? "" : dayjs(newEndDate).format(DATE_INPUT_FORMAT),
        start: newStartDate === undefined ? "" : dayjs(newStartDate).format(DATE_INPUT_FORMAT),
      };
      // Revalidate with the new inputs
      const validationErrors = validateInputs(newInputs);

      return {
        ...prev,
        currentMonth: clickedDate,
        inputs: newInputs,
        selectionTarget: nextTarget,
        validationErrors,
      };
    });
  };

  const handleInputChange =
    (field: "end" | "start", inputType: "date" | "time") => (event: React.ChangeEvent<HTMLInputElement>) => {
      const inputValue = event.target.value;
      const inputKey = inputType === "date" ? field : (`${field}Time` as const);

      setEditingState((prev) => {
        const newInputs = { ...prev.inputs, [inputKey]: inputValue };
        const validationErrors = validateInputs(newInputs);

        const dateStr = field === "start" ? newInputs.start : newInputs.end;
        const timeStr = field === "start" ? newInputs.startTime : newInputs.endTime;

        if (dayjs(dateStr, DATE_INPUT_FORMAT, true).isValid()) {
          const combinedDateTime = combineDateAndTime(dateStr, timeStr, selectedTimezone);

          if (Boolean(combinedDateTime)) {
            onChange({
              ...value,
              [field === "start" ? "startDate" : "endDate"]: combinedDateTime,
            });
          }
        }

        return {
          ...prev,
          inputs: newInputs,
          validationErrors,
        };
      });
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
      return `${translate("common:table.from")} ${formatDateTime(startDateValue)}`;
    }

    return endDateValue ? `${translate("common:table.to")} ${formatDateTime(endDateValue)}` : "";
  };

  const getFieldError = (field: ValidationError["field"]) =>
    editingState.validationErrors.find((error) => error.field === field);

  const hasValidationErrors = editingState.validationErrors.length > 0;

  return {
    editingState,
    endDateValue,
    formatDisplayValue,
    getFieldError,
    handleDateClick,
    handleInputChange,
    hasValidationErrors,
    setEditingState,
    startDateValue,
  };
};
