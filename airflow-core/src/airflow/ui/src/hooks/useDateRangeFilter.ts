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
import { useEffect, useMemo, useState } from "react";

import type { DateRangeValue } from "src/components/FilterBar/types";
import { isValidDateValue } from "src/components/FilterBar/utils";

export const DATE_INPUT_FORMAT = "YYYY/MM/DD";

export type DateSelection = "end" | "start" | undefined;

export type DateRangeEditingState = {
  currentMonth: dayjs.Dayjs;
  inputs: {
    end: string;
    start: string;
  };
  selectionTarget: DateSelection;
};

type UseDateRangeFilterArgs = {
  onChange: (next: DateRangeValue) => void;
  value: DateRangeValue;
};

export const useDateRangeFilter = ({ onChange, value }: UseDateRangeFilterArgs) => {
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
      start: startDateValue?.format(DATE_INPUT_FORMAT) ?? "",
    },
    selectionTarget: undefined,
  }));

  useEffect(() => {
    setEditingState((prev) => ({
      ...prev,
      inputs: {
        end: endDateValue?.format(DATE_INPUT_FORMAT) ?? "",
        start: startDateValue?.format(DATE_INPUT_FORMAT) ?? "",
      },
    }));
  }, [startDateValue, endDateValue]);

  const handleDateClick = (clickedDate: dayjs.Dayjs) => {
    const newDateStr = clickedDate.toISOString();
    const currentTarget = editingState.selectionTarget;

    let nextTarget: DateSelection = "end";
    let newStartDate = value.startDate;
    let newEndDate = value.endDate;

    if (currentTarget === "start" || (!startDateValue && !endDateValue)) {
      newStartDate = newDateStr;
      if (endDateValue && clickedDate.isAfter(endDateValue)) {
        newEndDate = undefined;
      }
    } else {
      newEndDate = newDateStr;
      if (startDateValue && clickedDate.isBefore(startDateValue)) {
        newStartDate = newDateStr;
        newEndDate = value.startDate;
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

  const handleInputChange = (field: "end" | "start") => (event: React.ChangeEvent<HTMLInputElement>) => {
    const inputValue = event.target.value;

    setEditingState((prev) => ({
      ...prev,
      inputs: { ...prev.inputs, [field]: inputValue },
    }));

    const parsedDate = dayjs(inputValue, DATE_INPUT_FORMAT, true);

    if (parsedDate.isValid()) {
      onChange({
        ...value,
        [field === "start" ? "startDate" : "endDate"]: parsedDate.toISOString(),
      });

      setEditingState((prev) => ({ ...prev, currentMonth: parsedDate }));
    }
  };

  const formatDisplayValue = () => {
    if (!startDateValue && !endDateValue) {
      return "";
    }
    if (startDateValue && endDateValue) {
      return `${startDateValue.format("MMM DD, YYYY")} - ${endDateValue.format("MMM DD, YYYY")}`;
    }
    if (startDateValue) {
      return `From ${startDateValue.format("MMM DD, YYYY")}`;
    }

    return `To ${endDateValue?.format("MMM DD, YYYY") ?? ""}`;
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
