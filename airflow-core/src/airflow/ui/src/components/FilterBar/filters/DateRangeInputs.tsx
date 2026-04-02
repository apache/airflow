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
import { HStack, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import type { TFunction } from "i18next";
import { MdAccessTime } from "react-icons/md";

import { useTimezone } from "src/context/timezone";
import type { DateRangeEditingState, ValidationError } from "src/hooks/useDateRangeFilter";
import { DATE_INPUT_FORMAT, TIME_INPUT_FORMAT } from "src/hooks/useDateRangeFilter";

import type { DateRangeValue } from "../types";
import { DateInput } from "./DateInput";

type DateRangeInputsProps = {
  readonly editingState: DateRangeEditingState;
  readonly endDateValue: dayjs.Dayjs | undefined;
  readonly getFieldError: (field: ValidationError["field"]) => ValidationError | undefined;
  readonly handleInputChange: (
    field: "end" | "start",
    inputType: "date" | "time",
  ) => (event: React.ChangeEvent<HTMLInputElement>) => void;
  readonly onChange: (value: DateRangeValue) => void;
  readonly setEditingState: React.Dispatch<React.SetStateAction<DateRangeEditingState>>;
  readonly startDateValue: dayjs.Dayjs | undefined;
  readonly translate: TFunction;
  readonly value: DateRangeValue;
};

export const DateRangeInputs = ({
  editingState,
  endDateValue,
  getFieldError,
  handleInputChange,
  onChange,
  setEditingState,
  startDateValue,
  translate,
  value,
}: DateRangeInputsProps) => {
  const { selectedTimezone } = useTimezone();

  const getBorderColor = (fieldName: ValidationError["field"]) => {
    if (getFieldError(fieldName)) {
      return "danger.solid";
    }
    // Check if there's a range error and this is a date field
    if (getFieldError("range") && (fieldName === "start" || fieldName === "end")) {
      return "danger.solid";
    }
    if (editingState.selectionTarget === fieldName) {
      return "brand.focusRing";
    }

    return "border";
  };

  const clearField = (field: "end" | "start") => {
    const updates = field === "start" ? { start: "", startTime: "" } : { end: "", endTime: "" };

    const dateField = field === "start" ? "startDate" : "endDate";

    onChange({ ...value, [dateField]: undefined });
    setEditingState((prev) => ({
      ...prev,
      inputs: { ...prev.inputs, ...updates },
      validationErrors: prev.validationErrors.filter(
        (error) => !error.field.includes(field) && error.field !== "range",
      ),
    }));
  };

  const handleDateBlur = (field: "end" | "start") => () => {
    const dateValue = field === "start" ? startDateValue : endDateValue;
    const inputValue = field === "start" ? editingState.inputs.start : editingState.inputs.end;

    if (dateValue && inputValue && !dayjs(inputValue, DATE_INPUT_FORMAT, true).isValid()) {
      setEditingState((prev) => ({
        ...prev,
        inputs: {
          ...prev.inputs,
          [field]: dateValue.format(DATE_INPUT_FORMAT),
        },
      }));
    }
  };

  const handleFocus = (target: ValidationError["field"]) => () => {
    setEditingState((prev) => ({ ...prev, selectionTarget: target as "end" | "start" | undefined }));
  };

  const clearTime = (field: "end" | "start") => () => {
    handleInputChange(
      field,
      "time",
    )({
      target: { value: "" },
    } as React.ChangeEvent<HTMLInputElement>);
  };

  return (
    <VStack gap={0.5} w="full">
      <HStack justify="flex-start" w="full">
        <HStack gap={1}>
          <MdAccessTime size={14} />
          <Text color="fg.muted" fontSize="xs">
            {selectedTimezone}
          </Text>
        </HStack>
      </HStack>

      <HStack alignItems="flex-start" gap={2} w="full">
        <DateInput
          field="start"
          getBorderColor={getBorderColor}
          getFieldError={getFieldError}
          handleInputChange={handleInputChange}
          inputType="date"
          inputValue={editingState.inputs.start}
          label={translate("common:table.from")}
          onClear={() => clearField("start")}
          onDateBlur={handleDateBlur("start")}
          onFocus={handleFocus("start")}
          placeholder={DATE_INPUT_FORMAT}
        />

        <DateInput
          field="end"
          getBorderColor={getBorderColor}
          getFieldError={getFieldError}
          handleInputChange={handleInputChange}
          inputType="date"
          inputValue={editingState.inputs.end}
          label={translate("common:table.to")}
          onClear={() => clearField("end")}
          onDateBlur={handleDateBlur("end")}
          onFocus={handleFocus("end")}
          placeholder={DATE_INPUT_FORMAT}
        />
      </HStack>

      <HStack alignItems="flex-start" gap={2} w="full">
        <DateInput
          field="start"
          getBorderColor={getBorderColor}
          getFieldError={getFieldError}
          handleInputChange={handleInputChange}
          inputType="time"
          inputValue={editingState.inputs.startTime}
          label={translate("common:filters.startTime")}
          onClear={clearTime("start")}
          placeholder={TIME_INPUT_FORMAT}
        />

        <DateInput
          field="end"
          getBorderColor={getBorderColor}
          getFieldError={getFieldError}
          handleInputChange={handleInputChange}
          inputType="time"
          inputValue={editingState.inputs.endTime}
          label={translate("common:filters.endTime")}
          onClear={clearTime("end")}
          placeholder={TIME_INPUT_FORMAT}
        />
      </HStack>

      {getFieldError("range") && (
        <Text color="danger.fg" fontSize="xs" textAlign="center">
          {getFieldError("range")?.message}
        </Text>
      )}
    </VStack>
  );
};
