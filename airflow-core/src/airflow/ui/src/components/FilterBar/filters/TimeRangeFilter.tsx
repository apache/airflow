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
import { useState, type ChangeEvent } from "react";

import { Box, HStack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdAccessTime, MdClose } from "react-icons/md";

import { IconButton, Popover } from "src/system-components";

import { useTimezone } from "src/context/timezone";
import type { ValidationError } from "src/hooks/useDateRangeFilter";
import { TIME_INPUT_FORMAT, validateTimeInput } from "src/hooks/useDateRangeFilter";

import { FilterPill } from "../FilterPill";
import type { FilterPluginProps } from "../types";
import { DateInput } from "./DateInput";

export const TimeRangeFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const { t: translate } = useTranslation(["common", "components"]);
  const { selectedTimezone } = useTimezone();
  const range =
    filter.value !== null && typeof filter.value === "object" && "startTime" in filter.value
      ? filter.value
      : undefined;
  const value = {
    endTime: range?.endTime ?? "",
    startTime: range?.startTime ?? "",
  };
  const [inputs, setInputs] = useState(value);
  const hasValue = Boolean(value.startTime || value.endTime);
  const displayValue = `${value.startTime || "…"} - ${value.endTime || "…"}`;
  const getFieldError = (field: ValidationError["field"]): ValidationError | undefined => {
    if (field !== "startTime" && field !== "endTime") {
      return undefined;
    }
    const valid =
      (inputs[field] === inputs[field].trim() && validateTimeInput(inputs[field])) ||
      (field === "endTime" && inputs[field] === "24:00");

    return valid
      ? undefined
      : { field, message: translate("components:dateRangeFilter.validation.invalidTimeFormat") };
  };
  const handleInputChange = (field: "end" | "start") => (event: ChangeEvent<HTMLInputElement>) => {
    const key = field === "start" ? "startTime" : "endTime";
    const next = { ...inputs, [key]: event.target.value };
    const startValid = next.startTime === next.startTime.trim() && validateTimeInput(next.startTime);
    const endValid =
      (next.endTime === next.endTime.trim() && validateTimeInput(next.endTime)) || next.endTime === "24:00";

    setInputs(next);
    if (
      startValid &&
      endValid &&
      (next.startTime === "" || next.endTime === "" || next.startTime < next.endTime)
    ) {
      onChange(next.startTime || next.endTime ? next : undefined);
    }
  };

  return (
    <FilterPill
      displayValue={displayValue}
      filter={filter}
      hasValue={hasValue}
      onRemove={onRemove}
      renderInput={(_props, { onRequestClose }) => (
        <Popover.Root
          defaultOpen
          onOpenChange={({ open }) => {
            if (open) {
              setInputs(value);
            } else {
              setTimeout(onRequestClose, 0);
            }
          }}
          positioning={{ placement: "bottom-start" }}
        >
          <Popover.Trigger asChild>
            <Box
              alignItems="center"
              as="button"
              bg={hasValue ? "brand.emphasized" : "gray.muted"}
              borderRadius="full"
              color="colorPalette.fg"
              colorPalette={hasValue ? "brand" : "gray"}
              display="flex"
              fontSize="sm"
              gap={2}
              h="9"
              pl={3}
              pr={1}
            >
              <MdAccessTime />
              <Text>
                {filter.config.label}: <strong>{displayValue}</strong>
              </Text>
              <IconButton
                aria-label={`Remove ${filter.config.label} filter`}
                borderRadius="full"
                label={translate("common:filters.removeFilter")}
                onClick={(event) => {
                  event.stopPropagation();
                  onRemove();
                }}
                size="2xs"
                variant="ghost"
              >
                <MdClose />
              </IconButton>
            </Box>
          </Popover.Trigger>
          <Popover.Content p={3} w="320px">
            <VStack align="stretch" gap={2}>
              <HStack color="fg.muted" fontSize="xs" gap={1}>
                <MdAccessTime />
                <Text>{selectedTimezone}</Text>
              </HStack>
              <HStack alignItems="flex-start" gap={2}>
                {(["start", "end"] as const).map((field) => (
                  <Box as="label" flex="1" key={field}>
                    <DateInput
                      field={field}
                      getBorderColor={(name) => (getFieldError(name) ? "danger.solid" : "border")}
                      getFieldError={getFieldError}
                      handleInputChange={handleInputChange}
                      inputType="time"
                      inputValue={field === "start" ? inputs.startTime : inputs.endTime}
                      label={translate(field === "start" ? "common:table.from" : "common:table.to")}
                      onClear={() =>
                        handleInputChange(field)({ target: { value: "" } } as ChangeEvent<HTMLInputElement>)
                      }
                      placeholder={TIME_INPUT_FORMAT}
                    />
                  </Box>
                ))}
              </HStack>
              {inputs.startTime !== "" && inputs.endTime !== "" && inputs.startTime >= inputs.endTime ? (
                <Text color="danger.fg" fontSize="xs">
                  {translate("components:dateRangeFilter.validation.startBeforeEnd")}
                </Text>
              ) : undefined}
            </VStack>
          </Popover.Content>
        </Popover.Root>
      )}
    />
  );
};
