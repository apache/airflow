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
import { Box, Text, createListCollection } from "@chakra-ui/react";

import { Select } from "src/components/ui";

import { FilterPill } from "../FilterPill";
import type { FilterConfig, FilterPluginProps } from "../types";

type SelectOption = {
  label: string;
  value: string;
};

type SelectFilterConfig = {
  options: Array<SelectOption>;
};

export const SelectFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const config = filter.config as FilterConfig & SelectFilterConfig;
  const isMultiple = config.multiple === true;

  const selectedValues: Array<string> = Array.isArray(filter.value)
    ? filter.value.filter((item): item is string => typeof item === "string" && item !== "")
    : filter.value !== null && filter.value !== undefined && filter.value !== ""
      ? [String(filter.value)]
      : [];

  const handleValueChange = ({ value }: { value: Array<string> }) => {
    if (isMultiple) {
      onChange(value);

      return;
    }

    const [newValue] = value;

    onChange(newValue ?? "");

    // Trigger blur to close the editing mode after selection (single select only)
    setTimeout(() => {
      const activeElement = document.activeElement as HTMLElement;

      activeElement.blur();
    }, 0);
  };

  const hasValue = selectedValues.length > 0;

  const selectedLabels = selectedValues
    .map((val) => config.options.find((option) => option.value === val)?.label)
    .filter((label): label is string => typeof label === "string" && label !== "");

  const displayValue = isMultiple ? selectedLabels.join(", ") : selectedLabels[0];

  return (
    <FilterPill
      displayValue={displayValue ?? ""}
      filter={filter}
      hasValue={hasValue}
      onChange={onChange}
      onRemove={onRemove}
    >
      <Box
        alignItems="center"
        bg="bg"
        border="0.5px solid"
        borderColor="border"
        borderRadius="full"
        display="flex"
        h="full"
        overflow="hidden"
        width="330px"
      >
        <Text
          alignItems="center"
          bg="gray.muted"
          borderLeftRadius="full"
          display="flex"
          fontSize="sm"
          fontWeight="medium"
          h="full"
          px={4}
          py={2}
          whiteSpace="nowrap"
        >
          {filter.config.label}:
        </Text>

        <Select.Root
          border="none"
          closeOnSelect={!isMultiple}
          collection={createListCollection({ items: config.options })}
          h="full"
          multiple={isMultiple}
          onValueChange={handleValueChange}
          value={selectedValues}
        >
          <Select.Trigger triggerProps={{ border: "none" }}>
            <Select.ValueText placeholder={filter.config.placeholder} />
          </Select.Trigger>

          <Select.Content>
            {config.options.map((option) => (
              <Select.Item item={option} key={option.value}>
                {option.label}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      </Box>
    </FilterPill>
  );
};
