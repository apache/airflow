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
import { createListCollection, Box, Text } from "@chakra-ui/react";

import { Select } from "src/components/ui";

import { FilterPill } from "../FilterPill";
import type { FilterPluginProps, FilterConfig } from "../types";

type SelectOption = {
  label: string;
  value: string;
};

type SelectFilterConfig = {
  options: Array<SelectOption>;
};

export const SelectFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const config = filter.config as FilterConfig & SelectFilterConfig;

  const handleValueChange = ({ value }: { value: Array<string> }) => {
    const [newValue] = value;

    onChange(newValue);

    // Trigger blur to close the editing mode after selection
    setTimeout(() => {
      const activeElement = document.activeElement as HTMLElement;

      activeElement.blur();
    }, 0);
  };

  const hasValue = filter.value !== null && filter.value !== undefined && filter.value !== "";
  const displayValue = config.options.find(
    (option) => option.value === (typeof filter.value === "string" ? filter.value : ""),
  )?.label;

  return (
    <FilterPill
      displayValue={displayValue ?? ""}
      filter={filter}
      hasValue={hasValue}
      onRemove={onRemove}
      renderInput={(props) => (
        <Box
          {...props}
          alignItems="center"
          bg="bg"
          border="0.5px solid"
          borderColor="border"
          borderRadius="full"
          display="flex"
          h="full"
          overflow="hidden"
          tabIndex={0}
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
            collection={createListCollection({ items: config.options })}
            h="full"
            onValueChange={handleValueChange}
            value={hasValue && typeof filter.value === "string" ? [filter.value] : []}
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
      )}
    />
  );
};
