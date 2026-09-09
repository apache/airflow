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
import { useRef, type ReactNode } from "react";

import { Box, createListCollection } from "@chakra-ui/react";

import { Select } from "src/system-components";

import { FilterPill } from "../FilterPill";
import type { FilterConfig, FilterPluginProps } from "../types";

type SelectOption = {
  label: string;
  // Richer rendering (e.g. a ``StateBadge``) used inside the dropdown menu only. The
  // pill and the select trigger always show ``label`` — both are fixed-height, and a
  // badge there overflows and gets clipped (see issue #72283).
  menuItem?: ReactNode;
  value: string;
};

type SelectFilterConfig = {
  options: Array<SelectOption>;
};

export const SelectFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const config = filter.config as FilterConfig & SelectFilterConfig;

  // Selecting an option closes the menu in the same tick as the value commits, before React has
  // re-rendered. Recording the selection keeps the close from being mistaken for an abandoned
  // filter, which a timing-based check gets wrong.
  const hasJustSelected = useRef(false);
  const handleValueChange = ({ value }: { value: Array<string> }) => {
    const [newValue] = value;

    hasJustSelected.current = true;
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
      // ``onKeyDown`` is deliberately not forwarded: the select owns Enter and Escape, and letting
      // the pill also act on Enter tore the filter down before the chosen value committed. Escape
      // still closes the pill, through ``onOpenChange`` below.
      renderInput={({ onBlur, onFocus }, { onRequestClose }) => (
        <Box
          alignItems="center"
          bg="bg"
          border="0.5px solid"
          borderColor="border"
          borderRadius="full"
          display="flex"
          h="full"
          onBlur={onBlur}
          onFocus={onFocus}
          overflow="hidden"
          tabIndex={0}
          width="330px"
        >
          <Box
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
          </Box>
          <Select.Root
            border="none"
            collection={createListCollection({ items: config.options })}
            // A filter added from the menu has nothing to show until it is given a value, so
            // open straight onto the options instead of making the user click again.
            defaultOpen={!hasValue}
            h="full"
            // Dismissing the menu hands focus back to the trigger inside the pill, so no blur
            // fires and the pill would stay in edit mode. A close that follows a selection is
            // left alone: the blur that selection triggers collapses the pill instead.
            onOpenChange={({ open }) => {
              if (!open && !hasJustSelected.current) {
                onRequestClose();
              }
              hasJustSelected.current = false;
            }}
            onValueChange={handleValueChange}
            value={hasValue && typeof filter.value === "string" ? [filter.value] : []}
          >
            <Select.Trigger dataTestId={`${filter.config.key}-filter`} triggerProps={{ border: "none" }}>
              <Select.ValueText placeholder={filter.config.placeholder} />
            </Select.Trigger>
            <Select.Content>
              {config.options.map((option) => (
                <Select.Item
                  data-testid={`${filter.config.key}-filter-${option.value === "" ? "all" : option.value}`}
                  item={option}
                  key={option.value}
                >
                  {option.menuItem ?? option.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </Box>
      )}
    />
  );
};
