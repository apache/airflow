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
import { Box, HStack, Text } from "@chakra-ui/react";
import { CreatableSelect, Select as ReactSelect, type SelectInstance } from "chakra-react-select";
import { useEffect, useRef } from "react";

import { MatchModeToggle } from "src/components/MatchModeToggle";
import { useMatchMode } from "src/hooks/useMatchMode";

import { FilterPill } from "../FilterPill";
import type { FilterPluginProps } from "../types";

type SelectOption = { label: string; value: string };

// Wider than the single-value editors, which sit at 180–330px: this one holds a row of chips,
// and a narrow control stacks them vertically instead. The match-mode toggle needs its own room
// again on top of that.
const PILL_WIDTH = "460px";
const PILL_WIDTH_WITH_MATCH_MODE = "620px";

type Props = FilterPluginProps & {
  readonly noOptionsMessage: string;
  readonly onInputChange?: (value: string) => void;
  readonly onMenuScrollToBottom?: () => void;
  readonly onMenuScrollToTop?: () => void;
  readonly options: Array<SelectOption>;
};

/**
 * Shared shell for every multiselect filter: the pill chrome, the ``chakra-react-select``
 * control, and the optional any/all toggle. Editors that need async options supply their
 * own ``options`` plus the search and scroll callbacks.
 */
export const MultiSelectPill = ({
  filter,
  noOptionsMessage,
  onChange,
  onInputChange,
  onMenuScrollToBottom,
  onMenuScrollToTop,
  onRemove,
  options,
}: Props) => {
  const { mode, setMode } = useMatchMode(filter.config.matchModeKey);
  const values = Array.isArray(filter.value) ? filter.value : [];
  const SelectComponent = filter.config.isCreatable === true ? CreatableSelect : ReactSelect;
  const showMatchMode = filter.config.matchModeKey !== undefined && values.length >= 2;
  const selectRef = useRef<SelectInstance<SelectOption, true> | null>(null);

  // The editor only mounts when editing begins, so the caret belongs in the select from the
  // start. This runs a frame late on purpose: FilterPill focuses its wrapper on the same tick,
  // and the Add Filter menu hands focus back to its trigger, so claiming focus any earlier
  // loses to one of them — and losing it blurs the pill, which discards the new filter.
  useEffect(() => {
    const frame = requestAnimationFrame(() => selectRef.current?.focus());

    return () => cancelAnimationFrame(frame);
  }, []);

  return (
    <FilterPill
      // Each value is its own node so the collapsed chip stays queryable by value.
      displayValue={
        <HStack display="inline-flex" gap={1}>
          {mode === "all" && values.length >= 2 ? (
            <Box as="span" color="fg.muted" fontSize="xs">
              {`(${mode})`}
            </Box>
          ) : undefined}
          {values.map((value) => value).join(", ")}
        </HStack>
      }
      filter={filter}
      hasValue={values.length > 0}
      onRemove={onRemove}
      // ``onKeyDown`` is deliberately not forwarded: react-select owns Enter (select the
      // highlighted option, or create one) and Escape (close the menu). Letting the pill also
      // act on Enter tears the filter down before the value it just created commits.
      renderInput={({ onBlur, onFocus, ref }) => (
        <Box
          _focusWithin={{ outlineColor: "colorPalette.solid", outlineStyle: "solid", outlineWidth: "1px" }}
          alignItems="center"
          bg="bg"
          border="0.5px solid"
          borderColor="border"
          borderRadius="full"
          colorPalette="brand"
          display="flex"
          h="full"
          onBlur={onBlur}
          onFocus={onFocus}
          overflow="hidden"
          ref={ref}
          tabIndex={0}
          width={showMatchMode ? PILL_WIDTH_WITH_MATCH_MODE : PILL_WIDTH}
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
          <Box flex="1" minW={0}>
            <SelectComponent<SelectOption, true>
              aria-label={filter.config.label}
              chakraStyles={{
                // The pill shows focus for the whole control; this inner outline is squarer than
                // the pill and gets clipped by its rounded corners.
                container: (provided) => ({ ...provided, width: "100%" }),
                control: (provided) => ({
                  ...provided,
                  // The pill draws focus for the whole control. This inner ring is squarer than
                  // the pill, so its right end is clipped by the rounded corner.
                  _focusVisible: { outline: "none" },
                  border: "none",
                  colorPalette: "brand",
                }),
                menu: (provided) => ({ ...provided, zIndex: 2 }),
              }}
              // A filter added from the menu has nothing to show until it is given a value, so
              // open straight onto the options instead of making the user click again. Focus is
              // left to FilterPill: autoFocus here loses a race with the Add Filter menu handing
              // focus back to its trigger, which blurs the pill and discards the new filter.
              // Picking a value is rarely the end of it on a multiselect, so leave the menu up
              // for the next one instead of making the user reopen it each time.
              closeMenuOnSelect={false}
              defaultMenuIsOpen={values.length === 0}
              isClearable
              isMulti
              // The pill clips its own corners, and the table header it sits in scrolls, so an
              // inline menu is hidden by one of those ancestors. Portal it out to the body.
              menuPortalTarget={document.body}
              noOptionsMessage={() => noOptionsMessage}
              onChange={(selected) => onChange(selected.map(({ value }) => value))}
              onInputChange={onInputChange}
              onMenuScrollToBottom={onMenuScrollToBottom}
              onMenuScrollToTop={onMenuScrollToTop}
              options={options}
              placeholder={filter.config.placeholder ?? filter.config.label}
              ref={selectRef}
              // menuPortal is a react-select style, not a chakraStyles one; the portalled menu
              // needs to clear the sticky table header.
              styles={{ menuPortal: (base) => ({ ...base, zIndex: 1500 }) }}
              value={values.map((value) => ({ label: value, value }))}
            />
          </Box>
          {showMatchMode ? <MatchModeToggle mode={mode} onModeChange={setMode} /> : undefined}
        </Box>
      )}
    />
  );
};
