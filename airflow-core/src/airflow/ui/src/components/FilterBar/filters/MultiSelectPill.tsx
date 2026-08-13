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
import { CreatableSelect, Select as ReactSelect } from "chakra-react-select";

import { MatchModeToggle } from "src/components/MatchModeToggle";
import { useMatchMode } from "src/hooks/useMatchMode";

import { FilterPill } from "../FilterPill";
import type { FilterPluginProps } from "../types";

type SelectOption = { label: string; value: string };

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

  return (
    <FilterPill
      // Each value is its own node so the collapsed chip stays queryable by value.
      displayValue={
        <HStack as="span" gap={1}>
          {mode === "all" && values.length >= 2 ? (
            <Text as="span" color="fg.muted" fontSize="xs">
              {`(${mode})`}
            </Text>
          ) : undefined}
          {values.map((value) => (
            <Text as="span" key={value}>
              {value}
            </Text>
          ))}
        </HStack>
      }
      filter={filter}
      hasValue={values.length > 0}
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
          // Wider than the single-value editors: chips plus the optional match-mode
          // toggle need the room, and a narrow control stacks them vertically.
          width={showMatchMode ? "620px" : "460px"}
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
                container: (provided) => ({ ...provided, width: "100%" }),
                control: (provided) => ({ ...provided, border: "none", colorPalette: "brand" }),
                menu: (provided) => ({ ...provided, zIndex: 2 }),
              }}
              isClearable
              isMulti
              noOptionsMessage={() => noOptionsMessage}
              onChange={(selected) => onChange(selected.map(({ value }) => value))}
              onInputChange={onInputChange}
              onMenuScrollToBottom={onMenuScrollToBottom}
              onMenuScrollToTop={onMenuScrollToTop}
              options={options}
              placeholder={filter.config.placeholder ?? filter.config.label}
              value={values.map((value) => ({ label: value, value }))}
            />
          </Box>
          {showMatchMode ? <MatchModeToggle mode={mode} onModeChange={setMode} /> : undefined}
        </Box>
      )}
    />
  );
};
