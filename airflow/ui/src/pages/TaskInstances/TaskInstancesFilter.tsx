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
import { HStack, type SelectValueChangeDetails } from "@chakra-ui/react";
import { useCallback } from "react";
import { useSearchParams, useParams } from "react-router-dom";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchBar } from "src/components/SearchBar";
import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { taskInstanceStateOptions as stateOptions } from "src/constants/stateOptions";
import { capitalize } from "src/utils";

const { NAME_PATTERN: NAME_PATTERN_PARAM, STATE: STATE_PARAM }: SearchParamsKeysType = SearchParamsKeys;

export const TaskInstancesFilter = ({
  setTaskDisplayNamePattern,
  taskDisplayNamePattern,
}: {
  readonly setTaskDisplayNamePattern: React.Dispatch<React.SetStateAction<string | undefined>>;
  readonly taskDisplayNamePattern: string | undefined;
}) => {
  const { runId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  const filteredState = searchParams.getAll(STATE_PARAM);
  const hasFilteredState = filteredState.length > 0;

  const handleStateChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val, ...rest] = value;

      if ((val === undefined || val === "all") && rest.length === 0) {
        searchParams.delete(STATE_PARAM);
      } else {
        searchParams.delete(STATE_PARAM);
        value.filter((state) => state !== "all").map((state) => searchParams.append(STATE_PARAM, state));
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    setTaskDisplayNamePattern(value);
    setSearchParams(searchParams);
  };

  return (
    <HStack>
      <Select.Root
        collection={stateOptions}
        maxW="250px"
        multiple
        onValueChange={handleStateChange}
        value={hasFilteredState ? filteredState : ["all"]}
      >
        <Select.Trigger
          {...(hasFilteredState ? { clearable: true } : {})}
          colorPalette="blue"
          isActive={Boolean(filteredState)}
        >
          <Select.ValueText>
            {() =>
              hasFilteredState ? (
                <HStack gap="10px">
                  {filteredState.map((state) => (
                    <StateBadge key={state} state={state as TaskInstanceState}>
                      {state === "none" ? "No Status" : capitalize(state)}
                    </StateBadge>
                  ))}
                </HStack>
              ) : (
                "All States"
              )
            }
          </Select.ValueText>
        </Select.Trigger>
        <Select.Content>
          {stateOptions.items.map((option) => (
            <Select.Item item={option} key={option.label}>
              {option.value === "all" ? (
                option.label
              ) : (
                <StateBadge state={option.value as TaskInstanceState}>{option.label}</StateBadge>
              )}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
      <SearchBar
        buttonProps={{ disabled: true }}
        defaultValue={taskDisplayNamePattern ?? ""}
        hideAdvanced
        hotkeyDisabled={Boolean(runId)}
        onChange={handleSearchChange}
        placeHolder="Search Tasks"
      />
    </HStack>
  );
};
