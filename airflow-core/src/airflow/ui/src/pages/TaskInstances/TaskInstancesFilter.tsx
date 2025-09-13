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
import { useTranslation } from "react-i18next";
import { useSearchParams, useParams } from "react-router-dom";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchBar } from "src/components/SearchBar";
import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { taskInstanceStateOptions } from "src/constants/stateOptions";

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
  const { t: translate } = useTranslation();

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
    <HStack paddingY="4px">
      <SearchBar
        buttonProps={{ disabled: true }}
        defaultValue={taskDisplayNamePattern ?? ""}
        hideAdvanced
        hotkeyDisabled={Boolean(runId)}
        onChange={handleSearchChange}
        placeHolder={translate("dags:search.tasks")}
      />
      <Select.Root
        collection={taskInstanceStateOptions}
        maxW="450px"
        multiple
        onValueChange={handleStateChange}
        value={hasFilteredState ? filteredState : ["all"]}
      >
        <Select.Trigger
          {...(hasFilteredState ? { clearable: true } : {})}
          colorPalette="brand"
          isActive={Boolean(filteredState)}
        >
          <Select.ValueText>
            {() =>
              hasFilteredState ? (
                <HStack flexWrap="wrap" fontSize="sm" gap="4px" paddingY="8px">
                  {filteredState.map((state) => (
                    <StateBadge key={state} state={state as TaskInstanceState}>
                      {translate(`common:states.${state}`)}
                    </StateBadge>
                  ))}
                </HStack>
              ) : (
                translate("dags:filters.allStates")
              )
            }
          </Select.ValueText>
        </Select.Trigger>
        <Select.Content>
          {taskInstanceStateOptions.items.map((option) => (
            <Select.Item item={option} key={option.label}>
              {option.value === "all" ? (
                translate(option.label)
              ) : (
                <StateBadge state={option.value as TaskInstanceState}>{translate(option.label)}</StateBadge>
              )}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
    </HStack>
  );
};
