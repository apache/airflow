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
import {
  Box,
  Button,
  createListCollection,
  Field,
  HStack,
  type SelectValueChangeDetails,
} from "@chakra-ui/react";
import { Select as ReactSelect, type MultiValue } from "chakra-react-select";
import { useCallback, useState } from "react";
import { LuX } from "react-icons/lu";
import { useSearchParams } from "react-router-dom";

import { useDagTagsInfinite } from "openapi/queries/useDagsInfinite";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { QuickFilterButton } from "src/components/QuickFilterButton";
import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { pluralize } from "src/utils";

const {
  LAST_DAG_RUN_STATE: LAST_DAG_RUN_STATE_PARAM,
  OFFSET: OFFSET_PARAM,
  PAUSED: PAUSED_PARAM,
  TAGS: TAGS_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const enabledOptions = createListCollection({
  items: [
    { label: "All", value: "all" },
    { label: "Enabled", value: "false" },
    { label: "Disabled", value: "true" },
  ],
});

export const DagsFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const showPaused = searchParams.get(PAUSED_PARAM);
  const state = searchParams.get(LAST_DAG_RUN_STATE_PARAM);
  const selectedTags = searchParams.getAll(TAGS_PARAM);
  const isAll = state === null;
  const isRunning = state === "running";
  const isFailed = state === "failed";
  const isSuccess = state === "success";

  const [pattern, setPattern] = useState("");

  const { data, fetchNextPage, fetchPreviousPage } = useDagTagsInfinite({
    limit: 10,
    orderBy: "name",
    tagNamePattern: pattern,
  });

  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const defaultShowPaused = hidePausedDagsByDefault ? "false" : "all";

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  const handlePausedChange = useCallback(
    ({ value }: SelectValueChangeDetails<string>) => {
      const [val] = value;

      if (val === undefined) {
        searchParams.delete(PAUSED_PARAM);
      } else {
        searchParams.set(PAUSED_PARAM, val);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      searchParams.delete(OFFSET_PARAM);
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );

  const handleStateChange: React.MouseEventHandler<HTMLButtonElement> = useCallback(
    ({ currentTarget: { value } }) => {
      if (value === "all") {
        searchParams.delete(LAST_DAG_RUN_STATE_PARAM);
      } else {
        searchParams.set(LAST_DAG_RUN_STATE_PARAM, value);
      }
      setTableURLState({
        pagination: { ...pagination, pageIndex: 0 },
        sorting,
      });
      searchParams.delete(OFFSET_PARAM);
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, sorting],
  );
  const handleSelectTagsChange = useCallback(
    (
      tags: MultiValue<{
        label: string;
        value: string;
      }>,
    ) => {
      searchParams.delete(TAGS_PARAM);
      tags.forEach(({ value }) => {
        searchParams.append(TAGS_PARAM, value);
      });
      searchParams.delete(OFFSET_PARAM);
      setSearchParams(searchParams);
    },
    [searchParams, setSearchParams],
  );

  const onClearFilters = () => {
    searchParams.delete(PAUSED_PARAM);
    searchParams.delete(LAST_DAG_RUN_STATE_PARAM);
    searchParams.delete(TAGS_PARAM);

    setSearchParams(searchParams);
    setPattern("");
  };

  let filterCount = 0;

  if (state !== null) {
    filterCount += 1;
  }
  if (showPaused !== null) {
    filterCount += 1;
  }
  if (selectedTags.length > 0) {
    filterCount += 1;
  }

  return (
    <HStack justifyContent="space-between">
      <HStack gap={4}>
        <HStack>
          <QuickFilterButton isActive={isAll} onClick={handleStateChange} value="all">
            All
          </QuickFilterButton>
          <QuickFilterButton
            data-testid="dags-failed-filter"
            isActive={isFailed}
            onClick={handleStateChange}
            value="failed"
          >
            <StateBadge state="failed" />
            Failed
          </QuickFilterButton>
          <QuickFilterButton
            data-testid="dags-running-filter"
            isActive={isRunning}
            onClick={handleStateChange}
            value="running"
          >
            <StateBadge state="running" />
            Running
          </QuickFilterButton>
          <QuickFilterButton
            data-testid="dags-success-filter"
            isActive={isSuccess}
            onClick={handleStateChange}
            value="success"
          >
            <StateBadge state="success" />
            Success
          </QuickFilterButton>
        </HStack>
        <Select.Root
          collection={enabledOptions}
          onValueChange={handlePausedChange}
          value={[showPaused ?? defaultShowPaused]}
        >
          <Select.Trigger colorPalette="blue" isActive={Boolean(showPaused)}>
            <Select.ValueText width={20} />
          </Select.Trigger>
          <Select.Content>
            {enabledOptions.items.map((option) => (
              <Select.Item item={option} key={option.label}>
                {option.label}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
        <Field.Root>
          <ReactSelect
            aria-label="Filter Dags by tag"
            chakraStyles={{
              clearIndicator: (provided) => ({
                ...provided,
                color: "gray.fg",
              }),
              container: (provided) => ({
                ...provided,
                minWidth: 64,
              }),
              control: (provided) => ({
                ...provided,
                colorPalette: "blue",
              }),
              menu: (provided) => ({
                ...provided,
                zIndex: 2,
              }),
            }}
            isClearable
            isMulti
            noOptionsMessage={() => "No tags found"}
            onChange={handleSelectTagsChange}
            onInputChange={(newValue) => setPattern(newValue)}
            onMenuScrollToBottom={() => {
              void fetchNextPage();
            }}
            onMenuScrollToTop={() => {
              void fetchPreviousPage();
            }}
            options={
              data?.pages.flatMap((response) =>
                response.tags.map((tag) => ({
                  label: tag,
                  value: tag,
                })),
              ) ?? []
            }
            placeholder="Filter by tag"
            value={selectedTags.map((tag) => ({
              label: tag,
              value: tag,
            }))}
          />
        </Field.Root>
      </HStack>
      <Box>
        {filterCount > 0 && (
          <Button onClick={onClearFilters} size="sm" variant="outline">
            <LuX /> Reset {pluralize("filter", filterCount)}
          </Button>
        )}
      </Box>
    </HStack>
  );
};
