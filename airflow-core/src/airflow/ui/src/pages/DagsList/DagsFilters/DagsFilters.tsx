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
import { Box, HStack } from "@chakra-ui/react";
import type { MultiValue } from "chakra-react-select";
import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";

import { useDagServiceGetDagTags } from "openapi/queries";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";

import { PausedFilter } from "./PausedFilter";
import { ResetButton } from "./ResetButton";
import { StateFilters } from "./StateFilters";
import { TagFilter } from "./TagFilter";

const {
  LAST_DAG_RUN_STATE: LAST_DAG_RUN_STATE_PARAM,
  OFFSET: OFFSET_PARAM,
  PAUSED: PAUSED_PARAM,
  TAGS: TAGS_PARAM,
  TAGS_MATCH_MODE: TAGS_MATCH_MODE_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const getFilterCount = (state: string | null, showPaused: string | null, selectedTags: Array<string>) => {
  let count = 0;

  if (state !== null) {
    count += 1;
  }
  if (showPaused !== null) {
    count += 1;
  }
  if (selectedTags.length > 0) {
    count += 1;
  }

  return count;
};

export const DagsFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const showPaused = searchParams.get(PAUSED_PARAM);
  const state = searchParams.get(LAST_DAG_RUN_STATE_PARAM);
  const selectedTags = searchParams.getAll(TAGS_PARAM);
  const tagFilterMode = searchParams.get(TAGS_MATCH_MODE_PARAM) ?? "any";
  const isAll = state === null;
  const isRunning = state === "running";
  const isFailed = state === "failed";
  const isSuccess = state === "success";

  const { data } = useDagServiceGetDagTags({
    orderBy: "name",
  });

  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const defaultShowPaused = hidePausedDagsByDefault ? "false" : "all";

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  const handlePausedChange = useCallback(
    ({ value }: { value: Array<string> }) => {
      const [val] = value;

      if (val === undefined || val === "all") {
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
      if (tags.length < 2) {
        searchParams.delete(TAGS_MATCH_MODE_PARAM);
      }
      searchParams.delete(OFFSET_PARAM);
      setSearchParams(searchParams);
    },
    [searchParams, setSearchParams],
  );

  const onClearFilters = () => {
    searchParams.delete(PAUSED_PARAM);
    searchParams.delete(LAST_DAG_RUN_STATE_PARAM);
    searchParams.delete(TAGS_PARAM);
    searchParams.delete(TAGS_MATCH_MODE_PARAM);

    setSearchParams(searchParams);
  };

  const handleTagModeChange = useCallback(
    ({ checked }: { checked: boolean }) => {
      const mode = checked ? "all" : "any";

      searchParams.set(TAGS_MATCH_MODE_PARAM, mode);
      setSearchParams(searchParams);
    },
    [searchParams, setSearchParams],
  );

  const filterCount = getFilterCount(state, showPaused, selectedTags);

  return (
    <HStack justifyContent="space-between">
      <HStack gap={4}>
        <StateFilters
          isAll={isAll}
          isFailed={isFailed}
          isRunning={isRunning}
          isSuccess={isSuccess}
          onStateChange={handleStateChange}
        />
        <PausedFilter
          defaultShowPaused={defaultShowPaused}
          onPausedChange={handlePausedChange}
          showPaused={showPaused}
        />
        <TagFilter
          onSelectTagsChange={handleSelectTagsChange}
          onTagModeChange={handleTagModeChange}
          selectedTags={selectedTags}
          tagFilterMode={tagFilterMode}
          tags={data?.tags ?? []}
        />
      </HStack>
      <Box>
        <ResetButton filterCount={filterCount} onClearFilters={onClearFilters} />
      </Box>
    </HStack>
  );
};
