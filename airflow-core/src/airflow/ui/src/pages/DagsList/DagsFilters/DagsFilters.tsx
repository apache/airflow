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
import { useCallback, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ResetButton } from "src/components/ui";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useDagTagsInfinite } from "src/queries/useDagTagsInfinite";
import { getFilterCount } from "src/utils/filterUtils";

import { FavoriteFilter } from "./FavoriteFilter";
import { PausedFilter } from "./PausedFilter";
import { StateFilters } from "./StateFilters";
import { TagFilter } from "./TagFilter";

const {
  FAVORITE: FAVORITE_PARAM,
  LAST_DAG_RUN_STATE: LAST_DAG_RUN_STATE_PARAM,
  NEEDS_REVIEW: NEEDS_REVIEW_PARAM,
  OFFSET: OFFSET_PARAM,
  PAUSED: PAUSED_PARAM,
  TAGS: TAGS_PARAM,
  TAGS_MATCH_MODE: TAGS_MATCH_MODE_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

export const DagsFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const showPaused = searchParams.get(PAUSED_PARAM);
  const showFavorites = searchParams.get(FAVORITE_PARAM);
  const needsReview = searchParams.get(NEEDS_REVIEW_PARAM);
  const state = searchParams.get(LAST_DAG_RUN_STATE_PARAM);
  const selectedTags = searchParams.getAll(TAGS_PARAM);
  const tagFilterMode = searchParams.get(TAGS_MATCH_MODE_PARAM) ?? "any";
  const isAll = state === null;
  const isRunning = state === "running";
  const isFailed = state === "failed";
  const isQueued = state === "queued";
  const isSuccess = state === "success";

  const [pattern, setPattern] = useState("");

  const { data, fetchNextPage, fetchPreviousPage } = useDagTagsInfinite({
    limit: 10,
    orderBy: ["name"],
    tagNamePattern: pattern,
  });

  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const defaultShowPaused = hidePausedDagsByDefault ? "false" : "all";

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  const handlePausedChange = useCallback(
    ({ value }: { value: Array<string> }) => {
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

  const handleFavoriteChange = useCallback(
    ({ value }: { value: Array<string> }) => {
      const [val] = value;

      if (val === undefined || val === "all") {
        searchParams.delete(FAVORITE_PARAM);
      } else {
        searchParams.set(FAVORITE_PARAM, val);
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
        searchParams.delete(NEEDS_REVIEW_PARAM);
      } else if (value === "needs_review") {
        searchParams.set(NEEDS_REVIEW_PARAM, "true");
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
    searchParams.delete(FAVORITE_PARAM);
    searchParams.delete(NEEDS_REVIEW_PARAM);
    searchParams.delete(LAST_DAG_RUN_STATE_PARAM);
    searchParams.delete(TAGS_PARAM);
    searchParams.delete(TAGS_MATCH_MODE_PARAM);

    setSearchParams(searchParams);
    setPattern("");
  };

  const handleTagModeChange = useCallback(
    ({ checked }: { checked: boolean }) => {
      const mode = checked ? "all" : "any";

      searchParams.set(TAGS_MATCH_MODE_PARAM, mode);
      setSearchParams(searchParams);
    },
    [searchParams, setSearchParams],
  );

  const filterCount = getFilterCount({
    needsReview,
    selectedTags,
    showFavorites,
    showPaused,
    state,
  });

  return (
    <HStack justifyContent="space-between">
      <HStack gap={4}>
        <StateFilters
          isAll={isAll}
          isFailed={isFailed}
          isQueued={isQueued}
          isRunning={isRunning}
          isSuccess={isSuccess}
          needsReview={needsReview === "true"}
          onStateChange={handleStateChange}
        />
        <PausedFilter
          defaultShowPaused={defaultShowPaused}
          onPausedChange={handlePausedChange}
          showPaused={showPaused}
        />
        <TagFilter
          onMenuScrollToBottom={() => {
            void fetchNextPage();
          }}
          onMenuScrollToTop={() => {
            void fetchPreviousPage();
          }}
          onSelectTagsChange={handleSelectTagsChange}
          onTagModeChange={handleTagModeChange}
          onUpdate={setPattern}
          selectedTags={selectedTags}
          tagFilterMode={tagFilterMode}
          tags={data?.pages.flatMap((dagResponse) => dagResponse.tags) ?? []}
        />
        <FavoriteFilter onFavoriteChange={handleFavoriteChange} showFavorites={showFavorites} />
      </HStack>
      <Box>
        <ResetButton filterCount={filterCount} onClearFilters={onClearFilters} />
      </Box>
    </HStack>
  );
};
