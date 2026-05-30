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
import { useState } from "react";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useDagTagsInfinite } from "src/queries/useDagTagsInfinite";

import {
  type DagsBooleanFilterValue,
  type DagsRunStateFilterValue,
  useDagsFilterParams,
} from "../useDagsFilterParams";
import { useTagFilter } from "../useTagFilter";
import { FavoriteFilter } from "./FavoriteFilter";
import { PausedFilter } from "./PausedFilter";
import { RequiredActionFilter } from "./RequiredActionFilter";
import { StateFilters } from "./StateFilters";
import { TagFilter } from "./TagFilter";

const {
  OFFSET: OFFSET_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

const toBooleanFilterValue = (
  value: DagsBooleanFilterValue | null,
  defaultValue: DagsBooleanFilterValue = "all",
): DagsBooleanFilterValue => value ?? defaultValue;

export const DagsFilters = () => {
  const [searchParams] = useSearchParams();
  const { selectedTags, setSelectedTags, setTagFilterMode, tagFilterMode } = useTagFilter();
  const {
    favoriteFilter,
    lastDagRunStateFilter,
    needsReviewFilter,
    pausedFilter,
    setFavoriteFilter,
    setLastDagRunStateFilter,
    setNeedsReviewFilter,
    setPausedFilter,
  } = useDagsFilterParams();

  const [pattern, setPattern] = useState("");

  const { data, fetchNextPage, fetchPreviousPage } = useDagTagsInfinite({
    limit: 10,
    orderBy: ["name"],
    tagNamePattern: pattern,
  });

  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const defaultShowPaused: DagsBooleanFilterValue = hidePausedDagsByDefault ? "false" : "all";

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  const resetPagination = () => {
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    searchParams.delete(OFFSET_PARAM);
  };

  const handlePausedChange = (value: DagsBooleanFilterValue) => {
    resetPagination();
    setPausedFilter(value, hidePausedDagsByDefault);
  };

  const handleFavoriteChange = (value: DagsBooleanFilterValue) => {
    resetPagination();
    setFavoriteFilter(value);
  };

  const handleStateChange = (value: DagsRunStateFilterValue) => {
    resetPagination();
    setLastDagRunStateFilter(value);
  };

  const handleNeedsReviewToggle = () => {
    resetPagination();
    setNeedsReviewFilter(needsReviewFilter === "true" ? "all" : "true");
  };

  const handleSelectTagsChange = (
    tags: MultiValue<{
      label: string;
      value: string;
    }>,
  ) => {
    setSelectedTags(tags.map(({ value }) => value));
  };

  const handleTagModeChange = ({ checked }: { checked: boolean }) => {
    setTagFilterMode(checked ? "all" : "any");
  };

  const stateValue = lastDagRunStateFilter ?? "all";
  const pausedValue = toBooleanFilterValue(pausedFilter, defaultShowPaused);
  const favoriteValue = toBooleanFilterValue(favoriteFilter);

  return (
    <HStack flexWrap="wrap" gap={2} justifyContent="space-between">
      <Box overflowX="auto">
        <StateFilters onChange={handleStateChange} value={stateValue} />
      </Box>
      <RequiredActionFilter needsReview={needsReviewFilter === "true"} onToggle={handleNeedsReviewToggle} />
      <PausedFilter onChange={handlePausedChange} value={pausedValue} />
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
      <FavoriteFilter onChange={handleFavoriteChange} value={favoriteValue} />
    </HStack>
  );
};
