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
import { HStack } from "@chakra-ui/react";
import type { MultiValue } from "chakra-react-select";
import { useState } from "react";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useDagTagsInfinite } from "src/queries/useDagTagsInfinite";

import { FavoriteFilter } from "./FavoriteFilter";
import { PausedFilter } from "./PausedFilter";
import { RequiredActionFilter } from "./RequiredActionFilter";
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

type StateValue = "all" | "failed" | "queued" | "running" | "success";
type BooleanFilterValue = "all" | "false" | "true";

const stateValues: ReadonlyArray<StateValue> = ["failed", "queued", "running", "success"];
const booleanFilterValues: ReadonlyArray<BooleanFilterValue> = ["all", "true", "false"];

const toStateValue = (value: string | null): StateValue =>
  stateValues.includes(value as StateValue) ? (value as StateValue) : "all";

const toBooleanFilterValue = (
  value: string | null,
  defaultValue: BooleanFilterValue = "all",
): BooleanFilterValue =>
  booleanFilterValues.includes(value as BooleanFilterValue) ? (value as BooleanFilterValue) : defaultValue;

export const DagsFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const showPaused = searchParams.get(PAUSED_PARAM);
  const showFavorites = searchParams.get(FAVORITE_PARAM);
  const needsReview = searchParams.get(NEEDS_REVIEW_PARAM);
  const state = searchParams.get(LAST_DAG_RUN_STATE_PARAM);
  const selectedTags = searchParams.getAll(TAGS_PARAM);
  const tagFilterMode = searchParams.get(TAGS_MATCH_MODE_PARAM) ?? "any";

  const [pattern, setPattern] = useState("");

  const { data, fetchNextPage, fetchPreviousPage } = useDagTagsInfinite({
    limit: 10,
    orderBy: ["name"],
    tagNamePattern: pattern,
  });

  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const defaultShowPaused: BooleanFilterValue = hidePausedDagsByDefault ? "false" : "all";

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  const resetPagination = () => {
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
    searchParams.delete(OFFSET_PARAM);
  };

  const handlePausedChange = (value: BooleanFilterValue) => {
    if (value === "all" && !hidePausedDagsByDefault) {
      searchParams.delete(PAUSED_PARAM);
    } else {
      searchParams.set(PAUSED_PARAM, value);
    }
    resetPagination();
    setSearchParams(searchParams);
  };

  const handleFavoriteChange = (value: BooleanFilterValue) => {
    if (value === "all") {
      searchParams.delete(FAVORITE_PARAM);
    } else {
      searchParams.set(FAVORITE_PARAM, value);
    }
    resetPagination();
    setSearchParams(searchParams);
  };

  const handleStateChange = (value: StateValue) => {
    if (value === "all") {
      searchParams.delete(LAST_DAG_RUN_STATE_PARAM);
    } else {
      searchParams.set(LAST_DAG_RUN_STATE_PARAM, value);
    }
    resetPagination();
    setSearchParams(searchParams);
  };

  const handleNeedsReviewToggle = () => {
    if (needsReview === "true") {
      searchParams.delete(NEEDS_REVIEW_PARAM);
    } else {
      searchParams.set(NEEDS_REVIEW_PARAM, "true");
    }
    resetPagination();
    setSearchParams(searchParams);
  };

  const handleSelectTagsChange = (
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
  };

  const handleTagModeChange = ({ checked }: { checked: boolean }) => {
    const mode = checked ? "all" : "any";

    searchParams.set(TAGS_MATCH_MODE_PARAM, mode);
    setSearchParams(searchParams);
  };

  const stateValue = toStateValue(state);
  const pausedValue = toBooleanFilterValue(showPaused, defaultShowPaused);
  const favoriteValue = toBooleanFilterValue(showFavorites);

  return (
    <HStack flexWrap="wrap" gap={2} justifyContent="space-between">
      <StateFilters onChange={handleStateChange} value={stateValue} />
      <RequiredActionFilter needsReview={needsReview === "true"} onToggle={handleNeedsReviewToggle} />
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
