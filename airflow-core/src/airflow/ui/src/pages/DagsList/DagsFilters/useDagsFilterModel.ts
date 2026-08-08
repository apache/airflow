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
import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useDagTagsInfinite } from "src/queries/useDagTagsInfinite";
import { useDagTimetableTypesInfinite } from "src/queries/useDagTimetableTypesInfinite";

import { useTagFilter } from "../useTagFilter";
import { getNormalizedDagsFilterSearchParams, getUniqueSearchParamValues } from "./normalizeDagsFilters";
import type { BooleanFilterValue, DagsFilterModel } from "./types";

const {
  DAG_RUN_STATE,
  FAVORITE,
  LAST_DAG_RUN_STATE,
  NEEDS_REVIEW,
  OFFSET,
  OWNERS,
  PAUSED,
  TAGS,
  TAGS_MATCH_MODE,
  TEAMS,
  TIMETABLE_TYPE,
}: SearchParamsKeysType = SearchParamsKeys;

const booleanFilterValues: ReadonlyArray<BooleanFilterValue> = ["all", "true", "false"];

const getBooleanFilterValue = (
  value: string | null,
  defaultValue: BooleanFilterValue = "all",
): BooleanFilterValue =>
  booleanFilterValues.includes(value as BooleanFilterValue) ? (value as BooleanFilterValue) : defaultValue;

export const useDagsFilterModel = (): DagsFilterModel => {
  const [searchParams, setSearchParams] = useSearchParams();
  const normalizedSearchParams = getNormalizedDagsFilterSearchParams(searchParams);
  const { resetSavedTagFilter, selectedTags, setSelectedTags, setTagFilterMode, tagFilterMode } =
    useTagFilter({ materializeSavedTags: false });
  const multiTeamEnabled = Boolean(useConfig("multi_team"));
  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));

  const [tagPattern, setTagPattern] = useState("");
  const [timetableTypePattern, setTimetableTypePattern] = useState("");

  const {
    data: tagData,
    error: tagError,
    fetchNextPage: fetchNextTagPage,
    fetchPreviousPage: fetchPreviousTagPage,
    hasNextPage: hasNextTagPage,
    isFetching: isFetchingTags,
    refetch: refetchTags,
  } = useDagTagsInfinite({
    limit: 10,
    orderBy: ["name"],
    tagNamePattern: tagPattern,
  });
  const {
    data: timetableTypeData,
    error: timetableTypeError,
    fetchNextPage: fetchNextTimetableTypePage,
    fetchPreviousPage: fetchPreviousTimetableTypePage,
    hasNextPage: hasNextTimetableTypePage,
    isFetching: isFetchingTimetableTypes,
    refetch: refetchTimetableTypes,
  } = useDagTimetableTypesInfinite({
    limit: 10,
    timetableTypePrefixPattern: timetableTypePattern,
  });

  useEffect(() => {
    if (normalizedSearchParams.toString() !== searchParams.toString()) {
      setSearchParams(normalizedSearchParams, { replace: true });
    }
  }, [normalizedSearchParams, searchParams, setSearchParams]);

  const updateSearchParams = (update: (params: URLSearchParams) => void) => {
    update(searchParams);
    searchParams.delete(OFFSET);
    setSearchParams(searchParams);
  };

  const setBooleanFilter = (key: string, value: BooleanFilterValue) => {
    updateSearchParams((params) => {
      if (value === "all" && !(key === PAUSED && hidePausedDagsByDefault)) {
        params.delete(key);
      } else {
        params.set(key, value);
      }
    });
  };

  const setSingleValueFilter = (key: string, value: string | undefined) => {
    updateSearchParams((params) => {
      if (value === undefined) {
        params.delete(key);
      } else {
        params.set(key, value);
      }
    });
  };

  const setMultiValueFilter = (key: string, values: Array<string>) => {
    updateSearchParams((params) => {
      params.delete(key);
      for (const value of new Set(values.filter(Boolean))) {
        params.append(key, value);
      }
    });
  };

  const clearAll = () => {
    updateSearchParams((params) => {
      for (const key of [
        DAG_RUN_STATE,
        FAVORITE,
        LAST_DAG_RUN_STATE,
        NEEDS_REVIEW,
        OWNERS,
        TAGS,
        TAGS_MATCH_MODE,
        TEAMS,
        TIMETABLE_TYPE,
      ]) {
        params.delete(key);
      }
      if (hidePausedDagsByDefault) {
        params.set(PAUSED, "all");
      } else {
        params.delete(PAUSED);
      }
    });
    resetSavedTagFilter();
    setTagPattern("");
    setTimetableTypePattern("");
  };

  const defaultPausedValue: BooleanFilterValue = hidePausedDagsByDefault ? "false" : "all";

  return {
    activeRunState: {
      onChange: (value) => setSingleValueFilter(DAG_RUN_STATE, value),
      value: normalizedSearchParams.get(DAG_RUN_STATE) ?? undefined,
    },
    clearAll,
    favorite: {
      onChange: (value) => setBooleanFilter(FAVORITE, value),
      value: getBooleanFilterValue(normalizedSearchParams.get(FAVORITE)),
    },
    lastRunState: {
      onChange: (value) => setSingleValueFilter(LAST_DAG_RUN_STATE, value),
      value: normalizedSearchParams.get(LAST_DAG_RUN_STATE) ?? undefined,
    },
    multiTeamEnabled,
    needsReview: {
      onChange: (value) => setBooleanFilter(NEEDS_REVIEW, value),
      value: getBooleanFilterValue(normalizedSearchParams.get(NEEDS_REVIEW)),
    },
    owners: {
      onChange: (values) => setMultiValueFilter(OWNERS, values),
      values: getUniqueSearchParamValues(normalizedSearchParams, OWNERS),
    },
    paused: {
      onChange: (value) => setBooleanFilter(PAUSED, value),
      value: getBooleanFilterValue(normalizedSearchParams.get(PAUSED), defaultPausedValue),
    },
    resetSuggestions: () => {
      setTagPattern("");
      setTimetableTypePattern("");
    },
    tags: {
      hasError: tagError !== null,
      hasNextPage: Boolean(hasNextTagPage),
      isLoading: isFetchingTags,
      matchMode: tagFilterMode,
      onChange: setSelectedTags,
      onInputChange: setTagPattern,
      onMatchModeChange: ({ checked }) => setTagFilterMode(checked ? "all" : "any"),
      onMenuScrollToBottom: () => {
        void fetchNextTagPage();
      },
      onMenuScrollToTop: () => {
        void fetchPreviousTagPage();
      },
      onRetry: () => {
        void refetchTags();
      },
      options: tagData?.pages.flatMap((response) => response.tags) ?? [],
      values: selectedTags,
    },
    teams: {
      onChange: (values) => setMultiValueFilter(TEAMS, values),
      values: multiTeamEnabled ? getUniqueSearchParamValues(normalizedSearchParams, TEAMS) : [],
    },
    timetableTypes: {
      hasError: timetableTypeError !== null,
      hasNextPage: Boolean(hasNextTimetableTypePage),
      isLoading: isFetchingTimetableTypes,
      onChange: (values) => setMultiValueFilter(TIMETABLE_TYPE, values),
      onInputChange: setTimetableTypePattern,
      onMenuScrollToBottom: () => {
        void fetchNextTimetableTypePage();
      },
      onMenuScrollToTop: () => {
        void fetchPreviousTimetableTypePage();
      },
      onRetry: () => {
        void refetchTimetableTypes();
      },
      options: timetableTypeData?.pages.flatMap((response) => response.timetable_types) ?? [],
      values: getUniqueSearchParamValues(normalizedSearchParams, TIMETABLE_TYPE),
    },
  };
};
