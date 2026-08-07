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
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useDagTagsInfinite } from "src/queries/useDagTagsInfinite";
import { useDagTimetableTypesInfinite } from "src/queries/useDagTimetableTypesInfinite";

import { useTagFilter } from "../useTagFilter";
import { FavoriteFilter } from "./FavoriteFilter";
import { PausedFilter } from "./PausedFilter";
import { RequiredActionFilter } from "./RequiredActionFilter";
import { RunStateLookbackSelect, type RunStateLookback } from "./RunStateLookbackSelect";
import { RunStateSelect } from "./RunStateSelect";
import { TagFilter } from "./TagFilter";
import { TeamFilter } from "./TeamFilter";
import { TimetableTypeFilter } from "./TimetableTypeFilter";
import { runLookbackFor, runStateSelectionFor } from "./runStateFilter";

const {
  DAG_RUN_STATE: DAG_RUN_STATE_PARAM,
  DAG_RUN_STATE_WITHIN_HOURS: DAG_RUN_STATE_WITHIN_HOURS_PARAM,
  FAVORITE: FAVORITE_PARAM,
  LAST_DAG_RUN_STATE: LAST_DAG_RUN_STATE_PARAM,
  NEEDS_REVIEW: NEEDS_REVIEW_PARAM,
  OFFSET: OFFSET_PARAM,
  PAUSED: PAUSED_PARAM,
  TEAMS: TEAMS_PARAM,
  TIMETABLE_TYPE: TIMETABLE_TYPE_PARAM,
}: SearchParamsKeysType = SearchParamsKeys;

type BooleanFilterValue = "all" | "false" | "true";

const runStates = ["failed", "queued", "running", "success"] as const;
const booleanFilterValues: ReadonlyArray<BooleanFilterValue> = ["all", "true", "false"];

const toBooleanFilterValue = (
  value: string | null,
  defaultValue: BooleanFilterValue = "all",
): BooleanFilterValue =>
  booleanFilterValues.includes(value as BooleanFilterValue) ? (value as BooleanFilterValue) : defaultValue;

export const DagsFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { t: translate } = useTranslation("dags");
  const { selectedTags, setSelectedTags, setTagFilterMode, tagFilterMode } = useTagFilter();
  const multiTeamEnabled = Boolean(useConfig("multi_team"));

  const showPaused = searchParams.get(PAUSED_PARAM);
  const showFavorites = searchParams.get(FAVORITE_PARAM);
  const needsReview = searchParams.get(NEEDS_REVIEW_PARAM);
  const lastRunState = searchParams.get(LAST_DAG_RUN_STATE_PARAM);
  const anyRunState = searchParams.get(DAG_RUN_STATE_PARAM);
  const anyRunStateLookback = searchParams.get(DAG_RUN_STATE_WITHIN_HOURS_PARAM);
  const selectedTeams = searchParams.getAll(TEAMS_PARAM);
  const timetableTypes = searchParams.getAll(TIMETABLE_TYPE_PARAM).filter(Boolean);

  const [tagPattern, setTagPattern] = useState("");
  const [timetableTypePattern, setTimetableTypePattern] = useState("");

  // One control, two dimensions: the state plus the lookback it is matched against.
  // "latest" maps to last_dag_run_state; the time lookbacks map to dag_run_state (+ within-hours bound).
  const runState = lastRunState ?? anyRunState ?? undefined;
  const runLookback = runLookbackFor(lastRunState, anyRunState, anyRunStateLookback);

  const {
    data: tagData,
    fetchNextPage: fetchNextTagPage,
    fetchPreviousPage: fetchPreviousTagPage,
  } = useDagTagsInfinite({
    limit: 10,
    orderBy: ["name"],
    tagNamePattern: tagPattern,
  });
  const {
    data: timetableTypeData,
    fetchNextPage: fetchNextTimetableTypePage,
    fetchPreviousPage: fetchPreviousTimetableTypePage,
  } = useDagTimetableTypesInfinite({
    limit: 10,
    timetableTypePrefixPattern: timetableTypePattern,
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

  const applyRunStateParams = (state: string | undefined, lookback: RunStateLookback) => {
    const selection = runStateSelectionFor(state, lookback);
    const setOrDelete = (key: string, value: string | undefined) =>
      value === undefined ? searchParams.delete(key) : searchParams.set(key, value);

    setOrDelete(LAST_DAG_RUN_STATE_PARAM, selection.lastDagRunState);
    setOrDelete(DAG_RUN_STATE_PARAM, selection.dagRunState);
    setOrDelete(DAG_RUN_STATE_WITHIN_HOURS_PARAM, selection.dagRunStateWithinHours);
    resetPagination();
    setSearchParams(searchParams);
  };

  const handleRunStateChange = (value: string | undefined) => {
    applyRunStateParams(value, runLookback);
  };

  const handleRunLookbackChange = (lookback: RunStateLookback) => {
    applyRunStateParams(runState, lookback);
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

  const handleTimetableTypeChange = (selectedTimetableTypes: Array<string>) => {
    searchParams.delete(TIMETABLE_TYPE_PARAM);
    for (const timetableType of selectedTimetableTypes) {
      searchParams.append(TIMETABLE_TYPE_PARAM, timetableType);
    }
    resetPagination();
    setSearchParams(searchParams);
  };

  const handleSelectTagsChange = (tags: Array<string>) => {
    setSelectedTags(tags);
  };

  const handleTagModeChange = ({ checked }: { checked: boolean }) => {
    setTagFilterMode(checked ? "all" : "any");
  };

  const handleTeamsChange = (teams: Array<string>) => {
    searchParams.delete(TEAMS_PARAM);
    for (const team of teams) {
      searchParams.append(TEAMS_PARAM, team);
    }
    resetPagination();
    setSearchParams(searchParams);
  };

  const pausedValue = toBooleanFilterValue(showPaused, defaultShowPaused);
  const favoriteValue = toBooleanFilterValue(showFavorites);

  return (
    <HStack alignItems="flex-start" flexWrap="wrap" gap={2} justifyContent="space-between">
      <HStack gap={0}>
        <RunStateSelect
          dataTestId="dags-run-state-filter"
          label={translate("filters.runState")}
          onChange={handleRunStateChange}
          states={runStates}
          triggerProps={runState === undefined ? undefined : { borderEndRadius: 0, borderInlineEndWidth: 0 }}
          value={runState}
        />
        {runState === undefined ? undefined : (
          <RunStateLookbackSelect
            dataTestId="dags-run-state-lookback-filter"
            onChange={handleRunLookbackChange}
            triggerProps={{ borderStartRadius: 0 }}
            value={runLookback}
          />
        )}
      </HStack>
      <RequiredActionFilter needsReview={needsReview === "true"} onToggle={handleNeedsReviewToggle} />
      <PausedFilter onChange={handlePausedChange} value={pausedValue} />
      <TimetableTypeFilter
        onChange={handleTimetableTypeChange}
        onInputChange={setTimetableTypePattern}
        onMenuScrollToBottom={() => {
          void fetchNextTimetableTypePage();
        }}
        onMenuScrollToTop={() => {
          void fetchPreviousTimetableTypePage();
        }}
        timetableTypes={timetableTypeData?.pages.flatMap((response) => response.timetable_types) ?? []}
        values={timetableTypes}
      />
      <TagFilter
        onMenuScrollToBottom={() => {
          void fetchNextTagPage();
        }}
        onMenuScrollToTop={() => {
          void fetchPreviousTagPage();
        }}
        onSelectTagsChange={handleSelectTagsChange}
        onTagModeChange={handleTagModeChange}
        onUpdate={setTagPattern}
        selectedTags={selectedTags}
        tagFilterMode={tagFilterMode}
        tags={tagData?.pages.flatMap((dagResponse) => dagResponse.tags) ?? []}
      />
      {multiTeamEnabled ? (
        <TeamFilter onChange={handleTeamsChange} selectedTeams={selectedTeams} />
      ) : undefined}
      <Box marginInlineStart="auto">
        <FavoriteFilter onChange={handleFavoriteChange} value={favoriteValue} />
      </Box>
    </HStack>
  );
};
