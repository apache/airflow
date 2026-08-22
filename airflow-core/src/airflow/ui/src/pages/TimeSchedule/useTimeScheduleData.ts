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
import { useQueries } from "@tanstack/react-query";
import { useState } from "react";
import { useSearchParams } from "react-router-dom";

import { useDagRunServiceGetDagRuns, useDagServiceGetDagsUi } from "openapi/queries";
import { DagRunService, DagService } from "openapi/requests/services.gen";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useDagTagsInfinite } from "src/queries/useDagTagsInfinite";
import { useDagTimetableTypesInfinite } from "src/queries/useDagTimetableTypesInfinite";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

import { aggregateTimelineItems, buildTimelineItems, buildTimelineRows } from "./timelineUtils";
import type { AggregationMode, DagRunLimit, RowSortMode, TimeScale, ViewMode } from "./types";

const DEFAULT_PAGE_SIZE = 100;

type UseTimeScheduleDataProps = {
  readonly aggregationMode: AggregationMode;
  readonly dagRunLimit: DagRunLimit;
  readonly rowSortMode: RowSortMode;
  readonly selectedTimezone: string;
  readonly showScheduledOnly: boolean;
  readonly timeScale: TimeScale;
  readonly viewMode: ViewMode;
};

export const getAdditionalPageOffsets = ({
  pageSize,
  requestedEntryCount,
  totalEntries,
}: {
  pageSize: number;
  requestedEntryCount: number;
  totalEntries: number;
}) =>
  Array.from(
    { length: Math.max(0, Math.ceil(Math.min(totalEntries, requestedEntryCount) / pageSize) - 1) },
    (_, index) => (index + 1) * pageSize,
  );

export const useTimeScheduleData = ({
  aggregationMode,
  dagRunLimit,
  rowSortMode,
  selectedTimezone,
  showScheduledOnly,
  timeScale,
  viewMode,
}: UseTimeScheduleDataProps) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [tagPattern, setTagPattern] = useState("");
  const [timetableTypePattern, setTimetableTypePattern] = useState("");
  const pageSize = (useConfig("fallback_page_limit") as number | undefined) ?? DEFAULT_PAGE_SIZE;
  const multiTeamEnabled = Boolean(useConfig("multi_team"));
  const selectedTags = searchParams.getAll(SearchParamsKeys.TAGS).filter(Boolean);
  const selectedTeams = multiTeamEnabled ? searchParams.getAll(SearchParamsKeys.TEAMS).filter(Boolean) : [];
  const selectedTimetableTypes = searchParams.getAll(SearchParamsKeys.TIMETABLE_TYPE).filter(Boolean);
  const tagFilterMode: "all" | "any" =
    searchParams.get(SearchParamsKeys.TAGS_MATCH_MODE) === "all" ? "all" : "any";
  const searchParamKeys: Array<FilterableSearchParamsKeys> = [
    SearchParamsKeys.DAG_ID_PATTERN,
    SearchParamsKeys.STATE,
    SearchParamsKeys.RUN_TYPE,
    SearchParamsKeys.RUN_AFTER_RANGE,
    SearchParamsKeys.START_DATE_RANGE,
    SearchParamsKeys.DURATION_GTE,
    SearchParamsKeys.DURATION_LTE,
  ];

  if (multiTeamEnabled) {
    searchParamKeys.push(SearchParamsKeys.TEAMS);
  }
  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(searchParamKeys);
  const { data: tagData, fetchNextPage: fetchNextTagPage } = useDagTagsInfinite({
    limit: 10,
    orderBy: ["name"],
    tagNamePattern: tagPattern,
  });
  const { data: timetableTypeData, fetchNextPage: fetchNextTimetableTypePage } = useDagTimetableTypesInfinite(
    { limit: 10, timetableTypePrefixPattern: timetableTypePattern },
  );

  const updateSearchParamValues = (key: string, values: Array<string>) => {
    setSearchParams((previousSearchParams) => {
      const nextSearchParams = new URLSearchParams(previousSearchParams);

      nextSearchParams.delete(key);
      values.forEach((value) => nextSearchParams.append(key, value));

      return nextSearchParams;
    });
  };

  const handleTagFilterModeChange = ({ checked }: { checked: boolean }) => {
    setSearchParams((previousSearchParams) => {
      const nextSearchParams = new URLSearchParams(previousSearchParams);

      nextSearchParams.set(SearchParamsKeys.TAGS_MATCH_MODE, checked ? "all" : "any");

      return nextSearchParams;
    });
  };

  const dagIdPattern = searchParams.get(SearchParamsKeys.DAG_ID_PATTERN);
  const state = searchParams.get(SearchParamsKeys.STATE);
  const runType = searchParams.get(SearchParamsKeys.RUN_TYPE);
  const runAfterGte = searchParams.get(SearchParamsKeys.RUN_AFTER_GTE);
  const runAfterLte = searchParams.get(SearchParamsKeys.RUN_AFTER_LTE);
  const startDateGte = searchParams.get(SearchParamsKeys.START_DATE_GTE);
  const startDateLte = searchParams.get(SearchParamsKeys.START_DATE_LTE);
  const durationGte = searchParams.get(SearchParamsKeys.DURATION_GTE);
  const durationLte = searchParams.get(SearchParamsKeys.DURATION_LTE);
  const dagRunQueryParams = {
    dagId: "~",
    dagIdPattern: dagIdPattern ?? undefined,
    durationGte: durationGte !== null && durationGte !== "" ? Number(durationGte) : undefined,
    durationLte: durationLte !== null && durationLte !== "" ? Number(durationLte) : undefined,
    limit: pageSize,
    orderBy: ["-start_date"],
    runAfterGte: runAfterGte ?? undefined,
    runAfterLte: runAfterLte ?? undefined,
    runType: runType === null ? undefined : [runType],
    startDateGte: startDateGte ?? undefined,
    startDateLte: startDateLte ?? undefined,
    state: state === null ? undefined : [state],
  };
  const dagQueryParams = {
    dagIdPattern: dagIdPattern ?? undefined,
    dagRunsLimit: 0,
    limit: pageSize,
    orderBy: ["dag_id"],
    tags: selectedTags.length > 0 ? selectedTags : undefined,
    tagsMatchMode: selectedTags.length > 0 ? tagFilterMode : undefined,
    teams: selectedTeams.length > 0 ? selectedTeams : undefined,
    timetableType: selectedTimetableTypes.length > 0 ? selectedTimetableTypes : undefined,
  };
  const {
    data: dagRunsData,
    error: dagRunsError,
    isLoading: isDagRunsLoading,
  } = useDagRunServiceGetDagRuns(dagRunQueryParams, undefined, { placeholderData: (previous) => previous });
  const requestedDagRunCount = dagRunLimit === "all" ? dagRunsData?.total_entries : dagRunLimit;
  const additionalDagRunPages = useQueries({
    queries: getAdditionalPageOffsets({
      pageSize,
      requestedEntryCount: requestedDagRunCount ?? 0,
      totalEntries: dagRunsData?.total_entries ?? 0,
    }).map((offset) => ({
      queryFn: () => DagRunService.getDagRuns({ ...dagRunQueryParams, offset }),
      queryKey: ["time-schedule-dag-runs", dagRunQueryParams, offset],
    })),
  });
  const {
    data: dagsData,
    error: dagsError,
    isLoading: isDagsLoading,
  } = useDagServiceGetDagsUi(dagQueryParams, undefined, { placeholderData: (previous) => previous });
  const additionalDagPages = useQueries({
    queries: getAdditionalPageOffsets({
      pageSize,
      requestedEntryCount: dagsData?.total_entries ?? 0,
      totalEntries: dagsData?.total_entries ?? 0,
    }).map((offset) => ({
      queryFn: () => DagService.getDagsUi({ ...dagQueryParams, offset }),
      queryKey: ["time-schedule-dags", dagQueryParams, offset],
    })),
  });
  const scheduledDags = [
    ...(dagsData?.dags ?? []),
    ...additionalDagPages.flatMap((page) => page.data?.dags ?? []),
  ];
  const scheduledDagIds = new Set(
    scheduledDags.filter((dag) => dag.timetable_periodic).map((dag) => dag.dag_id),
  );
  const visibleDagIds = new Set(scheduledDags.map((dag) => dag.dag_id));
  const metadataFilterApplied =
    selectedTags.length > 0 || selectedTeams.length > 0 || selectedTimetableTypes.length > 0;
  const visibleDagRuns = [
    ...(dagRunsData?.dag_runs ?? []),
    ...additionalDagRunPages.flatMap((page) => page.data?.dag_runs ?? []),
  ].filter(
    (dagRun) =>
      (!metadataFilterApplied || visibleDagIds.has(dagRun.dag_id)) &&
      (!showScheduledOnly || scheduledDagIds.has(dagRun.dag_id)),
  );
  const dagIdsWithRuns = new Set(visibleDagRuns.map((dagRun) => dagRun.dag_id));
  const plannedDags = scheduledDags.filter(
    (dag) =>
      !dagIdsWithRuns.has(dag.dag_id) &&
      dag.timetable_periodic &&
      dag.timetable_summary !== null &&
      dag.next_dagrun_run_after !== null,
  );
  const dagDetailQueries = useQueries({
    queries: plannedDags.map((dag) => ({
      queryFn: () => DagService.getDagDetails({ dagId: dag.dag_id }),
      queryKey: ["time-schedule-dag-details", dag.dag_id],
    })),
  });
  const dagRunTimeouts = new Map(
    dagDetailQueries.flatMap((query) =>
      query.data ? [[query.data.dag_id, query.data.dag_run_timeout] as const] : [],
    ),
  );
  const timelineItems = buildTimelineItems({
    dagRuns: visibleDagRuns,
    dagRunTimeouts,
    includeAllDags: !showScheduledOnly,
    scheduledDags,
  });
  const aggregatedDayItems = aggregateTimelineItems({
    aggregationMode,
    items: timelineItems,
    selectedTimezone,
    timeScale,
    viewMode: "day",
  });
  const aggregatedWeekItems = aggregateTimelineItems({
    aggregationMode,
    items: timelineItems,
    selectedTimezone,
    timeScale,
    viewMode: "week",
  });
  const dayRows = buildTimelineRows({
    items: aggregatedDayItems,
    rowSortMode,
    selectedTimezone,
  });
  const error =
    dagRunsError ??
    dagsError ??
    additionalDagPages.find((query) => query.error !== null)?.error ??
    additionalDagRunPages.find((query) => query.error !== null)?.error ??
    dagDetailQueries.find((query) => query.error !== null)?.error;

  return {
    aggregatedWeekItems,
    controls: {
      filterConfigs,
      initialValues,
      onFiltersChange: handleFiltersChange,
      onSelectTagsChange: (tags: Array<string>) => updateSearchParamValues(SearchParamsKeys.TAGS, tags),
      onTagFilterModeChange: handleTagFilterModeChange,
      onTagInputChange: setTagPattern,
      onTagMenuScrollToBottom: () => void fetchNextTagPage(),
      onTimetableTypeChange: (types: Array<string>) =>
        updateSearchParamValues(SearchParamsKeys.TIMETABLE_TYPE, types),
      onTimetableTypeInputChange: setTimetableTypePattern,
      onTimetableTypeMenuScrollToBottom: () => void fetchNextTimetableTypePage(),
      selectedTags,
      selectedTimetableTypes,
      tagFilterMode,
      tags: tagData?.pages.flatMap((response) => response.tags) ?? [],
      timetableTypes: timetableTypeData?.pages.flatMap((response) => response.timetable_types) ?? [],
    },
    dagRunCount: visibleDagRuns.length,
    dayRows: viewMode === "day" ? dayRows : [],
    error,
    isLoading:
      isDagRunsLoading ||
      isDagsLoading ||
      additionalDagPages.some((page) => page.isLoading) ||
      additionalDagRunPages.some((page) => page.isLoading) ||
      dagDetailQueries.some((query) => query.isLoading),
    timelineItems,
  };
};
