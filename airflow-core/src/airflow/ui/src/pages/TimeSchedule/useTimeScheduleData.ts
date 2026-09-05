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
import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useDebounce } from "use-debounce";

import { OpenAPI } from "openapi/requests/core/OpenAPI";
import type { TimeScheduleBatch, TimeScheduleItem } from "openapi/requests/types.gen";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

import { buildTimelineRows } from "./timelineUtils";
import type { AggregationMode, DagRunLimit, RowSortMode, TimeScale, TimelineItem, ViewMode } from "./types";

type UseTimeScheduleDataProps = {
  readonly aggregationMode: AggregationMode;
  readonly dagRunLimit: DagRunLimit;
  readonly rowSortMode: RowSortMode;
  readonly selectedTimezone: string;
  readonly showScheduledOnly: boolean;
  readonly timeScale: TimeScale;
  readonly viewMode: ViewMode;
};

const mapStreamItem = (item: TimeScheduleItem): TimelineItem => ({
  dagId: item.dag_id,
  dagRunId: item.dag_run_id,
  durationMs: item.duration_ms,
  endDate: item.end_date,
  isPlaceholder: item.is_placeholder,
  isPlanned: item.is_planned,
  isTimeScheduled: item.is_time_scheduled,
  label: item.label,
  runCount: item.run_count,
  startDate: item.start_date,
  state: item.state,
});

export const useTimeScheduleData = ({
  aggregationMode,
  dagRunLimit,
  rowSortMode,
  selectedTimezone,
  showScheduledOnly,
  timeScale,
  viewMode,
}: UseTimeScheduleDataProps) => {
  const [searchParams] = useSearchParams();
  const [streamTimeScale] = useDebounce(timeScale, 200);
  const [timelineItems, setTimelineItems] = useState<Array<TimelineItem>>([]);
  const [dagRunCount, setDagRunCount] = useState(0);
  const [error, setError] = useState<Error>();
  const [isLoading, setIsLoading] = useState(true);
  const previousNonZoomStreamQueryRef = useRef<string | undefined>(undefined);
  const multiTeamEnabled = Boolean(useConfig("multi_team"));
  const searchParamKeys: Array<FilterableSearchParamsKeys> = [
    SearchParamsKeys.DAG_ID_PATTERN,
    SearchParamsKeys.STATE,
    SearchParamsKeys.RUN_TYPE,
    SearchParamsKeys.RUN_AFTER_RANGE,
    SearchParamsKeys.START_DATE_RANGE,
    SearchParamsKeys.DURATION_GTE,
    SearchParamsKeys.DURATION_LTE,
    SearchParamsKeys.TAGS,
    SearchParamsKeys.TIMETABLE_TYPE,
  ];

  if (multiTeamEnabled) {
    searchParamKeys.push(SearchParamsKeys.TEAMS);
  }
  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(searchParamKeys);

  const streamQuery = useMemo(() => {
    const query = new URLSearchParams();
    const forwardedKeys = [
      SearchParamsKeys.DAG_ID_PATTERN,
      SearchParamsKeys.STATE,
      SearchParamsKeys.RUN_TYPE,
      SearchParamsKeys.RUN_AFTER_GTE,
      SearchParamsKeys.RUN_AFTER_LTE,
      SearchParamsKeys.START_DATE_GTE,
      SearchParamsKeys.START_DATE_LTE,
      SearchParamsKeys.DURATION_GTE,
      SearchParamsKeys.DURATION_LTE,
      SearchParamsKeys.TAGS,
      SearchParamsKeys.TAGS_MATCH_MODE,
      SearchParamsKeys.TIMETABLE_TYPE,
      ...(multiTeamEnabled ? [SearchParamsKeys.TEAMS] : []),
    ];

    forwardedKeys.forEach((key) =>
      searchParams.getAll(key).forEach((value) => {
        if (value !== "") {
          query.append(key, value);
        }
      }),
    );
    query.set("aggregation_mode", aggregationMode);
    query.set("limit", String(dagRunLimit));
    query.set("show_scheduled_only", String(showScheduledOnly));
    query.set("time_scale", String(streamTimeScale));
    query.set("timezone", selectedTimezone);
    query.set("view_mode", viewMode);

    return query.toString();
  }, [
    aggregationMode,
    dagRunLimit,
    multiTeamEnabled,
    searchParams,
    selectedTimezone,
    showScheduledOnly,
    streamTimeScale,
    viewMode,
  ]);
  const nonZoomStreamQuery = useMemo(() => {
    const query = new URLSearchParams(streamQuery);

    query.delete("time_scale");

    return query.toString();
  }, [streamQuery]);

  useEffect(() => {
    const abortController = new AbortController();
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    const isZoomRefresh = previousNonZoomStreamQueryRef.current === nonZoomStreamQuery;
    let replaceTimelineItems = isZoomRefresh;

    previousNonZoomStreamQueryRef.current = nonZoomStreamQuery;

    if (!isZoomRefresh) {
      setTimelineItems([]);
      setDagRunCount(0);
      setIsLoading(true);
    }
    setError(undefined);

    const applyBatch = (batch: TimeScheduleBatch, shouldReplaceTimelineItems: boolean) => {
      const batchItems = batch.items.map(mapStreamItem);

      setTimelineItems((currentItems) =>
        shouldReplaceTimelineItems ? batchItems : [...currentItems, ...batchItems],
      );
      setDagRunCount((currentCount) =>
        shouldReplaceTimelineItems ? batch.dag_run_count : currentCount + batch.dag_run_count,
      );
    };

    const fetchStream = async () => {
      try {
        const response = await fetch(`${OpenAPI.BASE}/ui/time-schedule?${streamQuery}`, {
          signal: abortController.signal,
        });

        if (!response.ok || !response.body) {
          setError(new Error(`Time Schedule request failed with status ${response.status}`));
          setIsLoading(false);

          return;
        }

        reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        for (;;) {
          // Each chunk depends on the previous buffer remainder.
          // eslint-disable-next-line no-await-in-loop
          const result = await reader.read();

          if (result.done) {
            break;
          }

          buffer += decoder.decode(result.value, { stream: true });
          const lines = buffer.split("\n");

          buffer = lines.pop() ?? "";
          const batches = lines
            .filter((line) => line.trim())
            .map((line) => JSON.parse(line) as TimeScheduleBatch);

          for (const batch of batches) {
            const shouldReplaceTimelineItems = replaceTimelineItems;

            replaceTimelineItems = false;
            applyBatch(batch, shouldReplaceTimelineItems);
          }
        }
      } catch (streamError) {
        if ((streamError as Error).name !== "AbortError") {
          setError(streamError as Error);
        }
      }
      if (!abortController.signal.aborted) {
        setIsLoading(false);
      }
    };

    void fetchStream();

    return () => {
      abortController.abort();
      void reader?.cancel();
    };
  }, [nonZoomStreamQuery, streamQuery]);

  const dayRows =
    viewMode === "day" ? buildTimelineRows({ items: timelineItems, rowSortMode, selectedTimezone }) : [];

  return {
    aggregatedWeekItems: viewMode === "week" ? timelineItems : [],
    controls: {
      filterConfigs,
      initialValues,
      onFiltersChange: handleFiltersChange,
    },
    dagRunCount,
    dayRows,
    error,
    isLoading,
    timelineItems,
  };
};
