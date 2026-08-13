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
import type { DAGRunResponse } from "openapi/requests/types.gen";

import { dayjs } from "./dateUtils";
import type {
  AggregationMode,
  DayRowLayout,
  RowSortMode,
  ScheduledDag,
  TimeMarker,
  TimeScale,
  TimelineItem,
  TimelineRow,
  ViewMode,
  WeekItemLayout,
} from "./types";

const getLocalStartTimeSortValue = (startDate: string | null, selectedTimezone: string) => {
  if (startDate === null) {
    return 0;
  }

  const localStart = dayjs(startDate).tz(selectedTimezone);

  return (
    localStart.hour() * 60 * 60 * 1000 +
    localStart.minute() * 60 * 1000 +
    localStart.second() * 1000 +
    localStart.millisecond()
  );
};

type BuildTimelineItemsParams = {
  readonly dagRuns: Array<DAGRunResponse>;
  readonly dagRunTimeouts: ReadonlyMap<string, string | null>;
  readonly includeAllDags: boolean;
  readonly scheduledDags: Array<ScheduledDag>;
};

export const buildTimelineItems = ({
  dagRuns,
  dagRunTimeouts,
  includeAllDags,
  scheduledDags,
}: BuildTimelineItemsParams): Array<TimelineItem> => {
  const dagIdsWithRuns = new Set(dagRuns.map((dagRun) => dagRun.dag_id));
  const timeScheduledDagIds = new Set(
    scheduledDags.filter((dag) => dag.timetable_periodic).map((dag) => dag.dag_id),
  );
  const runItems = dagRuns.map((dagRun) => {
    const startDate = dagRun.start_date ?? dagRun.run_after;
    const durationMs = dagRun.duration === null ? 0 : dagRun.duration * 1000;
    const endDate = dagRun.end_date ?? dayjs(startDate).add(durationMs, "millisecond").toISOString();

    return {
      dagId: dagRun.dag_id,
      dagRunId: dagRun.dag_run_id,
      durationMs,
      endDate,
      isPlaceholder: false,
      isPlanned: false,
      isTimeScheduled: timeScheduledDagIds.has(dagRun.dag_id),
      label: dagRun.dag_display_name,
      runCount: 1,
      startDate,
      state: dagRun.state,
    };
  });
  const plannedItems = scheduledDags.flatMap((dag) => {
    if (
      dagIdsWithRuns.has(dag.dag_id) ||
      !dag.timetable_periodic ||
      dag.timetable_summary === null ||
      dag.next_dagrun_run_after === null
    ) {
      return [];
    }
    const startDate = dag.next_dagrun_run_after;
    const dagRunTimeout = dagRunTimeouts.get(dag.dag_id);
    const durationMs =
      dagRunTimeout === undefined || dagRunTimeout === null || dagRunTimeout === ""
        ? 0
        : dayjs.duration(dagRunTimeout).asMilliseconds();

    return [
      {
        dagId: dag.dag_id,
        dagRunId: `${dag.dag_id}-planned`,
        durationMs,
        endDate: dayjs(startDate).add(durationMs, "millisecond").toISOString(),
        isPlaceholder: false,
        isPlanned: true,
        isTimeScheduled: true,
        label: dag.dag_display_name,
        runCount: 0,
        startDate,
        state: "planned" as const,
      },
    ];
  });
  const placeholderItems = includeAllDags
    ? scheduledDags
        .filter(
          (dag) =>
            !dagIdsWithRuns.has(dag.dag_id) &&
            (!dag.timetable_periodic || dag.next_dagrun_run_after === null),
        )
        .map((dag) => ({
          dagId: dag.dag_id,
          dagRunId: `${dag.dag_id}-placeholder`,
          durationMs: 0,
          endDate: null,
          isPlaceholder: true,
          isPlanned: false,
          isTimeScheduled: false,
          label: dag.dag_display_name,
          runCount: 0,
          startDate: dayjs().startOf("day").toISOString(),
          state: "placeholder" as const,
        }))
    : [];

  return [...runItems, ...plannedItems, ...placeholderItems];
};

type BuildTimelineRowsParams = {
  readonly items: Array<TimelineItem>;
  readonly rowSortMode: RowSortMode;
  readonly selectedTimezone: string;
};

export const buildTimelineRows = ({
  items,
  rowSortMode,
  selectedTimezone,
}: BuildTimelineRowsParams): Array<TimelineRow> => {
  const rowsByDagId = new Map<string, Array<TimelineItem>>();

  items.forEach((item) => {
    rowsByDagId.set(item.dagId, [...(rowsByDagId.get(item.dagId) ?? []), item]);
  });

  return Array.from(rowsByDagId, ([dagId, rowItems]) => ({
    dagId,
    isTimeScheduled: rowItems.some((item) => item.isTimeScheduled),
    items: rowItems.sort(
      (left, right) =>
        getLocalStartTimeSortValue(left.startDate, selectedTimezone) -
        getLocalStartTimeSortValue(right.startDate, selectedTimezone),
    ),
    label: rowItems[0]?.label ?? dagId,
  })).sort((left, right) => {
    if (rowSortMode === "dagIdAscending") {
      return left.dagId.localeCompare(right.dagId);
    }
    if (rowSortMode === "dagIdDescending") {
      return right.dagId.localeCompare(left.dagId);
    }
    if (left.isTimeScheduled !== right.isTimeScheduled) {
      return Number(right.isTimeScheduled) - Number(left.isTimeScheduled);
    }
    const difference =
      getLocalStartTimeSortValue(left.items[0]?.startDate ?? null, selectedTimezone) -
      getLocalStartTimeSortValue(right.items[0]?.startDate ?? null, selectedTimezone);

    return difference || left.dagId.localeCompare(right.dagId);
  });
};

export const getPosition = (value: dayjs.Dayjs, dayStart: dayjs.Dayjs) =>
  Math.max(0, Math.min(100, (value.diff(dayStart) / (24 * 60 * 60 * 1000)) * 100));

export const formatDurationLabel = (durationMs: number) => {
  if (durationMs <= 0) {
    return "";
  }
  const seconds = Math.max(1, Math.round(durationMs / 1000));

  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);

  if (minutes < 60) {
    return `${minutes}m`;
  }

  return minutes % 60 > 0 ? `${Math.floor(minutes / 60)}h ${minutes % 60}m` : `${Math.floor(minutes / 60)}h`;
};

export const getVisualDurationWidth = (durationMs: number) =>
  durationMs <= 0 ? 42 : `max(42px, ${(durationMs / 60_000 / (24 * 60)) * 100}%)`;

type BuildDayRowLayoutsParams = {
  readonly rows: Array<TimelineRow>;
  readonly selectedTimezone: string;
  readonly timelineWidth: number;
  readonly timeScale: TimeScale;
};

export const buildDayRowLayouts = ({
  rows,
  selectedTimezone,
  timelineWidth,
  timeScale,
}: BuildDayRowLayoutsParams): Array<DayRowLayout> => {
  let top = 0;

  return rows.map((row) => {
    const laneEnds: Array<number> = [];
    const items = row.items
      .filter((item) => !item.isPlaceholder && item.startDate !== null)
      .sort(
        (left, right) =>
          getLocalStartTimeSortValue(left.startDate, selectedTimezone) -
          getLocalStartTimeSortValue(right.startDate, selectedTimezone),
      )
      .map((item) => {
        const start = dayjs(item.startDate).tz(selectedTimezone);
        const startX = (getPosition(start, start.startOf("day")) / 100) * timelineWidth;
        const width = Math.max(
          timeScale === 1 ? timelineWidth / (24 * 60) : 42,
          (item.durationMs / (24 * 60 * 60 * 1000)) * timelineWidth,
        );
        let lane = laneEnds.findIndex((laneEnd) => laneEnd <= startX);

        if (lane === -1) {
          lane = laneEnds.length;
          laneEnds.push(startX + width);
        } else {
          laneEnds[lane] = startX + width;
        }

        return { item, lane };
      });
    const height = Math.max(48, laneEnds.length * 20 + 16);
    const layout = { height, items, row, top };

    top += height;

    return layout;
  });
};

type BuildWeekItemLayoutsParams = {
  readonly contentHeight: number;
  readonly items: Array<TimelineItem>;
  readonly selectedTimezone: string;
};

export const buildWeekItemLayouts = ({
  contentHeight,
  items,
  selectedTimezone,
}: BuildWeekItemLayoutsParams): Array<WeekItemLayout> => {
  const positionedItems = items
    .filter((item) => item.startDate !== null)
    .map((item) => {
      const start = dayjs(item.startDate).tz(selectedTimezone);
      const height = Math.min(
        contentHeight,
        Math.max(20, (item.durationMs / (24 * 60 * 60 * 1000)) * contentHeight),
      );

      return { height, item, startY: (getPosition(start, start.startOf("day")) / 100) * contentHeight };
    })
    .sort((left, right) => left.startY - right.startY || right.height - left.height);
  const layouts: Array<WeekItemLayout> = [];

  for (let clusterStart = 0; clusterStart < positionedItems.length;) {
    const firstItem = positionedItems[clusterStart];

    if (firstItem === undefined) {
      break;
    }
    let clusterEnd = firstItem.startY + firstItem.height;
    let clusterEndIndex = clusterStart + 1;

    while (clusterEndIndex < positionedItems.length) {
      const item = positionedItems[clusterEndIndex];

      if (item === undefined || item.startY >= clusterEnd) {
        break;
      }

      clusterEnd = Math.max(clusterEnd, item.startY + item.height);
      clusterEndIndex += 1;
    }
    const columnEnds: Array<number> = [];
    const clusterLayouts = positionedItems.slice(clusterStart, clusterEndIndex).map((positionedItem) => {
      let column = columnEnds.findIndex((columnEnd) => columnEnd <= positionedItem.startY);

      if (column === -1) {
        column = columnEnds.length;
        columnEnds.push(positionedItem.startY + positionedItem.height);
      } else {
        columnEnds[column] = positionedItem.startY + positionedItem.height;
      }

      return {
        column,
        height: positionedItem.height,
        item: positionedItem.item,
        top: Math.min(positionedItem.startY, contentHeight - positionedItem.height),
      };
    });

    layouts.push(...clusterLayouts.map((layout) => ({ ...layout, columnCount: columnEnds.length })));
    clusterStart = clusterEndIndex;
  }

  return layouts;
};

export const getAggregationWindowMinutes = (timeScale: TimeScale) => timeScale;

type AggregateTimelineItemsParams = {
  readonly aggregationMode: AggregationMode;
  readonly items: Array<TimelineItem>;
  readonly selectedTimezone: string;
  readonly timeScale: TimeScale;
  readonly viewMode: ViewMode;
};

export const aggregateTimelineItems = ({
  aggregationMode,
  items,
  selectedTimezone,
  timeScale,
  viewMode,
}: AggregateTimelineItemsParams): Array<TimelineItem> => {
  const groups = new Map<string, Array<TimelineItem>>();
  const window = getAggregationWindowMinutes(timeScale);

  items.forEach((item) => {
    if (!item.isPlaceholder && item.startDate !== null) {
      const start = dayjs(item.startDate).tz(selectedTimezone);
      const minute = start.hour() * 60 + start.minute();
      const bucketMinute = Math.floor(minute / window) * window;
      const timeKey = `${String(Math.floor(bucketMinute / 60)).padStart(2, "0")}:${String(bucketMinute % 60).padStart(2, "0")}`;
      const weekdayKey = viewMode === "week" ? `-${start.day()}` : "";
      const key = `${item.dagId}${weekdayKey}-${timeKey}-${item.state}`;

      groups.set(key, [...(groups.get(key) ?? []), item]);
    }
  });

  return [...groups.values()].flatMap((group) => {
    const [representative] = group;

    if (representative?.startDate === undefined || representative.startDate === null) {
      return [];
    }
    const representativeStart = dayjs(representative.startDate).tz(selectedTimezone);
    const dayStart = representativeStart.startOf("day");
    const timedItems = group.map((item) => {
      const itemStart = dayjs(item.startDate).tz(selectedTimezone);
      const itemEnd = dayjs(item.endDate ?? item.startDate).tz(selectedTimezone);

      return {
        endOffset: itemEnd.diff(itemStart.startOf("day")),
        item,
        startOffset: itemStart.diff(itemStart.startOf("day")),
      };
    });
    const shortestItem = timedItems.reduce((shortest, current) =>
      current.item.durationMs < shortest.item.durationMs ? current : shortest,
    );
    const startOffset =
      aggregationMode === "max"
        ? Math.min(...timedItems.map((item) => item.startOffset))
        : aggregationMode === "min"
          ? shortestItem.startOffset
          : timedItems.reduce((sum, item) => sum + item.startOffset, 0) / timedItems.length;
    const endOffset =
      aggregationMode === "max"
        ? Math.max(...timedItems.map((item) => item.endOffset))
        : aggregationMode === "min"
          ? shortestItem.endOffset
          : timedItems.reduce((sum, item) => sum + item.endOffset, 0) / timedItems.length;
    const aggregatedStart = dayStart.add(startOffset, "millisecond");
    const aggregatedEnd = dayStart.add(endOffset, "millisecond");

    return [
      {
        ...representative,
        durationMs: aggregatedEnd.diff(aggregatedStart),
        endDate: aggregatedEnd.toISOString(),
        runCount: group.filter((item) => !item.isPlanned).length,
        startDate: aggregatedStart.toISOString(),
      },
    ];
  });
};

export const buildTimeMarkers = (timeScale: TimeScale): Array<TimeMarker> =>
  Array.from({ length: Math.floor((24 * 60) / timeScale) + 1 }, (_, index) => {
    const minute = index * timeScale;

    return {
      label: `${String(Math.floor(minute / 60)).padStart(2, "0")}:${String(minute % 60).padStart(2, "0")}`,
      minute,
      position: (minute / (24 * 60)) * 100,
    };
  });

export const buildHourMarkers = () =>
  Array.from({ length: 25 }, (_, index) => ({
    minute: index * 60,
    position: (index * 60 * 100) / (24 * 60),
  }));

export const getTimelineItemColorPalette = (item: Pick<TimelineItem, "isPlanned" | "state">) =>
  item.isPlanned ? "scheduled" : item.state;
export const getTimelineItemDestination = (item: TimelineItem) =>
  item.isPlanned || item.isPlaceholder
    ? `/dags/${item.dagId}/runs`
    : `/dags/${item.dagId}/runs/${item.dagRunId}`;
export const getTimelineItemLinkLabel = (item: TimelineItem) =>
  item.isPlanned || item.isPlaceholder ? `View ${item.label} Dag runs` : `View Dag run ${item.dagRunId}`;
