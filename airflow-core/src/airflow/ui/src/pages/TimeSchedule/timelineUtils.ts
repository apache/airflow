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
import { dayjs } from "./dateUtils";
import type {
  DayRowLayout,
  RowSortMode,
  TimeMarker,
  TimeScale,
  TimelineItem,
  TimelineRow,
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
