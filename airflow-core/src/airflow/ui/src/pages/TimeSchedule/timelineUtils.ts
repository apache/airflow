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
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";

import type { TimeScheduleItem } from "openapi/requests/types.gen";

import { SearchParamsKeys } from "src/constants/searchParams";
import { renderDuration } from "src/utils/datetimeUtils";

import {
  DAY_DURATION_MS,
  DAY_MINUTES,
  DAY_ROW_MIN_HEIGHT_PX,
  DAY_LANE_HEIGHT_PX,
  DAY_ROW_PADDING_PX,
} from "./constants";
import type { DayRowLayout, RowSortMode, TimeMarker, TimeScale, TimelineRow, WeekItemLayout } from "./types";

dayjs.extend(utc);
dayjs.extend(timezone);

const getTimeFilterValue = (time: string, runAfter: string, selectedTimezone: string) => {
  const date = dayjs(runAfter).tz(selectedTimezone).format("YYYY-MM-DD");
  const dateTime = dayjs.tz(`${date}T${time}:00`, selectedTimezone);

  return dateTime.utc().format("HH:mm:ss.SSS[Z]");
};

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
  readonly items: Array<TimeScheduleItem>;
  readonly rowSortMode: RowSortMode;
  readonly selectedTimezone: string;
};

export const buildTimelineRows = ({
  items,
  rowSortMode,
  selectedTimezone,
}: BuildTimelineRowsParams): Array<TimelineRow> => {
  const rowsByDagId = new Map<string, Array<TimeScheduleItem>>();

  items.forEach((item) => {
    const rowItems = rowsByDagId.get(item.dag_id);

    if (rowItems) {
      rowItems.push(item);
    } else {
      rowsByDagId.set(item.dag_id, [item]);
    }
  });

  return Array.from(rowsByDagId, ([dagId, rowItems]) => ({
    dag_display_name: rowItems[0]?.dag_display_name ?? dagId,
    dag_id: dagId,
    is_time_scheduled: rowItems.some((item) => item.is_time_scheduled),
    items: rowItems.sort(
      (left, right) =>
        getLocalStartTimeSortValue(left.start_date, selectedTimezone) -
        getLocalStartTimeSortValue(right.start_date, selectedTimezone),
    ),
  })).sort((left, right) => {
    if (rowSortMode === "dagIdAscending") {
      return left.dag_id.localeCompare(right.dag_id);
    }
    if (rowSortMode === "dagIdDescending") {
      return right.dag_id.localeCompare(left.dag_id);
    }
    if (left.is_time_scheduled !== right.is_time_scheduled) {
      return Number(right.is_time_scheduled) - Number(left.is_time_scheduled);
    }
    const difference =
      getLocalStartTimeSortValue(left.items[0]?.start_date ?? null, selectedTimezone) -
      getLocalStartTimeSortValue(right.items[0]?.start_date ?? null, selectedTimezone);

    return difference || left.dag_id.localeCompare(right.dag_id);
  });
};

export const getPosition = (value: dayjs.Dayjs, dayStart: dayjs.Dayjs) =>
  Math.max(0, Math.min(100, (value.diff(dayStart) / DAY_DURATION_MS) * 100));

const STATE_ICON_AND_SPACING_WIDTH_PX = 18;
const DURATION_CHARACTER_WIDTH_PX = 10;

export const getTimelineDurationSeconds = (durationMs: number) =>
  durationMs >= 60_000 ? Math.floor(durationMs / 60_000) * 60 : durationMs / 1000;

export const getTimelineBarMinimumWidth = (durationMs: number, locale?: string) =>
  STATE_ICON_AND_SPACING_WIDTH_PX +
  (durationMs > 0 ? (renderDuration(getTimelineDurationSeconds(durationMs), locale)?.length ?? 0) : 0) *
    DURATION_CHARACTER_WIDTH_PX;

export const getVisualDurationWidth = (durationMs: number, locale?: string) =>
  `max(${getTimelineBarMinimumWidth(durationMs, locale)}px, ${(durationMs / DAY_DURATION_MS) * 100}%)`;

export const getTimelineBarLeft = (startPosition: number, width: number | string) =>
  `min(${startPosition}%, calc(100% - ${typeof width === "number" ? `${width}px` : width}))`;

type BuildDayRowLayoutsParams = {
  readonly locale?: string;
  readonly rows: Array<TimelineRow>;
  readonly selectedTimezone: string;
  readonly timelineWidth: number;
};

export const buildDayRowLayouts = ({
  locale,
  rows,
  selectedTimezone,
  timelineWidth,
}: BuildDayRowLayoutsParams): Array<DayRowLayout> =>
  rows.map((row) => {
    const laneEnds: Array<number> = [];
    const items = row.items
      .filter((item) => item.start_date !== null)
      .sort(
        (left, right) =>
          getLocalStartTimeSortValue(left.start_date, selectedTimezone) -
          getLocalStartTimeSortValue(right.start_date, selectedTimezone),
      )
      .map((item) => {
        const start = dayjs(item.start_date).tz(selectedTimezone);
        const startX = (getPosition(start, start.startOf("day")) / 100) * timelineWidth;
        const width = Math.max(
          getTimelineBarMinimumWidth(item.duration_ms, locale),
          (item.duration_ms / DAY_DURATION_MS) * timelineWidth,
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
    const height = Math.max(DAY_ROW_MIN_HEIGHT_PX, laneEnds.length * DAY_LANE_HEIGHT_PX + DAY_ROW_PADDING_PX);

    return { height, items, row };
  });

type BuildWeekItemLayoutsParams = {
  readonly contentHeight: number;
  readonly items: Array<TimeScheduleItem>;
  readonly selectedTimezone: string;
};

export const buildWeekItemLayouts = ({
  contentHeight,
  items,
  selectedTimezone,
}: BuildWeekItemLayoutsParams): Array<WeekItemLayout> => {
  const positionedItems = items
    .filter((item) => item.start_date !== null)
    .map((item) => {
      const start = dayjs(item.start_date).tz(selectedTimezone);
      const height = Math.min(
        contentHeight,
        Math.max(20, (item.duration_ms / DAY_DURATION_MS) * contentHeight),
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
  Array.from({ length: Math.floor(DAY_MINUTES / timeScale) + 1 }, (_, index) => {
    const minute = index * timeScale;

    return {
      label: `${String(Math.floor(minute / 60)).padStart(2, "0")}:${String(minute % 60).padStart(2, "0")}`,
      minute,
      position: (minute / DAY_MINUTES) * 100,
    };
  });

export const buildHourMarkers = () =>
  Array.from({ length: 25 }, (_, index) => ({
    minute: index * 60,
    position: (index * 60 * 100) / DAY_MINUTES,
  }));

export const getTimelineItemDestination = (item: TimeScheduleItem, selectedTimezone: string) => {
  const runsPath = `/dags/${encodeURIComponent(item.dag_id)}/runs`;

  if (item.run_count === 1) {
    return `${runsPath}/${encodeURIComponent(item.dag_run_id)}`;
  }

  const searchParams = new URLSearchParams({
    [SearchParamsKeys.RUN_AFTER_GTE]: item.run_after_min,
    [SearchParamsKeys.RUN_AFTER_LTE]: item.run_after_max,
    [SearchParamsKeys.STATE]: item.state,
  });

  if (item.start_time_gte !== null && item.start_time_gte !== undefined) {
    searchParams.set(
      SearchParamsKeys.START_TIME_GTE,
      getTimeFilterValue(item.start_time_gte, item.run_after_min, selectedTimezone),
    );
  }
  if (item.start_time_lt !== null && item.start_time_lt !== undefined) {
    searchParams.set(
      SearchParamsKeys.START_TIME_LT,
      getTimeFilterValue(item.start_time_lt, item.run_after_min, selectedTimezone),
    );
  }
  if (item.start_weekday !== null && item.start_weekday !== undefined && item.start_date !== null) {
    searchParams.set(SearchParamsKeys.START_WEEKDAY, String(dayjs(item.start_date).utc().day()));
  }

  return `${runsPath}?${searchParams.toString()}`;
};
