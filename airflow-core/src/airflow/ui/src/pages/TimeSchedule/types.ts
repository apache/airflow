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
import type { TimeScheduleItem } from "openapi/requests/types.gen";

export type AggregationMode = "max" | "mean" | "min";
export const DAG_RUN_LIMITS = [200, 600, 1000, 2000, 5000] as const;
export type DagRunLimit = (typeof DAG_RUN_LIMITS)[number];
export type RowSortMode = "dagIdAscending" | "dagIdDescending" | "startTime";
// eslint-disable-next-line perfectionist/sort-union-types -- Numeric options are ordered by magnitude.
export type TimeScale = 1 | 5 | 10 | 15 | 20 | 30 | 40 | 50 | 60;
export type ViewMode = "day" | "week";

export type TimelineRow = {
  readonly dag_display_name: string;
  readonly dag_id: string;
  readonly is_time_scheduled: boolean;
  readonly items: Array<TimeScheduleItem>;
};

export type DayRowLayout = {
  readonly height: number;
  readonly items: Array<{ readonly item: TimeScheduleItem; readonly lane: number }>;
  readonly row: TimelineRow;
  readonly top: number;
};

export type WeekItemLayout = {
  readonly column: number;
  readonly columnCount: number;
  readonly height: number;
  readonly item: TimeScheduleItem;
  readonly top: number;
};

export type TimeMarker = {
  readonly label: string;
  readonly minute: number;
  readonly position: number;
};

export type ZoomAnchor = {
  readonly axis: "horizontal" | "vertical";
  readonly offset: number;
  readonly ratio: number;
};
