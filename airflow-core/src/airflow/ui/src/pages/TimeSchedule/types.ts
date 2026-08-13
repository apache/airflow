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
import type { DAGRunResponse, DAGWithLatestDagRunsResponse } from "openapi/requests/types.gen";

export type AggregationMode = "max" | "mean" | "min";
export type DagRunLimit = "all" | 1000 | 200 | 2000 | 5000 | 600;
export type RowSortMode = "dagIdAscending" | "dagIdDescending" | "startTime";
export type TimeScale = 1 | 10 | 15 | 20 | 30 | 40 | 5 | 50 | 60;
export type ViewMode = "day" | "week";

export type TimelineItem = {
  readonly dagId: string;
  readonly dagRunId: string;
  readonly durationMs: number;
  readonly endDate: string | null;
  readonly isPlaceholder: boolean;
  readonly isPlanned: boolean;
  readonly isTimeScheduled: boolean;
  readonly label: string;
  readonly runCount: number;
  readonly startDate: string | null;
  readonly state: DAGRunResponse["state"] | "placeholder" | "planned";
};

export type TimelineRow = {
  readonly dagId: string;
  readonly isTimeScheduled: boolean;
  readonly items: Array<TimelineItem>;
  readonly label: string;
};

export type DayRowLayout = {
  readonly height: number;
  readonly items: Array<{ readonly item: TimelineItem; readonly lane: number }>;
  readonly row: TimelineRow;
  readonly top: number;
};

export type WeekItemLayout = {
  readonly column: number;
  readonly columnCount: number;
  readonly height: number;
  readonly item: TimelineItem;
  readonly top: number;
};

export type ScheduledDag = Pick<
  DAGWithLatestDagRunsResponse,
  "dag_display_name" | "dag_id" | "next_dagrun_run_after" | "timetable_periodic" | "timetable_summary"
>;

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
