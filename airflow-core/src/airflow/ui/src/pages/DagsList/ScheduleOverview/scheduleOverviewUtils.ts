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

const HOUR_TICKS: ReadonlyArray<number> = [0, 3, 6, 9, 12, 15, 18, 21, 24];
const SECONDS_PER_DAY = 24 * 60 * 60;

const formatSecondsAsClock = (totalSeconds: number | null | undefined): string => {
  if (totalSeconds === null || totalSeconds === undefined) {
    return "—";
  }
  const safe = ((totalSeconds % SECONDS_PER_DAY) + SECONDS_PER_DAY) % SECONDS_PER_DAY;
  const hours = Math.floor(safe / 3600);
  const minutes = Math.floor((safe % 3600) / 60);

  return `${hours.toString().padStart(2, "0")}:${minutes.toString().padStart(2, "0")}`;
};

const formatDurationSeconds = (totalSeconds: number | null | undefined): string => {
  if (totalSeconds === null || totalSeconds === undefined) {
    return "—";
  }
  const total = Math.max(0, Math.round(totalSeconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);

  if (hours > 0) {
    return `${hours}h ${minutes.toString().padStart(2, "0")}m`;
  }

  return `${minutes}m`;
};

const formatCount = (count: number): string =>
  count === 1 ? "1 successful run" : `${count} successful runs`;

const formatLogicalDate = (value: string | null | undefined): string => {
  if (value === null || value === undefined || value.length === 0) {
    return "—";
  }

  // Render as YYYY-MM-DD for the tooltip; full ISO is already in the
  // cell title attribute so users can hover for exact time.
  return value.slice(0, 10);
};

export const formatScheduleBar = (
  startMean: number | null | undefined,
  endMean: number | null | undefined,
): { leftPercent: number; widthPercent: number } => {
  if (startMean === null || startMean === undefined) {
    return { leftPercent: 0, widthPercent: 0 };
  }
  // When end_mean is missing, draw a small marker (~30 min) so the user
  // still sees *where* the run typically starts. When end_mean is
  // present, wrap around midnight (e.g. 22:00 -> 02:00) into a bar that
  // crosses the 24h boundary, so the Gantt-style 24h view stays linear.
  if (endMean === null || endMean === undefined) {
    const left = (startMean / SECONDS_PER_DAY) * 100;

    return { leftPercent: left, widthPercent: 1.5 };
  }
  const start = startMean;
  const end = endMean;

  if (end <= start) {
    // Treat as wrapping midnight: draw a 30 minute marker at the start
    // point rather than a full-width bar that would dominate the view.
    const left = (start / SECONDS_PER_DAY) * 100;

    return { leftPercent: left, widthPercent: 1.5 };
  }
  const widthSeconds = end - start;
  const leftPercent = (start / SECONDS_PER_DAY) * 100;
  const widthPercent = (widthSeconds / SECONDS_PER_DAY) * 100;

  return { leftPercent, widthPercent };
};

export const formatTooltipBody = (entry: {
  readonly dag_display_name: string;
  readonly duration_mean_seconds: number | null;
  readonly duration_median_seconds: number | null;
  readonly end_mean_seconds: number | null;
  readonly end_median_seconds: number | null;
  readonly newest_logical_date: string | null;
  readonly oldest_logical_date: string | null;
  readonly recent_runs_count: number;
  readonly start_mean_seconds: number | null;
  readonly start_median_seconds: number | null;
}): string => {
  const start = `${formatSecondsAsClock(entry.start_mean_seconds)} ~ ${formatSecondsAsClock(entry.start_median_seconds)}`;
  const end = `${formatSecondsAsClock(entry.end_mean_seconds)} ~ ${formatSecondsAsClock(entry.end_median_seconds)}`;
  const duration = `${formatDurationSeconds(entry.duration_mean_seconds)} (mean) / ${formatDurationSeconds(entry.duration_median_seconds)} (median)`;
  const range = `${formatLogicalDate(entry.oldest_logical_date)} → ${formatLogicalDate(entry.newest_logical_date)}`;

  return [
    entry.dag_display_name,
    formatCount(entry.recent_runs_count),
    `Mean ~ Median: ${start} → ${end}`,
    `Duration: ${duration}`,
    `Range: ${range}`,
  ].join("\n");
};

export { formatSecondsAsClock, formatDurationSeconds, HOUR_TICKS, SECONDS_PER_DAY };
