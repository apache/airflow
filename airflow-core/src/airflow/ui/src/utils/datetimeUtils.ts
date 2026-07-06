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
import dayjsDuration from "dayjs/plugin/duration";
import relativeTime from "dayjs/plugin/relativeTime";
import tz from "dayjs/plugin/timezone";

dayjs.extend(dayjsDuration);
dayjs.extend(relativeTime);
dayjs.extend(tz);

export const DEFAULT_DATETIME_FORMAT = "YYYY-MM-DD HH:mm:ss";
export const DEFAULT_DATETIME_FORMAT_WITH_TZ = `${DEFAULT_DATETIME_FORMAT} z`;

export const renderDuration = (
  durationSeconds: dayjsDuration.Duration | number | null | undefined,
  withMilliseconds: boolean = true,
): string | undefined => {
  if (durationSeconds === null || durationSeconds === undefined) {
    return undefined;
  }

  // Handle floating point milliseconds
  const duration = dayjs.isDuration(durationSeconds)
    ? dayjs.duration(Math.round(durationSeconds.asMilliseconds()))
    : dayjs.duration(Number(durationSeconds.toFixed(3)), "seconds");

  if (duration.asMilliseconds() < 1) {
    return undefined;
  }

  // If under 60 seconds, render milliseconds
  if (duration.asSeconds() < 60 && duration.milliseconds() > 0 && withMilliseconds) {
    return duration.format("HH:mm:ss.SSS");
  }

  // If under 1 day, render as HH:mm:ss otherwise include the number of days
  return duration.asSeconds() < 86_400 ? duration.format("HH:mm:ss") : duration.format("D[d]HH:mm:ss");
};

export const getDuration = (
  startDate?: string | null,
  endDate?: string | null,
  withMilliseconds: boolean = true,
) => {
  if (startDate === undefined || startDate === null) {
    return undefined;
  }

  const end = endDate ?? dayjs().toISOString();
  const milliseconds = dayjs.duration(dayjs(end).diff(startDate));

  return renderDuration(milliseconds, withMilliseconds);
};

export const formatDate = (
  date: number | string | null | undefined,
  timezone: string,
  format: string = DEFAULT_DATETIME_FORMAT,
) => {
  if (date === null || date === undefined || !dayjs(date).isValid()) {
    return dayjs().tz(timezone).format(format);
  }

  return dayjs(date).tz(timezone).format(format);
};

export const getRelativeTime = (date: string | null | undefined): string => {
  if (date === null || date === "" || date === undefined) {
    return "";
  }

  return dayjs(date).fromNow();
};

export const getTimezoneOffsetString = (timezone: string): string => dayjs().tz(timezone).format("Z");

export const getTimezoneTooltipLabel = (timezone: string): string => {
  const now = dayjs().tz(timezone);

  return `${timezone} — ${now.format(DEFAULT_DATETIME_FORMAT_WITH_TZ)}`;
};
