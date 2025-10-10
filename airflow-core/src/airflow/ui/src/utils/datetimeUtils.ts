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
import tz from "dayjs/plugin/timezone";

dayjs.extend(dayjsDuration);
dayjs.extend(tz);

export const DEFAULT_DATETIME_FORMAT = "YYYY-MM-DD HH:mm:ss";
export const DEFAULT_DATETIME_FORMAT_WITH_TZ = `${DEFAULT_DATETIME_FORMAT} z`;

export const renderDuration = (durationSeconds: number | null | undefined): string => {
  if (
    durationSeconds === null ||
    durationSeconds === undefined ||
    isNaN(durationSeconds) ||
    durationSeconds <= 0
  ) {
    return "00:00:00";
  }

  if (durationSeconds < 10) {
    return `${durationSeconds.toFixed(2)}s`;
  }

  return durationSeconds < 86_400
    ? dayjs.duration(durationSeconds, "seconds").format("HH:mm:ss")
    : dayjs.duration(durationSeconds, "seconds").format("D[d]HH:mm:ss");
};

export const getDuration = (startDate?: string | null, endDate?: string | null) => {
  const seconds = dayjs.duration(dayjs(endDate ?? undefined).diff(startDate ?? undefined)).asSeconds();

  return renderDuration(seconds);
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
