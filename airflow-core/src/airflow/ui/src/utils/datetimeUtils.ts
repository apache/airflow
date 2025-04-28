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

dayjs.extend(dayjsDuration);

export const getDuration = (startDate?: string | null, endDate?: string | null) => {
  const segments = [];

  const seconds = dayjs.duration(dayjs(endDate ?? undefined).diff(startDate ?? undefined)).asSeconds();

  if (seconds < 10) {
    return `${seconds.toFixed(2)}s`;
  }

  const day = Math.floor(seconds / 60 / 60 / 24);

  if (day) {
    segments.push(`${day}d`);
  }

  const hr = Math.floor((seconds / 60 / 60) % 24);

  segments.push(`${hr || "00"}:`);

  const min = Math.floor((seconds / 60) % 60);

  segments.push(`${min || "00"}:`);

  const sec = Math.round(seconds % 60);

  segments.push(Math.round(sec) || "00");

  return segments.join("");
};
