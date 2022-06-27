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

/* global moment */

import { defaultFormatWithTZ } from '../../../../../datetime_utils';

export enum LogLevel {
  DEBUG = 'DEBUG',
  INFO = 'INFO',
  WARNING = 'WARNING',
  ERROR = 'ERROR',
  CRITICAL = 'CRITICAL',
}

export const logLevelColorMapping = {
  [LogLevel.DEBUG]: 'gray.300',
  [LogLevel.INFO]: 'green.200',
  [LogLevel.WARNING]: 'yellow.200',
  [LogLevel.ERROR]: 'red.200',
  [LogLevel.CRITICAL]: 'red.400',
};

export const parseLogs = (
  data: string | undefined,
  timezone: string | null,
  logLevelFilters: Array<LogLevel>,
  fileSourceFilters: Array<string>,
) => {
  if (!data) {
    return {};
  }

  const lines = data.split('\n');

  const parsedLines: Array<string> = [];
  const fileSources: Set<string> = new Set();

  lines.forEach((line) => {
    let parsedLine = line;

    // Apply log level filter.
    if (logLevelFilters.length > 0 && logLevelFilters.every((level) => !line.includes(level))) {
      return;
    }

    const regExp = /\[(.*?)\] \{(.*?)\}/;
    // e.g) '2022-06-15 10:30:06,020' or '2022-06-15 10:30:06+0900'
    const dateRegex = /(\d{4}[./-]\d{2}[./-]\d{2} \d{2}:\d{2}:\d{2})((,\d{3})|([+-]\d{4} \d{3}ms))/;
    // above regex is a kind of duplication of 'dateRegex'
    // in airflow/www/static/js/tl_log.js
    const matches = line.match(regExp);
    let logGroup = '';
    if (matches) {
      // Replace system timezone with user selected timezone.
      const dateTime = matches[1];

      // e.g) '2022-06-15 10:30:06,020' or '2022-06-15 10:30:06+0900 123ms'
      const dateMatches = dateTime?.match(dateRegex);
      if (dateMatches) {
        const [date, msecOrUTCOffset] = [dateMatches[1], dateMatches[2]];
        if (msecOrUTCOffset.startsWith(',')) { // e.g) date='2022-06-15 10:30:06', msecOrUTCOffset=',020'
          // for backward compatibility. (before 2.3.3)
          // keep previous behavior if utcoffset not found. (consider it UTC)
          //
          if (dateTime && timezone) { // dateTime === fullMatch
            // @ts-ignore
            const localDateTime = moment.utc(dateTime).tz(timezone).format(defaultFormatWithTZ);
            parsedLine = line.replace(dateTime, localDateTime);
          }
        } else {
          // e.g) date='2022-06-15 10:30:06', msecOrUTCOffset='+0900 123ms'
          // (formatted by airflow.utils.log.timezone_aware.TimezoneAware) (since 2.3.3)
          const [utcoffset, threeDigitMs] = msecOrUTCOffset.split(' ');
          const msec = threeDigitMs.replace(/\D+/g, ''); // drop 'ms'
          // e.g) datetime='2022-06-15 10:30:06.123+0900'
          // @ts-ignore
          const localDateTime = moment(`${date}.${msec}${utcoffset}`).tz(timezone).format(defaultFormatWithTZ);
          parsedLine = line.replace(dateTime, localDateTime);
        }
      }
      [logGroup] = matches[2].split(':');
      fileSources.add(logGroup);
    }

    if (fileSourceFilters.length === 0
        || fileSourceFilters.some((fileSourceFilter) => line.includes(fileSourceFilter))) {
      parsedLines.push(parsedLine);
    }
  });

  return { parsedLogs: parsedLines.join('\n'), fileSources: Array.from(fileSources).sort() };
};
