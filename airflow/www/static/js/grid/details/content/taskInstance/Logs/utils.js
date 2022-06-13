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

export const logLevel = {
  DEBUG: 'DEBUG',
  INFO: 'INFO',
  WARNING: 'WARNING',
  ERROR: 'ERROR',
  CRITICAL: 'CRITICAL',
};

export const parseLogs = (data, timezone, logLevelFilter, fileSourceFilter) => {
  const lines = data.split('\n');

  if (!data) {
    return {};
  }

  const parsedLines = [];
  const fileSources = new Set();

  lines.forEach((line) => {
    let parsedLine = line;

    // Apply log level filter.
    if (logLevelFilter && !line.includes(logLevelFilter)) {
      return;
    }

    const regExp = /\[(.*?)\] \{(.*?)\}/;
    const matches = line.match(regExp);
    let logGroup = '';
    if (matches) {
      // Replace UTC with the local timezone.
      const dateTime = matches[1];
      [logGroup] = matches[2].split(':');
      if (dateTime && timezone) {
        const localDateTime = moment.utc(dateTime).tz(timezone).format(defaultFormatWithTZ);
        parsedLine = line.replace(dateTime, localDateTime);
      }

      fileSources.add(logGroup);
    }
    if (!fileSourceFilter || fileSourceFilter === logGroup) {
      parsedLines.push(parsedLine);
    }
  });

  return { parsedLogs: parsedLines.join('\n'), fileSources: Array.from(fileSources).sort() };
};
