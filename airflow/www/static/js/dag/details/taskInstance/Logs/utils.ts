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

import { AnsiUp } from "ansi_up";
import { getMetaValue, highlightByKeywords } from "src/utils";
import { defaultFormatWithTZ } from "src/datetime_utils";

export enum LogLevel {
  DEBUG = "DEBUG",
  INFO = "INFO",
  WARNING = "WARNING",
  ERROR = "ERROR",
  CRITICAL = "CRITICAL",
}

export const logLevelColorMapping = {
  [LogLevel.DEBUG]: "gray.300",
  [LogLevel.INFO]: "green.200",
  [LogLevel.WARNING]: "yellow.200",
  [LogLevel.ERROR]: "red.200",
  [LogLevel.CRITICAL]: "red.400",
};

const errorKeywords = getMetaValue("color_log_error_keywords")
  .split(",")
  .filter((keyword) => keyword.length > 0)
  .map((keyword) => keyword.toLowerCase());
const warningKeywords = getMetaValue("color_log_warning_keywords")
  .split(",")
  .filter((keyword) => keyword.length > 0)
  .map((keyword) => keyword.toLowerCase());

// Detect log groups which can be collapsed
// Either in Github like format '::group::<group name>' to '::endgroup::'
// see https://docs.github.com/en/actions/using-workflows/workflow-commands-for-github-actions#grouping-log-lines
// Or in ADO pipeline like format '##[group]<group name>' to '##[endgroup]'
// see https://learn.microsoft.com/en-us/azure/devops/pipelines/scripts/logging-commands?view=azure-devops&tabs=powershell#formatting-commands
export const logGroupStart = / INFO - (::|##\[])group(::|\])([^\n])*/g;
export const logGroupEnd = / INFO - (::|##\[])endgroup(::|\])/g;

export const parseLogs = (
  data: string | undefined,
  timezone: string | null,
  logLevelFilters: Array<string>,
  fileSourceFilters: Array<string>,
  unfoldedLogGroups: Array<string>
) => {
  if (!data) {
    return {};
  }
  let lines;

  let warning;

  try {
    lines = data.split("\n");
  } catch (err) {
    warning = "Unable to show logs. There was an error parsing logs.";
    return { warning };
  }

  const parsedLines: Array<string> = [];
  const fileSources: Set<string> = new Set();
  const targetLogLevels: Set<string> = new Set(logLevelFilters);
  const targetFileSources: Set<string> = new Set(fileSourceFilters);
  const ansiUp = new AnsiUp();
  ansiUp.url_allowlist = {};

  const urlRegex =
    /http(s)?:\/\/[\w.-]+(\.?:[\w.-]+)*([/?#][\w\-._~:/?#[\]@!$&'()*+,;=.%]+)?/g;
  // Coloring (blue-60 as chakra style, is #0060df) and style such that log group appears like a link
  const logGroupStyle = "color:#0060df;cursor:pointer;font-weight:bold;";

  // Example Log Format: [2021-08-26 00:00:00,000] {filename.py:42} INFO - Log message
  const regExp = /\[(.*?)\] \{(.*?)\} (.*?) -/;
  let currentLevel: LogLevel = LogLevel.INFO;
  let currentFileSource = "";
  lines.forEach((line) => {
    let parsedLine = line;
    const matches = line.match(regExp);
    let fileSource = "";
    if (matches) {
      // Replace UTC with the local timezone.
      const dateTime = matches[1];
      [fileSource] = matches[2].split(":");
      const logLevel = matches[3];

      if (dateTime && timezone) {
        // @ts-ignore
        const localDateTime = moment
          .utc(dateTime)
          // @ts-ignore
          .tz(timezone)
          .format(defaultFormatWithTZ);
        parsedLine = line.replace(dateTime, localDateTime);
      }

      // The `currentLogLevel` and `currentFileSource` should remain same
      // until a new `logLevel` or `fileSource` is encountered.
      currentLevel = logLevel as LogLevel;
      currentFileSource = fileSource;
    }

    // Apply log level filter.
    if (logLevelFilters.length > 0 && !targetLogLevels.has(currentLevel)) {
      return;
    }
    if (fileSource) {
      // Only add file source if it is not empty.
      fileSources.add(fileSource);
    }
    // Apply file source filter.
    if (
      fileSourceFilters.length > 0 &&
      !targetFileSources.has(currentFileSource)
    ) {
      return;
    }

    parsedLine = highlightByKeywords(
      parsedLine,
      currentLevel,
      errorKeywords,
      warningKeywords,
      logGroupStart,
      logGroupEnd
    );
    // for lines with color convert to nice HTML
    const coloredLine = ansiUp.ansi_to_html(parsedLine);

    // for lines with links, transform to hyperlinks
    const lineWithHyperlinks = coloredLine
      .replace(
        urlRegex,
        (url) =>
          `<a href="${url}" target="_blank" rel="noopener noreferrer" style="color: blue; text-decoration: underline;">${url}</a>`
      )
      .replace(logGroupStart, (textLine) => {
        const unfoldIdSuffix = "_unfold";
        const foldIdSuffix = "_fold";
        const gName = textLine.substring(17);
        const gId = gName.replace(/\W+/g, "_").toLowerCase();
        const isFolded = unfoldedLogGroups.indexOf(gId) === -1;
        const ufDisplay = isFolded ? "" : "display:none;";
        const unfold = `<span id="${gId}${unfoldIdSuffix}" style="${ufDisplay}${logGroupStyle}"> &#9654; ${gName}</span>`;
        const fDisplay = isFolded ? "display:none;" : "";
        const fold = `<span style="${fDisplay}"><span id="${gId}${foldIdSuffix}" style="${logGroupStyle}"> &#9660; ${gName}</span>`;
        return unfold + fold;
      })
      .replace(
        logGroupEnd,
        " <span style='color:#0060df;'>&#9650;&#9650;&#9650; Log group end</span></span>"
      );
    parsedLines.push(lineWithHyperlinks);
  });

  return {
    parsedLogs: parsedLines
      .map((l) => {
        if (l.length >= 1000000) {
          warning =
            "Large log file. Some lines have been truncated. Download logs in order to see everything.";
          return `${l.slice(0, 1000000)}...`;
        }
        return l;
      })
      .join("\n"),
    fileSources: Array.from(fileSources).sort(),
    warning,
  };
};
