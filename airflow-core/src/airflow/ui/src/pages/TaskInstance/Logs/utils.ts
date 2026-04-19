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
import type { Virtualizer } from "@tanstack/react-virtual";
import type { TFunction } from "i18next";

import type { TaskInstancesLogResponse } from "openapi/requests/types.gen";
import { renderStructuredLog } from "src/components/renderStructuredLog";
import { parseStreamingLogContent } from "src/utils/logs";

type GetDownloadTextOptions = {
  fetchedData: TaskInstancesLogResponse | undefined;
  logLevelFilters: Array<string>;
  showSource: boolean;
  showTimestamp: boolean;
  sourceFilters: Array<string>;
  translate: TFunction;
};

/**
 * Parse raw streaming log data into plain-text lines for log download.
 * Search matching now uses the searchableText array from useLogs instead,
 * which is derived from the rendered parsedLogs to stay aligned with log groups.
 */
export const getDownloadText = ({
  fetchedData,
  logLevelFilters,
  showSource,
  showTimestamp,
  sourceFilters,
  translate,
}: GetDownloadTextOptions): Array<string> => {
  const lines = parseStreamingLogContent(fetchedData);

  return lines.map((line) =>
    renderStructuredLog({
      index: 0,
      logLevelFilters,
      logLink: "",
      logMessage: line,
      renderingMode: "text",
      showSource,
      showTimestamp,
      sourceFilters,
      translate,
    }),
  );
};

export type HighlightOptions = {
  currentMatchLineIndex?: number;
  hash: string;
  index: number;
  searchMatchIndices?: Set<number>;
};

/**
 * Returns the background color token for a virtualized log row.
 * Priority: current search match (yellow.emphasized) > URL hash line > transparent.
 * Non-current search matches use yellow.subtle for a softer highlight.
 */
export const getHighlightColor = ({
  currentMatchLineIndex,
  hash,
  index,
  searchMatchIndices,
}: HighlightOptions): string => {
  if (currentMatchLineIndex !== undefined && index === currentMatchLineIndex) {
    return "yellow.emphasized";
  }
  if (searchMatchIndices?.has(index)) {
    return "yellow.subtle";
  }
  if (Boolean(hash) && index === Number(hash) - 1) {
    return "brand.emphasized";
  }

  return "transparent";
};

/**
 * Wraps matching substrings in a log line with a highlight marker.
 * Returns an array of strings and { text, highlight } objects for rendering.
 */
export type HighlightSegment = { highlight: boolean; text: string };

export const splitBySearchQuery = (text: string, query: string): Array<HighlightSegment> => {
  if (!query) {
    return [{ highlight: false, text }];
  }

  const lowerText = text.toLowerCase();
  const lowerQuery = query.toLowerCase();
  const segments: Array<HighlightSegment> = [];
  let lastIndex = 0;

  let matchIndex = lowerText.indexOf(lowerQuery, lastIndex);

  while (matchIndex !== -1) {
    if (matchIndex > lastIndex) {
      segments.push({ highlight: false, text: text.slice(lastIndex, matchIndex) });
    }
    segments.push({ highlight: true, text: text.slice(matchIndex, matchIndex + query.length) });
    lastIndex = matchIndex + query.length;
    matchIndex = lowerText.indexOf(lowerQuery, lastIndex);
  }

  if (lastIndex < text.length) {
    segments.push({ highlight: false, text: text.slice(lastIndex) });
  }

  if (segments.length === 0) {
    return [{ highlight: false, text }];
  }

  return segments;
};

type VirtualizerInstance = Virtualizer<HTMLDivElement, Element>;

type ScrollToTopOptions = {
  element: HTMLElement;
  virtualizer: VirtualizerInstance;
};

type ScrollToBottomOptions = {
  element: HTMLElement;
  virtualizer: VirtualizerInstance;
};

/**
 * Scroll to the top of the list
 */
export const scrollToTop = ({ element, virtualizer }: ScrollToTopOptions): void => {
  virtualizer.scrollToOffset(0);
  element.scrollTop = 0;
};

/**
 * Scroll to the bottom of the list
 */
export const scrollToBottom = ({ element, virtualizer }: ScrollToBottomOptions): void => {
  const totalSize = virtualizer.getTotalSize();
  const clientHeight = element.clientHeight || 0;
  const offset = Math.max(0, totalSize - clientHeight);

  virtualizer.scrollToOffset(offset);
  element.scrollTop = offset;
};
