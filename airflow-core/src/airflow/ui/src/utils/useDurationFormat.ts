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
import { useMemo } from "react";

import type dayjsDuration from "dayjs/plugin/duration";
import { useTranslation } from "react-i18next";

import { getDuration, getRelativeTime, renderDuration, renderExactDuration } from "./datetimeUtils";

/**
 * Duration formatters bound to the language currently on screen.
 *
 * The plain formatters read the language from the i18next singleton, which React does not track, so
 * a component that shows a duration but never subscribes to `languageChanged` keeps rendering the
 * previous locale. Reading the language through `useTranslation` here makes it an ordinary render
 * input: switching language re-renders every consumer, and `locale` can be added to a caller's memo
 * dependencies so derived columns, chart options and tick callbacks rebuild with it.
 *
 * Components should always format durations through this hook. Reach for the raw functions in
 * `datetimeUtils` only outside React, and pass a locale explicitly there.
 */
/**
 * The formatters this hook returns. Column builders and other helpers that receive them should
 * `Pick` from this rather than restating the signatures, so they cannot drift from the hook.
 */
export type DurationFormat = ReturnType<typeof useDurationFormat>;

export const useDurationFormat = () => {
  const { i18n } = useTranslation();
  const locale = i18n.language;

  return useMemo(
    () => ({
      /** Elapsed time between two timestamps, counting an absent end as still running. */
      formatElapsed: (startDate?: string | null, endDate?: string | null) =>
        getDuration(startDate, endDate, locale),
      /** Relative wall-clock time, e.g. "2 hours ago". */
      formatRelative: (date: string | null | undefined) => getRelativeTime(date, locale),
      locale,
      /** Compact duration for tables, charts and tooltips, e.g. "1h 2m". */
      renderDuration: (duration: dayjsDuration.Duration | number | null | undefined) =>
        renderDuration(duration, locale),
      /** Every unit down to fractional seconds, for a `title` beside the rounded form. */
      renderExactDuration: (duration: number | null | undefined) => renderExactDuration(duration, locale),
    }),
    [locale],
  );
};
