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
import { useEffect, useRef, useState } from "react";

import dayjs from "dayjs";
import { useSearchParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";

const DEFAULT_RANGE_DAYS = 7;

/**
 * Opens the audit log on the last week of events, since loading every event of a busy instance is slow.
 *
 * The default goes into the URL so it shows up as a date range pill that can be cleared. Until that
 * write lands the returned value stands in for it, so the unfiltered request is never sent.
 * Seeds once per page load, so removing the pill shows every event instead of putting it back.
 */
export const useDefaultAfter = (enabled: boolean) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [pendingAfter, setPendingAfter] = useState(() =>
    enabled && !searchParams.has(SearchParamsKeys.AFTER) && !searchParams.has(SearchParamsKeys.BEFORE)
      ? dayjs().subtract(DEFAULT_RANGE_DAYS, "day").toISOString()
      : undefined,
  );
  const hasSeeded = useRef(false);

  useEffect(() => {
    if (pendingAfter === undefined) {
      return;
    }

    // React Router commits the URL change in a transition, so the pending value has to outlive the
    // write; dropping it straight away renders once with neither and sends the unfiltered request.
    if (searchParams.has(SearchParamsKeys.AFTER)) {
      setPendingAfter(undefined);

      return;
    }

    if (hasSeeded.current) {
      return;
    }

    hasSeeded.current = true;

    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);

        next.set(SearchParamsKeys.AFTER, pendingAfter);

        return next;
      },
      { replace: true },
    );
  }, [pendingAfter, searchParams, setSearchParams]);

  return pendingAfter;
};
