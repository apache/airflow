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

import { useDeadlinesServiceGetDeadlines } from "openapi/queries";

export const DASHBOARD_DEADLINE_LIMIT = 5;

type DashboardPendingDeadlinesQueryOptions = {
  readonly refetchInterval: number | false;
};

type DashboardMissedDeadlinesQueryOptions = {
  readonly endDate: string;
  readonly refetchInterval: number | false;
  readonly startDate: string;
};

export const useDashboardPendingDeadlines = ({ refetchInterval }: DashboardPendingDeadlinesQueryOptions) => {
  const now = dayjs().startOf("minute").toISOString();

  return useDeadlinesServiceGetDeadlines(
    {
      dagId: "~",
      dagRunId: "~",
      deadlineTimeGt: now,
      limit: DASHBOARD_DEADLINE_LIMIT,
      missed: false,
      orderBy: ["deadline_time"],
    },
    undefined,
    { refetchInterval },
  );
};

export const useDashboardMissedDeadlines = ({
  endDate,
  refetchInterval,
  startDate,
}: DashboardMissedDeadlinesQueryOptions) =>
  useDeadlinesServiceGetDeadlines(
    {
      dagId: "~",
      dagRunId: "~",
      deadlineTimeGte: startDate,
      deadlineTimeLt: endDate,
      limit: DASHBOARD_DEADLINE_LIMIT,
      missed: true,
      orderBy: ["-deadline_time"],
    },
    undefined,
    { refetchInterval },
  );
