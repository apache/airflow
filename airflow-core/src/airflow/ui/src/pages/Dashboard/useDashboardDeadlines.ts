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
import { useQuery } from "@tanstack/react-query";
import dayjs from "dayjs";

import { DeadlinesService } from "openapi/requests/services.gen";

export const DASHBOARD_DEADLINE_LIMIT = 5;

type DashboardDeadlinesQueryOptions = {
  readonly refetchInterval: number | false;
};

export const getDeadlineWindow = () => {
  const currentMinute = dayjs().startOf("minute");

  return {
    lastDay: currentMinute.subtract(24, "hour").toISOString(),
    now: currentMinute.toISOString(),
  };
};

export const useDashboardPendingDeadlines = ({ refetchInterval }: DashboardDeadlinesQueryOptions) =>
  useQuery({
    queryFn: () => {
      const { now } = getDeadlineWindow();

      return DeadlinesService.getDeadlines({
        dagId: "~",
        dagRunId: "~",
        deadlineTimeGt: now,
        limit: DASHBOARD_DEADLINE_LIMIT,
        missed: false,
        orderBy: ["deadline_time"],
      });
    },
    queryKey: ["dashboard", "deadlines", "pending"],
    refetchInterval,
  });

export const useDashboardMissedDeadlines = ({ refetchInterval }: DashboardDeadlinesQueryOptions) =>
  useQuery({
    queryFn: () => {
      const { lastDay, now } = getDeadlineWindow();

      return DeadlinesService.getDeadlines({
        dagId: "~",
        dagRunId: "~",
        deadlineTimeGte: lastDay,
        deadlineTimeLt: now,
        limit: DASHBOARD_DEADLINE_LIMIT,
        missed: true,
        orderBy: ["-deadline_time"],
      });
    },
    queryKey: ["dashboard", "deadlines", "missed"],
    refetchInterval,
  });
