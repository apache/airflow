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
import { useDeadlinesServiceGetDeadlines } from "openapi/queries";

type UseDeadlinesParams = {
  readonly dagId: string;
  readonly enabled?: boolean;
  readonly endDate: string;
  readonly limit: number;
  readonly missed: boolean;
  readonly offset?: number;
  readonly refetchInterval?: number | false;
  readonly startDate: string;
};

export const useDeadlines = ({
  dagId,
  enabled,
  endDate,
  limit,
  missed,
  offset = 0,
  refetchInterval,
  startDate,
}: UseDeadlinesParams) =>
  useDeadlinesServiceGetDeadlines(
    missed
      ? {
          dagId,
          dagRunId: "~",
          lastUpdatedAtGte: startDate,
          lastUpdatedAtLte: endDate,
          limit,
          missed: true,
          offset,
          orderBy: ["-last_updated_at"],
        }
      : {
          dagId,
          dagRunId: "~",
          deadlineTimeGte: endDate,
          limit,
          missed: false,
          offset,
          orderBy: ["deadline_time"],
        },
    undefined,
    { enabled, refetchInterval },
  );
