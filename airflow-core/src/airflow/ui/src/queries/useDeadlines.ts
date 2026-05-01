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
import { useAutoRefresh } from "src/utils";

type UseDeadlinesParams = {
  readonly dagId: string;
  readonly enabled?: boolean;
  readonly limit: number;
  readonly offset?: number;
};

export const useDeadlines = ({ dagId, enabled, limit, offset = 0 }: UseDeadlinesParams) => {
  const refetchInterval = useAutoRefresh({ dagId });

  return useDeadlinesServiceGetDeadlines(
    {
      dagId,
      dagRunId: "~",
      limit,
      offset,
      orderBy: ["deadline_time"],
    },
    undefined,
    {
      enabled,
      refetchInterval: ({ state: { data } }) => {
        if (data === undefined) {
          return refetchInterval;
        }
        // Stop polling only when every deadline in the full result set is missed
        const allMissed = data.total_entries > 0 && data.deadlines.every((deadline) => deadline.missed);

        return allMissed ? false : refetchInterval;
      },
    },
  );
};
