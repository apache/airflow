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
import { useGridServiceGetGridTiSummaries } from "openapi/queries";
import type { TaskInstanceState } from "openapi/requests";
import { isStatePending, useAutoRefresh } from "src/utils";

export const useGridTiSummaries = ({
  dagId,
  enabled,
  runId,
  state,
}: {
  dagId: string;
  enabled?: boolean;
  runId: string;
  state?: TaskInstanceState | null | undefined;
}) => {
  const refetchInterval = useAutoRefresh({ dagId });

  const { data: gridTiSummaries, ...rest } = useGridServiceGetGridTiSummaries(
    {
      dagId,
      runId,
    },
    undefined,
    {
      enabled: Boolean(runId) && Boolean(dagId) && enabled,
      placeholderData: (prev) => prev,
      refetchInterval: (query) =>
        ((state !== undefined && isStatePending(state)) ||
          query.state.data?.task_instances.some((ti) => isStatePending(ti.state))) &&
        refetchInterval,
    },
  );

  return { data: gridTiSummaries, ...rest };
};
