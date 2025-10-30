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
import { useDagRunServiceGetDagRuns, useDagServiceGetDagDetails } from "openapi/queries";
import type { TaskInstanceState } from "openapi/requests/types.gen";
import { useConfig } from "src/queries/useConfig";

export const isStatePending = (state?: TaskInstanceState | null) =>
  state === "deferred" ||
  state === "scheduled" ||
  state === "running" ||
  state === "up_for_reschedule" ||
  state === "up_for_retry" ||
  state === "queued" ||
  state === "restarting" ||
  !Boolean(state);

// checkPendingRuns=false assumes that the component is already handling pending, setting to true will have useAutoRefresh handle it
export const useAutoRefresh = ({
  checkPendingRuns = false,
  dagId,
}: {
  checkPendingRuns?: boolean;
  dagId?: string;
}) => {
  const autoRefreshInterval = useConfig("auto_refresh_interval") as number | undefined;
  const { data: dag } = useDagServiceGetDagDetails(
    {
      dagId: dagId ?? "",
    },
    undefined,
    { enabled: dagId !== undefined },
  );

  const { data: dagRunData } = useDagRunServiceGetDagRuns(
    {
      dagId: dagId ?? "~",
      state: ["running", "queued"],
    },
    undefined,
    // Scale back refetching to 10x longer if there are no pending runs (eg: every 3 secs for active runs, otherwise 30 secs)
    {
      enabled: checkPendingRuns,
      refetchInterval: (query) =>
        autoRefreshInterval !== undefined &&
        ((query.state.data?.dag_runs ?? []).length > 0
          ? autoRefreshInterval * 1000
          : autoRefreshInterval * 10 * 1000),
    },
  );

  const pendingRuns = checkPendingRuns ? (dagRunData?.dag_runs ?? []).length >= 1 : true;

  const paused = Boolean(dagId) ? dag?.is_paused : false;

  // Suspend auto refresh when any dialog is open to avoid unmounting modal parents on refetch
  const globalWithDoc = globalThis as unknown as { document?: Document };
  const doc = globalWithDoc.document;
  const hasOpenDialog = typeof globalThis !== "undefined" && doc !== undefined
    ? Boolean(doc.querySelector('[role="dialog"]'))
    : false;

  const canRefresh = autoRefreshInterval !== undefined && !paused && pendingRuns && !hasOpenDialog;

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  return (canRefresh ? autoRefreshInterval * 1000 : false) as number | false;
};
