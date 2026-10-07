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
import { useLocation } from "react-router-dom";

import type { DAGRunResponse } from "openapi/requests/types.gen";

import { DagRunTab } from "src/constants/tab";
import type { TabItem } from "src/hooks/useRequiredActionTabs";
import { canHaveUpstreamAssetEvents } from "src/utils/assetEvents";

/**
 * Drops Dag run tabs the run itself rules out.
 *
 * Reading how the run was triggered rather than the Dag's current schedule keeps the answer correct
 * after the Dag is edited: a run triggered back when the Dag was asset-scheduled still shows its
 * events, and a new cron run of a formerly asset-scheduled Dag does not.
 *
 * A tab the user is currently on is always kept, so deep links keep working and the tab bar never
 * renders with nothing selected.
 */
export const useDagRunTabs = <T extends TabItem>(
  dagRun: DAGRunResponse | undefined,
  tabs: Array<T>,
): { tabs: Array<T> } => {
  const { pathname } = useLocation();
  const lastSegment = pathname.split("/").pop() ?? "";

  // Unknown until the run loads, so everything stays put rather than flickering out and back.
  const canFill: Record<string, boolean> = {
    [DagRunTab.AssetEvents]: canHaveUpstreamAssetEvents(dagRun),
  };

  return { tabs: tabs.filter((tab) => (canFill[tab.value] ?? true) || tab.value === lastSegment) };
};
