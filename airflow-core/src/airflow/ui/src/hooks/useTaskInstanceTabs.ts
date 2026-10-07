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

import { useTaskServiceGetTask } from "openapi/queries";

import { TaskInstanceTab } from "src/constants/tab";
import type { TabItem } from "src/hooks/useRequiredActionTabs";

type Params = {
  dagId: string;
  /** False until the task instance is known, so the task is never fetched unpinned first. */
  isVersionKnown: boolean;
  taskId: string;
  /** The version this task instance ran under; omit to describe the Dag as it is now. */
  versionNumber?: number;
};

/**
 * Drops task-instance tabs the Dag definition rules out.
 *
 * Both rules are answered from the task alone, so they hold before the task has run and cost no
 * extra request: a task with no outlets can never emit an asset event, and one with no template
 * fields never renders anything.
 *
 * Tabs whose emptiness is only knowable from observed data are left alone. An empty tab is a
 * smaller cost than a tab that disappears once its query comes back.
 *
 * A tab the user is currently on is always kept, so deep links keep working and the tab bar
 * never renders with nothing selected.
 */
export const useTaskInstanceTabs = <T extends TabItem>(
  { dagId, isVersionKnown, taskId, versionNumber }: Params,
  tabs: Array<T>,
): { tabs: Array<T> } => {
  const { pathname } = useLocation();
  const lastSegment = pathname.split("/").pop() ?? "";

  // Pinned to the version this instance ran under, so an older instance is judged by the
  // definition it actually ran, not by a later edit. A run can span versions, so this comes
  // from the task instance rather than the run.
  //
  // Held until the instance is known: fetching unpinned first would answer from the latest
  // version, then answer again from the pinned one, which is exactly the flicker this avoids.
  const { data: task } = useTaskServiceGetTask({ dagId, taskId, versionNumber }, undefined, {
    enabled: Boolean(dagId) && Boolean(taskId) && isVersionKnown,
  });

  // Unknown until the task loads, so everything stays put rather than flickering out and back.
  // `has_outlets` is optional, so a server that omits it leaves the tab alone too: only a
  // definite "no outlets" hides it.
  const canFill: Record<string, boolean> = {
    [TaskInstanceTab.AssetEvents]: task?.has_outlets !== false,
    [TaskInstanceTab.RenderedTemplates]: task === undefined || (task.template_fields ?? []).length > 0,
  };

  return { tabs: tabs.filter((tab) => (canFill[tab.value] ?? true) || tab.value === lastSegment) };
};
