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
import { useLocalStorage } from "usehooks-ts";

import type { Direction } from "src/components/Graph/DirectionDropdown";

import {
  CLEAR_PREVENT_RUNNING_TASK_KEY,
  CLEAR_RUN_DEFAULT_OPTIONS_KEY,
  CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
  DEFAULT_GRAPH_DIRECTION_KEY,
  DEFAULT_LANDING_PAGE_KEY,
  DEFAULT_TASK_INSTANCE_TAB_KEY,
  MARK_TASK_INSTANCE_DEFAULT_OPTIONS_KEY,
} from "src/constants/localStorage";
import type { DefaultTaskInstanceTab } from "src/constants/tab";

/** Page shown at the app root. */
export type LandingPageOption = "dags" | "dashboard";

/**
 * User-configurable defaults surfaced in the Settings page and consumed as
 * fallbacks by the individual features. Everything is persisted in this
 * browser's localStorage only — there is no server-side user profile.
 */

/** Fallback graph layout direction used when a graph has no per-graph choice. */
export const useDefaultGraphDirection = () =>
  useLocalStorage<Direction>(DEFAULT_GRAPH_DIRECTION_KEY, "RIGHT");

/** Default selection for the Dag-run Clear dialog toggle (existing / only-failed / queue-new). */
export const useClearRunDefaultOptions = () =>
  useLocalStorage<Array<string>>(CLEAR_RUN_DEFAULT_OPTIONS_KEY, ["existingTasks"]);

/** Default selection for the task-instance Clear dialog toggle (past / future / … / only-failed). */
export const useClearTaskInstanceDefaultOptions = () =>
  useLocalStorage<Array<string>>(CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY, ["downstream"]);

/** Default state of the "prevent running tasks" checkbox when clearing task instances. */
export const useClearPreventRunningTaskDefault = () =>
  useLocalStorage<boolean>(CLEAR_PREVENT_RUNNING_TASK_KEY, true);

/** Default selection for the "Mark as" task-instance dialog toggle (past / future / … ). */
export const useMarkTaskInstanceDefaultOptions = () =>
  useLocalStorage<Array<string>>(MARK_TASK_INSTANCE_DEFAULT_OPTIONS_KEY, []);

/** Tab shown first when a task instance is opened without an explicit tab in the URL. */
export const useDefaultTaskInstanceTab = () =>
  useLocalStorage<DefaultTaskInstanceTab>(DEFAULT_TASK_INSTANCE_TAB_KEY, "logs");

/** Page the app root ("/") lands on: the dashboard or the Dags list. */
export const useDefaultLandingPage = () =>
  useLocalStorage<LandingPageOption>(DEFAULT_LANDING_PAGE_KEY, "dashboard");
