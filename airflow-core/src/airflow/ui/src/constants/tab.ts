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

export enum TabEntity {
  Dag = "dag",
  Task = "task",
  TaskInstance = "task-instance",
}

export enum TabName {
  Backfills = "backfills",
  Calendar = "calendar",
  Code = "code",
  Details = "details",
  Events = "events",
  Overview = "",
  Runs = "runs",
  Tasks = "tasks",
}

/** Route path segments for the task-instance detail tabs. Single source of truth powering the router. */
export enum TaskInstanceTab {
  AssetEvents = "asset_events",
  Code = "code",
  Details = "details",
  Events = "events",
  Logs = "logs",
  RenderedTemplates = "rendered_templates",
  RequiredActions = "required_actions",
  TaskInstances = "task_instances",
  TaskStateStore = "task-state-store",
  XCom = "xcom",
}

/** Plain-string form of the tab paths, for APIs that accept a path segment. */
export type TaskInstanceTabValue = `${TaskInstanceTab}`;

/**
 * Tabs offered as the user's default landing tab on the Settings page, each mapped to the
 * route path it redirects to. Logs is the index route, so it maps to "" (renders in place).
 */
export const DEFAULT_TASK_INSTANCE_TAB_PATHS = {
  asset_events: "asset_events",
  code: "code",
  details: "details",
  events: "events",
  logs: "",
  rendered_templates: "rendered_templates",
  xcom: "xcom",
} as const satisfies Partial<Record<TaskInstanceTabValue, TaskInstanceTabValue | "">>;

export type DefaultTaskInstanceTab = keyof typeof DEFAULT_TASK_INSTANCE_TAB_PATHS;
