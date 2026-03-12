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

// Global keys
export const TIMEZONE_KEY = "timezone";
export const DEFAULT_DAG_VIEW_KEY = "default_dag_view";
export const DAGS_LIST_DISPLAY_KEY = "dags_list_display";
export const CALENDAR_GRANULARITY_KEY = "calendar-granularity";
export const CALENDAR_VIEW_MODE_KEY = "calendar-view-mode";
export const LOG_WRAP_KEY = "log_wrap";
export const LOG_SHOW_TIMESTAMP_KEY = "log_show_timestamp";
export const LOG_SHOW_SOURCE_KEY = "log_show_source";
export const VERSION_INDICATOR_DISPLAY_MODE_KEY = "version_indicator_display_mode";

// Dag-scoped keys
export const dagViewKey = (dagId: string) => `dag_view-${dagId}`;
export const dagRunsLimitKey = (dagId: string) => `dag_runs_limit-${dagId}`;
export const runTypeFilterKey = (dagId: string) => `run_type_filter-${dagId}`;
export const triggeringUserFilterKey = (dagId: string) => `triggering_user_filter-${dagId}`;
export const dagRunStateFilterKey = (dagId: string) => `dag_run_state_filter-${dagId}`;
export const showGanttKey = (dagId: string) => `show_gantt-${dagId}`;
export const dependenciesKey = (dagId: string) => `dependencies-${dagId}`;
export const directionKey = (dagId: string) => `direction-${dagId}`;
export const openGroupsKey = (dagId: string) => `${dagId}/open-groups`;
export const allGroupsKey = (dagId: string) => `${dagId}/all-groups`;

// Page-scoped keys
export const tableSortKey = (pageName: string) => `${pageName.replaceAll("/", "-").slice(1)}-table-sort`;
