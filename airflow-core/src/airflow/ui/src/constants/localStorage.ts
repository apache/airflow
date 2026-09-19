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
export const LOG_SHOW_LOG_LEVEL_KEY = "log_show_log_level";
export const VERSION_INDICATOR_DISPLAY_MODE_KEY = "version_indicator_display_mode";
export const COLLAPSED_UI_ALERTS_KEY = "collapsed_ui_alerts";
export const SHOW_ALL_DEPENDENCIES_KEY = "show_all_dependencies";
export const DEFAULT_GRAPH_DIRECTION_KEY = "default_graph_direction";
export const CLEAR_RUN_DEFAULT_OPTIONS_KEY = "clear_run_default_options";
export const CLEAR_TASK_INSTANCE_DEFAULT_OPTIONS_KEY = "clear_task_instance_default_options";
export const CLEAR_PREVENT_RUNNING_TASK_KEY = "clear_prevent_running_task";
export const MARK_TASK_INSTANCE_DEFAULT_OPTIONS_KEY = "mark_task_instance_default_options";
export const DEFAULT_TASK_INSTANCE_TAB_KEY = "default_task_instance_tab";
export const DEFAULT_LANDING_PAGE_KEY = "default_landing_page";

// Dag-scoped keys
export const dagRunsLimitKey = (dagId: string) => `dag_runs_limit-${dagId}`;
export const directionKey = (dagId: string) => `direction-${dagId}`;
export const openGroupsKey = (dagId: string) => `${dagId}/open-groups`;
export const allGroupsKey = (dagId: string) => `${dagId}/all-groups`;

// Page-scoped keys
export const tableSortKey = (pageName: string) => `${pageName.replaceAll("/", "-").slice(1)}-table-sort`;
export const presetFiltersKey = (pageName: string) =>
  `${pageName.replaceAll("/", "-").slice(1)}-preset-filters`;
export const presetFiltersDefaultKey = (pageName: string) =>
  `${pageName.replaceAll("/", "-").slice(1)}-preset-filters-default`;

// SearchBar advanced (substring) toggle, scoped per searchbar via a caller-provided id.
export const advancedSearchKey = (id: string) => `advanced_search-${id}`;

// One-time cleanup of the pre-consolidation per-Dag dependency toggle keys
// (`dependencies-<dag_id>`), now superseded by the global SHOW_ALL_DEPENDENCIES_KEY. Without this
// they linger in every user's localStorage forever. Safe because no current key shares the prefix.
export const pruneLegacyDependencyKeys = (storage: Storage = globalThis.localStorage): void => {
  const staleKeys: Array<string> = [];

  for (let index = 0; index < storage.length; index += 1) {
    const key = storage.key(index);

    if (key?.startsWith("dependencies-")) {
      staleKeys.push(key);
    }
  }

  staleKeys.forEach((key) => storage.removeItem(key));
};

// One-time cleanup of the per-browser tag-filter persistence (`tags` / `tags_match_mode`) that
// predates preset filters (#63273). Tags are URL-driven now like every other filter, and preset
// filters own cross-navigation persistence, so these keys are stale.
export const pruneLegacyTagFilterKeys = (storage: Storage = globalThis.localStorage): void => {
  storage.removeItem("tags");
  storage.removeItem("tags_match_mode");
};
