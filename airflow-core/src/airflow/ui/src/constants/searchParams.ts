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
export enum SearchParamsKeys {
  AFTER = "after",
  BEFORE = "before",
  DAG_DISPLAY_NAME_PATTERN = "dag_display_name_pattern",
  DAG_ID = "dag_id",
  DAG_ID_PATTERN = "dag_id_pattern",
  DEPENDENCIES = "dependencies",
  END_DATE = "end_date",
  EVENT_TYPE = "event_type",
  EXCLUDED_EVENTS = "excluded_events",
  FAVORITE = "favorite",
  INCLUDED_EVENTS = "included_events",
  KEY_PATTERN = "key_pattern",
  LAST_DAG_RUN_STATE = "last_dag_run_state",
  LIMIT = "limit",
  LOG_LEVEL = "log_level",
  LOGICAL_DATE_GTE = "logical_date_gte",
  LOGICAL_DATE_LTE = "logical_date_lte",
  MAP_INDEX = "map_index",
  MAPPED = "mapped",
  NAME_PATTERN = "name_pattern",
  NEEDS_REVIEW = "needs_review",
  OFFSET = "offset",
  OPERATOR = "operator",
  OWNERS = "owners",
  PAUSED = "paused",
  POOL = "pool",
  RESPONSE_RECEIVED = "response_received",
  RETRIES = "retries",
  RUN_AFTER_GTE = "run_after_gte",
  RUN_AFTER_LTE = "run_after_lte",
  RUN_ID = "run_id",
  RUN_ID_PATTERN = "run_id_pattern",
  RUN_TYPE = "run_type",
  SORT = "sort",
  SOURCE = "log_source",
  START_DATE = "start_date",
  STATE = "state",
  TAGS = "tags",
  TAGS_MATCH_MODE = "tags_match_mode",
  TASK_ID = "task_id",
  TASK_ID_PATTERN = "task_id_pattern",
  TRIGGER_RULE = "trigger_rule",
  TRIGGERING_USER_NAME_PATTERN = "triggering_user_name_pattern",
  TRY_NUMBER = "try_number",
  USER = "user",
  VERSION_NUMBER = "version_number",
}

export type SearchParamsKeysType = Record<keyof typeof SearchParamsKeys, string>;
