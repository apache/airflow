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
  DEPENDENCIES = "dependencies",
  END_DATE = "end_date",
  LAST_DAG_RUN_STATE = "last_dag_run_state",
  LIMIT = "limit",
  LOG_LEVEL = "log_level",
  NAME_PATTERN = "name_pattern",
  OFFSET = "offset",
  PAUSED = "paused",
  RUN_TYPE = "run_type",
  SORT = "sort",
  SOURCE = "log_source",
  START_DATE = "start_date",
  STATE = "state",
  TAGS = "tags",
  TRY_NUMBER = "try_number",
  VERSION_NUMBER = "version_number",
}

export type SearchParamsKeysType = Record<keyof typeof SearchParamsKeys, string>;
