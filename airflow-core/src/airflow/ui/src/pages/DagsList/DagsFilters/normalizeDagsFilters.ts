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
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";

const {
  DAG_RUN_STATE,
  FAVORITE,
  LAST_DAG_RUN_STATE,
  NEEDS_REVIEW,
  OWNERS,
  PAUSED,
  TAGS,
  TAGS_MATCH_MODE,
  TEAMS,
  TIMETABLE_TYPE,
}: SearchParamsKeysType = SearchParamsKeys;

const booleanValues = new Set(["all", "false", "true"]);
const positiveBooleanValues = new Set(["all", "true"]);
const runStateValues = new Set(["failed", "queued", "running", "success"]);
const tagMatchModes = new Set(["all", "any"]);

export const getUniqueSearchParamValues = (searchParams: URLSearchParams, key: string) => [
  ...new Set(searchParams.getAll(key).filter(Boolean)),
];

const normalizeSingleValue = (params: URLSearchParams, key: string, allowedValues: Set<string>) => {
  const value = params.getAll(key).find((candidate) => allowedValues.has(candidate));

  params.delete(key);
  if (value !== undefined) {
    params.set(key, value);
  }
};

const normalizeMultiValue = (params: URLSearchParams, key: string) => {
  const values = getUniqueSearchParamValues(params, key);

  params.delete(key);
  values.forEach((value) => params.append(key, value));
};

export const getNormalizedDagsFilterSearchParams = (searchParams: URLSearchParams) => {
  const normalized = new URLSearchParams(searchParams);

  normalizeSingleValue(normalized, DAG_RUN_STATE, runStateValues);
  normalizeSingleValue(normalized, LAST_DAG_RUN_STATE, runStateValues);
  normalizeSingleValue(normalized, FAVORITE, booleanValues);
  normalizeSingleValue(normalized, NEEDS_REVIEW, positiveBooleanValues);
  normalizeSingleValue(normalized, PAUSED, booleanValues);
  normalizeMultiValue(normalized, TAGS);
  normalizeMultiValue(normalized, OWNERS);
  normalizeMultiValue(normalized, TEAMS);
  normalizeMultiValue(normalized, TIMETABLE_TYPE);

  const tagMatchMode = normalized.getAll(TAGS_MATCH_MODE).find((value) => tagMatchModes.has(value));

  normalized.delete(TAGS_MATCH_MODE);
  if (normalized.has(TAGS) && tagMatchMode !== undefined) {
    normalized.set(TAGS_MATCH_MODE, tagMatchMode);
  }

  return normalized;
};

export const getNormalizedTagMatchMode = (value: unknown, fallback: "all" | "any" = "any") =>
  value === "all" || value === "any" ? value : fallback;
