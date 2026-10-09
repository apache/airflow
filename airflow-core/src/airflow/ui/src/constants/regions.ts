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
import { SearchParamsKeys } from "src/constants/searchParams";

/**
 * Coordinates identify one execution, so they must not survive a move to a different one.
 * Navigation away from a task instance drops them rather than carrying them to the next page.
 */
export const clearCoordinates = (searchParams: URLSearchParams): URLSearchParams => {
  for (const key of [
    SearchParamsKeys.TRY_NUMBER,
    SearchParamsKeys.REGION_ID,
    SearchParamsKeys.REGION_INDEX,
  ]) {
    searchParams.delete(key);
  }

  return searchParams;
};

/**
 * Point a link at one execution. A task inside a loop has a row per iteration, so a link with
 * no coordinate is ambiguous and the API rejects it; the newest iteration is the sensible
 * landing place from a view that shows the task once.
 */
export const setCoordinates = (
  searchParams: URLSearchParams,
  regionId: string | null | undefined,
  regionIndex: number | null | undefined,
): URLSearchParams => {
  if (regionId !== null && regionId !== undefined && regionIndex !== null && regionIndex !== undefined) {
    searchParams.set(SearchParamsKeys.REGION_ID, regionId);
    searchParams.set(SearchParamsKeys.REGION_INDEX, String(regionIndex));
  }

  return searchParams;
};
