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

const EXECUTION_PARAMS = [
  SearchParamsKeys.TRY_NUMBER,
  SearchParamsKeys.REGION_ID,
  SearchParamsKeys.REGION_INDEX,
  SearchParamsKeys.ITERATION,
  SearchParamsKeys.LOOP_REGION_ID,
];

/** A copy of `searchParams` without the params that pin one execution, try or loop iteration. */
export const stripExecutionParams = (searchParams: string | URLSearchParams): URLSearchParams => {
  const stripped = new URLSearchParams(searchParams);

  for (const key of EXECUTION_PARAMS) {
    stripped.delete(key);
  }

  return stripped;
};
