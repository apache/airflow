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
import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";

export const useFilterSearchParams = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { DAG_ID_PATTERN, END_DATE, RUN_TYPE, START_DATE, TASK_ID_PATTERN } = SearchParamsKeys;

  const startDate = searchParams.get(START_DATE) ?? "";
  const endDate = searchParams.get(END_DATE) ?? "";
  const dagIdPattern = searchParams.get(DAG_ID_PATTERN) ?? "";
  const taskIdPattern = searchParams.get(TASK_ID_PATTERN) ?? "";
  const runType = searchParams.get(RUN_TYPE) ?? "";

  const handleFilterChange = useCallback(
    (paramKey: string) => (value: string) => {
      if (value === "") {
        searchParams.delete(paramKey);
      } else {
        searchParams.set(paramKey, value);
      }
      setSearchParams(searchParams);
    },
    [searchParams, setSearchParams],
  );

  return {
    dagIdPattern,
    endDate,
    handleFilterChange,
    runType,
    startDate,
    taskIdPattern,
  };
};
