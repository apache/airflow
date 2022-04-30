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

import { useSearchParams } from 'react-router-dom';

// Params names
const BASE_DATE = 'baseDate';
const NUM_RUNS = 'numRuns';
const RUN_TYPE = 'runType';
const RUN_STATE = 'runState';
const TASK_STATE = 'taskState';

const useFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const baseDate = searchParams.get(BASE_DATE);
  const numRuns = searchParams.get(NUM_RUNS);
  const runType = searchParams.get(RUN_TYPE);
  const runState = searchParams.get(RUN_STATE);
  // taskState is only used to change the opacity of the tasks,
  // it is not send to the api for filtering.
  const taskState = searchParams.get(TASK_STATE);

  const makeOnChangeFn = (paramName) => (e) => {
    const { value } = e.target;
    const params = new URLSearchParams(searchParams);

    if (value) params.set(paramName, value);
    else params.delete(paramName);

    setSearchParams(params);
  };

  const onBaseDateChange = makeOnChangeFn(BASE_DATE);
  const onNumRunsChange = makeOnChangeFn(NUM_RUNS);
  const onRunTypeChange = makeOnChangeFn(RUN_TYPE);
  const onRunStateChange = makeOnChangeFn(RUN_STATE);
  const onTaskStateChange = makeOnChangeFn(TASK_STATE);

  const clearFilters = () => {
    searchParams.delete(BASE_DATE);
    searchParams.delete(NUM_RUNS);
    searchParams.delete(RUN_TYPE);
    searchParams.delete(RUN_STATE);
    searchParams.delete(TASK_STATE);
    setSearchParams(searchParams);
  };

  return {
    filters: {
      baseDate,
      numRuns,
      runType,
      runState,
      taskState,
    },
    onBaseDateChange,
    onNumRunsChange,
    onRunTypeChange,
    onRunStateChange,
    onTaskStateChange,
    clearFilters,
  };
};

export default useFilters;
