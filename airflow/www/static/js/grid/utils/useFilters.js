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

/* global defaultDagRunDisplayNumber, moment */

import { useSearchParams } from 'react-router-dom';

// Params names
export const BASE_DATE_PARAM = 'base_date';
export const NUM_RUNS_PARAM = 'num_runs';
export const RUN_TYPE_PARAM = 'run_type';
export const RUN_STATE_PARAM = 'run_state';

const date = new Date();
date.setMilliseconds(0);

export const now = date.toISOString();

const useFilters = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const baseDate = searchParams.get(BASE_DATE_PARAM) || now;
  const numRuns = searchParams.get(NUM_RUNS_PARAM) || defaultDagRunDisplayNumber;
  const runType = searchParams.get(RUN_TYPE_PARAM);
  const runState = searchParams.get(RUN_STATE_PARAM);

  const makeOnChangeFn = (paramName, formatFn) => (value) => {
    const formattedValue = formatFn ? formatFn(value) : value;
    const params = new URLSearchParams(searchParams);

    if (formattedValue) params.set(paramName, formattedValue);
    else params.delete(paramName);

    setSearchParams(params);
  };

  const onBaseDateChange = makeOnChangeFn(
    BASE_DATE_PARAM,
    (localDate) => moment(localDate).utc().format(),
  );
  const onNumRunsChange = makeOnChangeFn(NUM_RUNS_PARAM);
  const onRunTypeChange = makeOnChangeFn(RUN_TYPE_PARAM);
  const onRunStateChange = makeOnChangeFn(RUN_STATE_PARAM);

  const clearFilters = () => {
    searchParams.delete(BASE_DATE_PARAM);
    searchParams.delete(NUM_RUNS_PARAM);
    searchParams.delete(RUN_TYPE_PARAM);
    searchParams.delete(RUN_STATE_PARAM);
    setSearchParams(searchParams);
  };

  return {
    filters: {
      baseDate,
      numRuns,
      runType,
      runState,
    },
    onBaseDateChange,
    onNumRunsChange,
    onRunTypeChange,
    onRunStateChange,
    clearFilters,
  };
};

export default useFilters;
