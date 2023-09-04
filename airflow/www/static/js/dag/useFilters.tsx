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

/* global moment */

import { useSearchParams } from "react-router-dom";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";

declare const defaultDagRunDisplayNumber: number;

export interface Filters {
  root: string | undefined;
  filterUpstream: boolean | undefined;
  filterDownstream: boolean | undefined;
  baseDate: string | null;
  numRuns: string | null;
  runType: string | null;
  runState: string | null;
}

export interface FilterTasksProps {
  root: string;
  filterUpstream: boolean;
  filterDownstream: boolean;
}

export interface UtilFunctions {
  onBaseDateChange: (value: string) => void;
  onNumRunsChange: (value: string) => void;
  onRunTypeChange: (value: string) => void;
  onRunStateChange: (value: string) => void;
  onFilterTasksChange: (args: FilterTasksProps) => void;
  clearFilters: () => void;
  resetRoot: () => void;
}

export interface FilterHookReturn extends UtilFunctions {
  filters: Filters;
}

// Params names
export const BASE_DATE_PARAM = "base_date";
export const NUM_RUNS_PARAM = "num_runs";
export const RUN_TYPE_PARAM = "run_type";
export const RUN_STATE_PARAM = "run_state";

export const ROOT_PARAM = "root";
export const FILTER_UPSTREAM_PARAM = "filter_upstream";
export const FILTER_DOWNSTREAM_PARAM = "filter_downstream";

const date = new Date();
date.setMilliseconds(0);

export const now = date.toISOString();

const useFilters = (): FilterHookReturn => {
  const [searchParams, setSearchParams] = useSearchParams();

  const root = searchParams.get(ROOT_PARAM) || undefined;
  const filterUpstream = root
    ? searchParams.get(FILTER_UPSTREAM_PARAM) === "true"
    : undefined;
  const filterDownstream = root
    ? searchParams.get(FILTER_DOWNSTREAM_PARAM) === "true"
    : undefined;

  const baseDate = searchParams.get(BASE_DATE_PARAM) || now;
  const numRuns =
    searchParams.get(NUM_RUNS_PARAM) || defaultDagRunDisplayNumber.toString();
  const runType = searchParams.get(RUN_TYPE_PARAM);
  const runState = searchParams.get(RUN_STATE_PARAM);

  const makeOnChangeFn =
    (paramName: string, formatFn?: (arg: string) => string) =>
    (value: string) => {
      const formattedValue = formatFn ? formatFn(value) : value;
      const params = new URLSearchParamsWrapper(searchParams);

      if (formattedValue) params.set(paramName, formattedValue);
      else params.delete(paramName);

      setSearchParams(params);
    };

  const onBaseDateChange = makeOnChangeFn(
    BASE_DATE_PARAM,
    // @ts-ignore
    (localDate: string) => moment(localDate).utc().format()
  );
  const onNumRunsChange = makeOnChangeFn(NUM_RUNS_PARAM);
  const onRunTypeChange = makeOnChangeFn(RUN_TYPE_PARAM);
  const onRunStateChange = makeOnChangeFn(RUN_STATE_PARAM);

  const onFilterTasksChange = ({
    root: newRoot,
    filterUpstream: newUpstream,
    filterDownstream: newDownstream,
  }: FilterTasksProps) => {
    const params = new URLSearchParamsWrapper(searchParams);

    if (
      root === newRoot &&
      newUpstream === filterUpstream &&
      newDownstream === filterDownstream
    ) {
      params.delete(ROOT_PARAM);
      params.delete(FILTER_UPSTREAM_PARAM);
      params.delete(FILTER_DOWNSTREAM_PARAM);
    } else {
      params.set(ROOT_PARAM, newRoot);
      params.set(FILTER_UPSTREAM_PARAM, newUpstream.toString());
      params.set(FILTER_DOWNSTREAM_PARAM, newDownstream.toString());
    }

    setSearchParams(params);
  };

  const clearFilters = () => {
    searchParams.delete(BASE_DATE_PARAM);
    searchParams.delete(NUM_RUNS_PARAM);
    searchParams.delete(RUN_TYPE_PARAM);
    searchParams.delete(RUN_STATE_PARAM);
    setSearchParams(searchParams);
  };

  const resetRoot = () => {
    searchParams.delete(ROOT_PARAM);
    searchParams.delete(FILTER_UPSTREAM_PARAM);
    searchParams.delete(FILTER_DOWNSTREAM_PARAM);
    setSearchParams(searchParams);
  };

  return {
    filters: {
      root,
      filterUpstream,
      filterDownstream,
      baseDate,
      numRuns,
      runType,
      runState,
    },
    onBaseDateChange,
    onNumRunsChange,
    onRunTypeChange,
    onRunStateChange,
    onFilterTasksChange,
    clearFilters,
    resetRoot,
  };
};

export default useFilters;
