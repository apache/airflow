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

export interface Filters {
  startDate: string;
  endDate: string;
}

export interface UtilFunctions {
  onStartDateChange: (value: string) => void;
  onEndDateChange: (value: string) => void;
  clearFilters: () => void;
}

export interface FilterHookReturn extends UtilFunctions {
  filters: Filters;
}

// Params names
export const START_DATE_PARAM = "start_date";
export const END_DATE_PARAM = "end_date";

const date = new Date();
date.setMilliseconds(0);

export const now = date.toISOString();

const useFilters = (): FilterHookReturn => {
  const [searchParams, setSearchParams] = useSearchParams();

  const endDate = searchParams.get(END_DATE_PARAM) || now;
  const startDate =
    searchParams.get(START_DATE_PARAM) ||
    // @ts-ignore
    moment(endDate).subtract(1, "d").toISOString();

  const makeOnChangeFn =
    (paramName: string, formatFn?: (arg: string) => string) =>
    (value: string) => {
      const formattedValue = formatFn ? formatFn(value) : value;
      const params = new URLSearchParamsWrapper(searchParams);

      if (formattedValue) params.set(paramName, formattedValue);
      else params.delete(paramName);

      setSearchParams(params);
    };

  const onStartDateChange = makeOnChangeFn(
    START_DATE_PARAM,
    // @ts-ignore
    (localDate: string) => moment(localDate).utc().format()
  );

  const onEndDateChange = makeOnChangeFn(END_DATE_PARAM, (localDate: string) =>
    // @ts-ignore
    moment(localDate).utc().format()
  );

  const clearFilters = () => {
    searchParams.delete(START_DATE_PARAM);
    searchParams.delete(END_DATE_PARAM);
    setSearchParams(searchParams);
  };

  return {
    filters: {
      startDate,
      endDate,
    },
    onStartDateChange,
    onEndDateChange,
    clearFilters,
  };
};

export default useFilters;
