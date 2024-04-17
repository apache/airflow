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

import { useCallback, useMemo } from "react";

import { useSearchParams } from "react-router-dom";
import type { TableState } from "./NewTable";
import {
  buildQueryParams,
  searchParamsToState,
  stateToSearchParams,
} from "./searchParams";

export const defaultTableState: TableState = {
  pagination: {
    pageIndex: 0,
    pageSize: 25,
  },
  sorting: [],
};

export const useTableURLState = (defaultState?: Partial<TableState>) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const handleStateChange = useCallback(
    (state: TableState) => {
      setSearchParams(stateToSearchParams(state, defaultTableState), {
        replace: true,
      });
    },
    [setSearchParams]
  );

  const tableURLState = useMemo(
    () =>
      searchParamsToState(searchParams, {
        ...defaultTableState,
        ...defaultState,
      }),
    [searchParams, defaultState]
  );

  const requestParams = useMemo(
    () => buildQueryParams(tableURLState),
    [tableURLState]
  );

  return {
    tableURLState,
    requestParams,
    setTableURLState: handleStateChange,
  };
};
