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

import type { SortingState } from "@tanstack/react-table";
import { isEqual } from "lodash";

import type { TableState } from "./NewTable";

export const LIMIT_PARAM = "limit";
export const OFFSET_PARAM = "offset";
export const SORT_PARAM = "sort.";

export const stateToSearchParams = (
  state: TableState,
  defaultTableState?: TableState
): URLSearchParams => {
  const queryParams = new URLSearchParams(window.location.search);
  if (isEqual(state.pagination, defaultTableState?.pagination)) {
    queryParams.delete(LIMIT_PARAM);
    queryParams.delete(OFFSET_PARAM);
  } else if (state.pagination) {
    queryParams.set(LIMIT_PARAM, `${state.pagination.pageSize}`);
    queryParams.set(OFFSET_PARAM, `${state.pagination.pageIndex}`);
  }

  if (isEqual(state.sorting, defaultTableState?.sorting)) {
    state.sorting.forEach(({ id }) => {
      queryParams.delete(`${SORT_PARAM}${id}`);
    });
  } else if (state.sorting) {
    state.sorting.forEach(({ id, desc }) => {
      queryParams.set(`${SORT_PARAM}${id}`, desc ? "desc" : "asc");
    });
  }

  return queryParams;
};

export const searchParamsToState = (
  searchParams: URLSearchParams,
  defaultState: TableState
) => {
  let urlState: Partial<TableState> = {};
  const pageIndex = searchParams.get(OFFSET_PARAM);
  const pageSize = searchParams.get(LIMIT_PARAM);
  if (pageIndex && pageSize) {
    urlState = {
      ...urlState,
      pagination: {
        pageIndex: parseInt(pageIndex, 10),
        pageSize: parseInt(pageSize, 10),
      },
    };
  }
  const sorting: SortingState = [];
  searchParams.forEach((v, k) => {
    if (k.startsWith(SORT_PARAM)) {
      sorting.push({
        id: k.replace(SORT_PARAM, ""),
        desc: v === "desc",
      });
    }
  });
  if (sorting.length) {
    urlState = {
      ...urlState,
      sorting,
    };
  }
  return {
    ...defaultState,
    ...urlState,
  };
};

interface CoreServiceQueryParams {
  offset?: number;
  limit?: number;
  sorts?: any[];
  search?: string;
}

/**
 * helper function that build query params that core services rely on
 * @param state
 * @returns
 */
export const buildQueryParams = (
  state?: Partial<TableState>
): CoreServiceQueryParams => {
  let queryParams = {};
  if (state?.pagination) {
    const { pageIndex, pageSize } = state.pagination;
    queryParams = {
      ...queryParams,
      limit: pageSize,
      offset: pageIndex * pageSize,
    };
  }
  if (state?.sorting) {
    const sorts = state.sorting.map(
      ({ id, desc }) => `${id}:${desc ? "desc" : "asc"}`
    ) as any[];
    queryParams = {
      ...queryParams,
      sorts,
    };
  }
  return queryParams;
};
