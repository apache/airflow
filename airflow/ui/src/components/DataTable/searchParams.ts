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

import type { TableState } from "./types";

export const LIMIT_PARAM = "limit";
export const OFFSET_PARAM = "offset";
export const SORT_PARAM = "sort";

export const stateToSearchParams = (
  state: TableState,
  defaultTableState?: TableState
): URLSearchParams => {
  const queryParams = new URLSearchParams(window.location.search);

  if (state.pagination.pageSize === defaultTableState?.pagination.pageSize) {
    queryParams.delete(LIMIT_PARAM);
  } else if (state.pagination) {
    queryParams.set(LIMIT_PARAM, `${state.pagination.pageSize}`);
  }

  if (state.pagination.pageIndex === defaultTableState?.pagination.pageIndex) {
    queryParams.delete(OFFSET_PARAM);
  } else if (state.pagination) {
    queryParams.set(OFFSET_PARAM, `${state.pagination.pageIndex}`);
  }

  if (!state.sorting.length) {
    queryParams.delete(SORT_PARAM);
  } else {
    state.sorting.forEach(({ id, desc }) => {
      if (
        defaultTableState?.sorting.find(
          (sort) => sort.id === id && sort.desc === desc
        )
      ) {
        queryParams.delete(SORT_PARAM, `${desc ? "-" : ""}${id}`);
      } else {
        queryParams.set(SORT_PARAM, `${desc ? "-" : ""}${id}`);
      }
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

  if (pageIndex) {
    urlState = {
      ...urlState,
      pagination: {
        pageIndex: parseInt(pageIndex, 10),
        pageSize: pageSize
          ? parseInt(pageSize, 10)
          : defaultState.pagination.pageSize,
      },
    };
  }
  const sorts = searchParams.getAll(SORT_PARAM);
  const sorting: SortingState = sorts.map((sort) => ({
    id: sort.replace("-", ""),
    desc: sort.startsWith("-"),
  }));
  urlState = {
    ...urlState,
    sorting,
  };
  return {
    ...defaultState,
    ...urlState,
  };
};
