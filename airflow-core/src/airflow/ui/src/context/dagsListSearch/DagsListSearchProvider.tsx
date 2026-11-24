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
import { type PropsWithChildren, useState } from "react";

import { useLocation, useMatch } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";

import { DagsListSearchContext } from "./Context";

const LIST_POSITION_KEYS = [
  SearchParamsKeys.CURSOR,
  SearchParamsKeys.LIMIT,
  SearchParamsKeys.OFFSET,
  SearchParamsKeys.SORT,
];

const removeListPosition = (search: string) => {
  const params = new URLSearchParams(search);

  LIST_POSITION_KEYS.forEach((key) => params.delete(key));

  return params.toString();
};

// Remembers the Dag list's last query so links back to the list can reopen the same view. It lives
// in memory above the routes, so each browser tab keeps its own and a new tab starts from the
// plain list.
export const DagsListSearchProvider = ({ children }: PropsWithChildren) => {
  const { search } = useLocation();
  const isOnDagsList = useMatch("/dags") !== null;
  const [listSearch, setListSearch] = useState("");
  const currentListSearch = isOnDagsList ? removeListPosition(search) : listSearch;

  if (currentListSearch !== listSearch) {
    setListSearch(currentListSearch);
  }

  return <DagsListSearchContext.Provider value={listSearch}>{children}</DagsListSearchContext.Provider>;
};
