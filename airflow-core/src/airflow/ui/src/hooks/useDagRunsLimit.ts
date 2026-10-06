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
import { dagRunsLimitKey } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";

import { useUrlOrStoredState } from "./useUrlOrStoredState";

const DEFAULT_LIMIT = 10;

const readLimitParam = (params: URLSearchParams) => {
  const limitParam = params.get(SearchParamsKeys.LIMIT);

  return limitParam === null ? undefined : Number(limitParam);
};

// The remembered per-Dag value survives tab switches and revisits; a `?limit=` URL param overrides it.
export const useDagRunsLimit = (dagId: string) => {
  const [limit, setLimit] = useUrlOrStoredState<number>({
    defaultValue: DEFAULT_LIMIT,
    readParams: readLimitParam,
    replace: true,
    storageKey: dagRunsLimitKey(dagId),
    // The stored value takes over once the user picks a limit, so the URL doesn't pin the old one.
    writeParams: (params) => params.delete(SearchParamsKeys.LIMIT),
  });

  return { limit, setLimit };
};
