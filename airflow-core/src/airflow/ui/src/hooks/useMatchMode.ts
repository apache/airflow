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
import { useSearchParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";

export type MatchMode = "all" | "any";

/**
 * Any/all match mode for a multiselect filter, stored in its own URL param.
 *
 * Mirrors ``useAdvancedSearch``: state that belongs to a pill but sits outside the
 * value ``FilterBar`` manages. ``paramKey`` is optional so the hook can be called
 * unconditionally by editors whose config may not declare a ``matchModeKey``.
 */
export const useMatchMode = (paramKey?: string) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const mode: MatchMode = paramKey !== undefined && searchParams.get(paramKey) === "all" ? "all" : "any";

  const setMode = (next: MatchMode) => {
    if (paramKey === undefined) {
      return;
    }

    setSearchParams((prev) => {
      const params = new URLSearchParams(prev);

      // "any" is the default, so drop the param rather than spelling it out — this keeps
      // preset "is there anything to save?" checks honest.
      if (next === "any") {
        params.delete(paramKey);
      } else {
        params.set(paramKey, next);
      }
      params.delete(SearchParamsKeys.OFFSET);
      params.delete(SearchParamsKeys.CURSOR);

      return params;
    });
  };

  return { mode, setMode };
};
