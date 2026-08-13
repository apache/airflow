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
import { useEffect } from "react";
import { useSearchParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";

/**
 * Writes the ``hide_paused_dags_by_default`` default into the URL so it shows up as a
 * pill instead of filtering invisibly.
 *
 * Reset deliberately re-seeds: it deletes ``paused``, which reopens the guard below. That
 * keeps Reset idempotent with a cold page load — to see paused Dags the user picks "All",
 * which writes ``paused=all``.
 */
export const usePausedDefault = () => {
  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const [searchParams, setSearchParams] = useSearchParams();

  useEffect(() => {
    if (!hidePausedDagsByDefault || searchParams.has(SearchParamsKeys.PAUSED)) {
      return;
    }

    // Functional form is required: PresetFiltersMenu restores a default preset from a
    // deeper effect that commits first, so an object write would clobber it with stale params.
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);

        next.set(SearchParamsKeys.PAUSED, "false");

        return next;
      },
      { replace: true },
    );
  }, [hidePausedDagsByDefault, searchParams, setSearchParams]);
};
