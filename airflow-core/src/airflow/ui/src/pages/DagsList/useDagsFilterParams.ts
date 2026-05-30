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
import { useLocalStorage } from "usehooks-ts";

import { dagsFilterKey } from "src/constants/localStorage";
import { SearchParamsKeys, type SearchParamsKeysType } from "src/constants/searchParams";

const {
  FAVORITE,
  LAST_DAG_RUN_STATE,
  NEEDS_REVIEW,
  OFFSET,
  PAUSED,
}: SearchParamsKeysType = SearchParamsKeys;

export type DagsBooleanFilterValue = "all" | "false" | "true";
export type DagsRunStateFilterValue = "all" | "failed" | "queued" | "running" | "success";

const stateValues: ReadonlyArray<DagsRunStateFilterValue> = [
  "all",
  "failed",
  "queued",
  "running",
  "success",
];
const booleanFilterValues: ReadonlyArray<DagsBooleanFilterValue> = ["all", "true", "false"];

const getSavedFilterValue = (
  searchParams: URLSearchParams,
  searchParamName: string,
  savedValue: string | null,
) => searchParams.get(searchParamName) ?? savedValue;

const toStateValue = (value: string | null): DagsRunStateFilterValue | null =>
  stateValues.includes(value as DagsRunStateFilterValue) ? (value as DagsRunStateFilterValue) : null;

const toBooleanFilterValue = (value: string | null): DagsBooleanFilterValue | null =>
  booleanFilterValues.includes(value as DagsBooleanFilterValue)
    ? (value as DagsBooleanFilterValue)
    : null;

export const useDagsFilterParams = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [savedPaused, setSavedPaused] = useLocalStorage<string | null>(dagsFilterKey(PAUSED), null);
  const [savedFavorite, setSavedFavorite] = useLocalStorage<string | null>(
    dagsFilterKey(FAVORITE),
    null,
  );
  const [savedLastDagRunState, setSavedLastDagRunState] = useLocalStorage<string | null>(
    dagsFilterKey(LAST_DAG_RUN_STATE),
    null,
  );
  const [savedNeedsReview, setSavedNeedsReview] = useLocalStorage<string | null>(
    dagsFilterKey(NEEDS_REVIEW),
    null,
  );

  const setFilterParam = ({
    persistAllInUrl = false,
    saveValue,
    searchParamName,
    value,
  }: {
    persistAllInUrl?: boolean;
    saveValue: (value: string) => void;
    searchParamName: string;
    value: DagsBooleanFilterValue | DagsRunStateFilterValue;
  }) => {
    saveValue(value);
    if (value === "all" && !persistAllInUrl) {
      searchParams.delete(searchParamName);
    } else {
      searchParams.set(searchParamName, value);
    }
    searchParams.delete(OFFSET);
    setSearchParams(searchParams);
  };

  return {
    favoriteFilter: toBooleanFilterValue(getSavedFilterValue(searchParams, FAVORITE, savedFavorite)),
    lastDagRunStateFilter: toStateValue(
      getSavedFilterValue(searchParams, LAST_DAG_RUN_STATE, savedLastDagRunState),
    ),
    needsReviewFilter: toBooleanFilterValue(
      getSavedFilterValue(searchParams, NEEDS_REVIEW, savedNeedsReview),
    ),
    pausedFilter: toBooleanFilterValue(getSavedFilterValue(searchParams, PAUSED, savedPaused)),
    setFavoriteFilter: (value: DagsBooleanFilterValue) => {
      setFilterParam({ saveValue: setSavedFavorite, searchParamName: FAVORITE, value });
    },
    setLastDagRunStateFilter: (value: DagsRunStateFilterValue) => {
      setFilterParam({ saveValue: setSavedLastDagRunState, searchParamName: LAST_DAG_RUN_STATE, value });
    },
    setNeedsReviewFilter: (value: DagsBooleanFilterValue) => {
      setFilterParam({ saveValue: setSavedNeedsReview, searchParamName: NEEDS_REVIEW, value });
    },
    setPausedFilter: (value: DagsBooleanFilterValue, persistAllInUrl = false) => {
      setFilterParam({ persistAllInUrl, saveValue: setSavedPaused, searchParamName: PAUSED, value });
    },
  };
};
