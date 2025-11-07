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
import { useMemo } from "react";

import { FilterBar } from "src/components/FilterBar";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

type EventsFiltersProps = {
  readonly urlDagId?: string;
  readonly urlRunId?: string;
  readonly urlTaskId?: string;
};

export const EventsFilters = ({ urlDagId, urlRunId, urlTaskId }: EventsFiltersProps) => {
  const searchParamKeys = useMemo((): Array<FilterableSearchParamsKeys> => {
    const keys: Array<FilterableSearchParamsKeys> = [
      SearchParamsKeys.EVENT_DATE_RANGE,
      SearchParamsKeys.EVENT_TYPE,
      SearchParamsKeys.USER,
      SearchParamsKeys.MAP_INDEX,
      SearchParamsKeys.TRY_NUMBER,
    ];

    // Only add DAG ID filter if not in URL context
    if (urlDagId === undefined) {
      keys.push(SearchParamsKeys.DAG_ID);
    }

    // Only add Run ID filter if not in URL context
    if (urlRunId === undefined) {
      keys.push(SearchParamsKeys.RUN_ID);
    }

    // Only add Task ID filter if not in URL context
    if (urlTaskId === undefined) {
      keys.push(SearchParamsKeys.TASK_ID);
    }

    return keys;
  }, [urlDagId, urlRunId, urlTaskId]);

  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(searchParamKeys);

  return (
    <FilterBar configs={filterConfigs} initialValues={initialValues} onFiltersChange={handleFiltersChange} />
  );
};
