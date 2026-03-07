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
import { Navigate, useLocation, useSearchParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";

import { DagRuns } from "../../DagRuns";

const RUN_TYPE_PARAM = SearchParamsKeys.RUN_TYPE;

/**
 * PartitionedRuns displays dag runs filtered to asset-triggered (partitioned) runs
 * for consumer DAGs using PartitionedAssetTimetable.
 * It ensures run_type=asset_triggered is set when the user navigates to this tab.
 */
export const PartitionedRuns = () => {
  const [searchParams] = useSearchParams();
  const location = useLocation();

  if (!searchParams.get(RUN_TYPE_PARAM)) {
    const next = new URLSearchParams(searchParams);
    next.set(RUN_TYPE_PARAM, "asset_triggered");
    return (
      <Navigate
        replace
        to={`${location.pathname}?${next.toString()}`}
      />
    );
  }

  return <DagRuns />;
};
