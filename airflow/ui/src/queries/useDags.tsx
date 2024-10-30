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
import {
  useDagServiceGetDags,
  useDagsServiceRecentDagRuns,
} from "openapi/queries";
import type { DagRunState } from "openapi/requests/types.gen";

const queryOptions = {
  refetchOnMount: true,
  refetchOnReconnect: false,
  refetchOnWindowFocus: false,
  staleTime: 5 * 60 * 1000,
};

export const useDags = (
  searchParams: {
    dagDisplayNamePattern?: string;
    dagIdPattern?: string;
    lastDagRunState?: DagRunState;
    limit?: number;
    offset?: number;
    onlyActive?: boolean;
    orderBy?: string;
    owners?: Array<string>;
    paused?: boolean;
    tags?: Array<string>;
  } = {},
) => {
  const { data, error, isFetching, isLoading } = useDagServiceGetDags(
    searchParams,
    undefined,
    queryOptions,
  );

  const { lastDagRunState, orderBy, ...runsParams } = searchParams;
  const {
    data: runsData,
    error: runsError,
    isFetching: isRunsFetching,
    isLoading: isRunsLoading,
  } = useDagsServiceRecentDagRuns(
    {
      ...runsParams,
      dagRunsLimit: 14,
    },
    undefined,
    queryOptions,
  );

  const dags = (data?.dags ?? []).map((dag) => {
    const dagWithRuns = runsData?.dags.find(
      (runsDag) => runsDag.dag_id === dag.dag_id,
    );

    return dagWithRuns ?? { ...dag, latest_dag_runs: [] };
  });

  return {
    data: { dags, total_entries: data?.total_entries ?? 0 },
    error: error ?? runsError,
    isFetching: isFetching || isRunsFetching,
    isLoading: isLoading || isRunsLoading,
  };
};
