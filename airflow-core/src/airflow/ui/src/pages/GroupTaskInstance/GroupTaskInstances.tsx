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
import { useParams, useSearchParams } from "react-router-dom";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useIsLoopGroup } from "src/queries/useIsLoopGroup";
import { useLoopSummary } from "src/queries/useLoopSummary";

import { TaskInstances } from "../TaskInstances";
import { IterationSelect } from "./LoopIterations";
import { getSelectedIteration, ITERATION_ALL } from "./LoopIterations/loopUtils";

export const GroupTaskInstances = () => {
  const { dagId = "", groupId = "", runId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const isLoopGroup = useIsLoopGroup(groupId);
  const { data: summary, isError } = useLoopSummary({ dagId, groupId, runId });
  const param = searchParams.get(SearchParamsKeys.ITERATION);
  const iteration =
    summary === undefined
      ? (param ?? (isError ? ITERATION_ALL : undefined))
      : getSelectedIteration(param, summary);

  return (
    <TaskInstances
      extraFilter={summary === undefined ? undefined : <IterationSelect summary={summary} />}
      iteration={iteration}
      loopGroupId={isLoopGroup ? groupId : undefined}
    />
  );
};
