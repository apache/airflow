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
import { useQueryClient, type QueryKey } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";

import {
  UseDagRunServiceGetDagRunsKeyFn,
  UseDagServiceGetDagDetailsKeyFn,
  UseDagServiceGetDagKeyFn,
  useDagServicePatchDag,
  useDagServiceGetDagsUiKey,
  UseTaskInstanceServiceGetTaskInstancesKeyFn,
} from "openapi/queries";
import type {
  DAGDetailsResponse,
  DAGPatchBody,
  DAGResponse,
  DAGWithLatestDagRunsCollectionResponse,
} from "openapi/requests/types.gen";
import { createErrorToaster } from "src/utils";

type TogglePauseVariables = { dagId: string; requestBody: DAGPatchBody };

type TogglePauseContext = {
  previousDag: DAGResponse | undefined;
  previousDagDetails: DAGDetailsResponse | undefined;
  previousDagsLists: Array<[QueryKey, DAGWithLatestDagRunsCollectionResponse | undefined]>;
};

export const useTogglePause = ({ dagId }: { dagId: string }) => {
  const queryClient = useQueryClient();
  const { t: translate } = useTranslation("common");

  const dagKey = UseDagServiceGetDagKeyFn({ dagId }, [{ dagId }]);
  const dagDetailsKey = UseDagServiceGetDagDetailsKeyFn({ dagId }, [{ dagId }]);
  const dagsListPrefix: QueryKey = [useDagServiceGetDagsUiKey];

  const onMutate = async ({ requestBody }: TogglePauseVariables): Promise<TogglePauseContext> => {
    const nextIsPaused = requestBody.is_paused;

    // Cancel in-flight refetches so they cannot overwrite the optimistic update.
    await Promise.all([
      queryClient.cancelQueries({ queryKey: dagKey }),
      queryClient.cancelQueries({ queryKey: dagDetailsKey }),
      queryClient.cancelQueries({ queryKey: dagsListPrefix }),
    ]);

    const previousDag = queryClient.getQueryData<DAGResponse>(dagKey);
    const previousDagDetails = queryClient.getQueryData<DAGDetailsResponse>(dagDetailsKey);
    const previousDagsLists = queryClient.getQueriesData<DAGWithLatestDagRunsCollectionResponse>({
      queryKey: dagsListPrefix,
    });

    // Optimistically reflect the new is_paused value so the Switch flips
    // immediately on click rather than waiting for the server round-trip.
    if (previousDag !== undefined) {
      queryClient.setQueryData<DAGResponse>(dagKey, { ...previousDag, is_paused: nextIsPaused });
    }
    if (previousDagDetails !== undefined) {
      queryClient.setQueryData<DAGDetailsResponse>(dagDetailsKey, {
        ...previousDagDetails,
        is_paused: nextIsPaused,
      });
    }
    queryClient.setQueriesData<DAGWithLatestDagRunsCollectionResponse>(
      { queryKey: dagsListPrefix },
      (current) =>
        current === undefined
          ? current
          : {
              ...current,
              dags: current.dags.map((dag) =>
                dag.dag_id === dagId ? { ...dag, is_paused: nextIsPaused } : dag,
              ),
            },
    );

    return { previousDag, previousDagDetails, previousDagsLists };
  };

  const onError = (
    error: unknown,
    _variables: TogglePauseVariables,
    context: TogglePauseContext | undefined,
  ) => {
    // Roll back the optimistic update if the server rejected the change.
    if (context !== undefined) {
      if (context.previousDag !== undefined) {
        queryClient.setQueryData(dagKey, context.previousDag);
      }
      if (context.previousDagDetails !== undefined) {
        queryClient.setQueryData(dagDetailsKey, context.previousDagDetails);
      }
      context.previousDagsLists.forEach(([key, data]) => {
        queryClient.setQueryData(key, data);
      });
    }

    createErrorToaster(error, { titleKey: "common:error.title" }, translate);
  };

  const onSettled = async () => {
    // Invalidate after the mutation settles (success or error) so filtered list
    // queries (e.g. paused=true/false) refetch and may move the dag in or out
    // of the visible page.
    const queryKeys: Array<QueryKey> = [
      dagsListPrefix,
      dagKey,
      dagDetailsKey,
      UseDagRunServiceGetDagRunsKeyFn({ dagId }, [{ dagId }]),
      UseTaskInstanceServiceGetTaskInstancesKeyFn({ dagId, dagRunId: "~" }, [{ dagId, dagRunId: "~" }]),
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));
  };

  return useDagServicePatchDag({
    onError,
    onMutate,
    onSettled,
  });
};
