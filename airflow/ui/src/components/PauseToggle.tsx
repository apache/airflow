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

import { Switch } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";

import {
  DagServiceGetDagsDefaultResponse,
  DagServicePatchDagMutationResult,
  UseDagServiceGetDagsKeyFn,
  useDagServicePatchDag,
} from "openapi/queries";
import { useTableURLState } from "./DataTable/useTableUrlState";

type Props = {
  dagId: string;
  isPaused: boolean;
};

export const PauseToggle = ({ dagId, isPaused }: Props) => {
  const [searchParams] = useSearchParams();
  const { tableURLState } = useTableURLState();
  const { sorting, pagination } = tableURLState;
  const showPaused = searchParams.get("paused") === "true";
  const sort = sorting[0];
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const onSuccess = (data: DagServicePatchDagMutationResult) => {
    // Update Dags list query on result instead of refetching it
    queryClient.setQueryData(
      UseDagServiceGetDagsKeyFn({
        limit: pagination.pageSize,
        offset: pagination.pageIndex * pagination.pageSize,
        onlyActive: true,
        paused: showPaused === true ? undefined : false, // undefined returns all dags
        orderBy,
      }),
      (oldData: DagServiceGetDagsDefaultResponse) => {
        if (!showPaused && data.is_paused)
          return {
            ...oldData,
            total_entries: oldData.total_entries
              ? oldData.total_entries - 1
              : oldData.total_entries,
            dags: oldData.dags?.filter((dag) => dag.dag_id !== data.dag_id),
          };
        return {
          ...oldData,
          dags: oldData.dags?.map((dag) => (dag.dag_id === dagId ? data : dag)),
        };
      }
    );
  };
  const { mutate } = useDagServicePatchDag({ onSuccess });
  const queryClient = useQueryClient();
  const onChange = () => {
    mutate({
      dagId,
      requestBody: {
        is_paused: !isPaused,
      },
    });
  };
  return <Switch size="sm" isChecked={!isPaused} onChange={onChange} />;
};
