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

import axios from "axios";
import { useMutation, useQueryClient } from "react-query";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";
import { getMetaValue } from "../utils";
import { useAutoRefresh } from "../context/autorefresh";
import useErrorToast from "../utils/useErrorToast";

const csrfToken = getMetaValue("csrf_token");
const successUrl = getMetaValue("success_url");

export default function useMarkSuccessTask({
  dagId,
  runId,
  taskId,
}: {
  dagId: string;
  runId: string;
  taskId: string;
}) {
  const queryClient = useQueryClient();
  const errorToast = useErrorToast();
  const { startRefresh } = useAutoRefresh();
  return useMutation(
    ["markSuccess", dagId, runId, taskId],
    ({
      past,
      future,
      upstream,
      downstream,
      mapIndexes = [],
    }: {
      past: boolean;
      future: boolean;
      upstream: boolean;
      downstream: boolean;
      mapIndexes: number[];
    }) => {
      const params = new URLSearchParamsWrapper({
        csrf_token: csrfToken,
        dag_id: dagId,
        dag_run_id: runId,
        task_id: taskId,
        confirmed: true,
        past,
        future,
        upstream,
        downstream,
      });

      mapIndexes.forEach((mi: number) => {
        params.append("map_index", mi.toString());
      });

      return axios.post(successUrl, params.toString(), {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
      });
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries("gridData");
        queryClient.invalidateQueries([
          "mappedInstances",
          dagId,
          runId,
          taskId,
        ]);
        startRefresh();
      },
      onError: (error: Error) => errorToast({ error }),
    }
  );
}
