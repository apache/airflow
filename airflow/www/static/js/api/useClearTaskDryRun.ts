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

import axios, { AxiosResponse } from "axios";
import { useQuery } from "react-query";
import type { MinimalTaskInstance } from "src/types";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";
import { getMetaValue } from "../utils";

const csrfToken = getMetaValue("csrf_token");
const clearUrl = getMetaValue("clear_url");

const useClearTaskDryRun = ({
  dagId,
  runId,
  taskId,
  executionDate,
  isGroup,
  past,
  future,
  upstream,
  downstream,
  recursive,
  failed,
  mapIndexes = [],
}: {
  dagId: string;
  runId: string;
  taskId: string;
  executionDate: string;
  isGroup: boolean;
  past: boolean;
  future: boolean;
  upstream: boolean;
  downstream: boolean;
  recursive: boolean;
  failed: boolean;
  mapIndexes?: number[];
}) =>
  useQuery(
    [
      "clearTask",
      dagId,
      runId,
      taskId,
      mapIndexes,
      past,
      future,
      upstream,
      downstream,
      recursive,
      failed,
    ],
    () => {
      const params = new URLSearchParamsWrapper({
        csrf_token: csrfToken,
        dag_id: dagId,
        dag_run_id: runId,
        confirmed: false,
        execution_date: executionDate,
        past,
        future,
        upstream,
        downstream,
        recursive,
        only_failed: failed,
      });

      if (isGroup) {
        params.append("group_id", taskId);
      } else {
        params.append("task_id", taskId);
      }

      mapIndexes.forEach((mi: number) => {
        params.append("map_index", mi.toString());
      });

      return axios.post<AxiosResponse, MinimalTaskInstance[]>(
        clearUrl,
        params.toString(),
        {
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
          },
        }
      );
    }
  );

export default useClearTaskDryRun;
