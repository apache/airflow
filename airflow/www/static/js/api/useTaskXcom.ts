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

import type { API } from "src/types";
import { getMetaValue } from "src/utils";
import { useQuery } from "react-query";
import axios, { AxiosResponse } from "axios";

// tryNumber is not required to get XCom keys or values but is used
// in query key so refetch will occur if new tries are available
interface TaskXcomCollectionProps extends API.GetXcomEntriesVariables {
  tryNumber: number;
}
interface TaskXcomProps extends API.GetXcomEntryVariables {
  tryNumber: number;
}

export const useTaskXcomCollection = ({
  dagId,
  dagRunId,
  taskId,
  mapIndex,
  tryNumber,
}: TaskXcomCollectionProps) =>
  useQuery(["taskXcoms", dagId, dagRunId, taskId, mapIndex, tryNumber], () =>
    axios.get<AxiosResponse, API.XComCollection>(
      getMetaValue("task_xcom_entries_api")
        .replace("_DAG_RUN_ID_", dagRunId)
        .replace("_TASK_ID_", taskId),
      { params: { map_index: mapIndex } }
    )
  );

export const useTaskXcomEntry = ({
  dagId,
  dagRunId,
  taskId,
  mapIndex,
  xcomKey,
  tryNumber,
}: TaskXcomProps) =>
  useQuery(
    ["taskXcom", dagId, dagRunId, taskId, mapIndex, xcomKey, tryNumber],
    () =>
      axios.get<AxiosResponse, API.XCom>(
        getMetaValue("task_xcom_entry_api")
          .replace("_DAG_RUN_ID_", dagRunId)
          .replace("_TASK_ID_", taskId)
          .replace("_XCOM_KEY_", xcomKey),
        { params: { map_index: mapIndex, stringify: false } }
      ),
    {
      enabled: !!xcomKey,
    }
  );
