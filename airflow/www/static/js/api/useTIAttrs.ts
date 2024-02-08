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

import { getMetaValue } from "src/utils";
import type { TaskInstanceAttributes } from "src/types";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";

interface Props {
  dagId: string;
  runId: string;
  taskId: string;
}

export default function useTIAttrs({ dagId, runId, taskId }: Props) {
  return useQuery(["tiAttrs", dagId, runId, taskId], () => {
    const url = getMetaValue("task_instance_attributes_url");
    const params = new URLSearchParamsWrapper({
      dag_id: dagId,
      run_id: runId,
      task_id: taskId,
    });
    return axios.get<AxiosResponse, TaskInstanceAttributes>(
      `${url}?${params.toString()}`
    );
  });
}
