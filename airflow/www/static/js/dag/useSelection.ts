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

import { useSearchParams } from "react-router-dom";
import URLSearchParamsWrapper from "src/utils/URLSearchParamWrapper";

const RUN_ID = "dag_run_id";
const TASK_ID = "task_id";
const MAP_INDEX = "map_index";

export interface SelectionProps {
  runId?: string | null;
  taskId?: string | null;
  mapIndex?: number;
}

const useSelection = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  // Clear selection, but keep other search params
  const clearSelection = () => {
    searchParams.delete(RUN_ID);
    searchParams.delete(TASK_ID);
    searchParams.delete(MAP_INDEX);
    setSearchParams(searchParams);
  };

  const onSelect = ({ runId, taskId, mapIndex }: SelectionProps) => {
    const params = new URLSearchParamsWrapper(searchParams);

    if (runId) params.set(RUN_ID, runId);
    else params.delete(RUN_ID);

    if (taskId) params.set(TASK_ID, taskId);
    else params.delete(TASK_ID);

    if (mapIndex || mapIndex === 0) params.set(MAP_INDEX, mapIndex.toString());
    else params.delete(MAP_INDEX);

    setSearchParams(params);
  };

  const runId = searchParams.get(RUN_ID);
  const taskId = searchParams.get(TASK_ID);
  const mapIndexParam = searchParams.get(MAP_INDEX);
  const mapIndex =
    mapIndexParam !== null ? parseInt(mapIndexParam, 10) : undefined;

  return {
    selected: {
      runId,
      taskId,
      mapIndex,
    },
    clearSelection,
    onSelect,
  };
};

export default useSelection;
