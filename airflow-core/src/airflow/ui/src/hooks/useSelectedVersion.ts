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

import {
  useDagRunServiceGetDagRun,
  useDagServiceGetDagDetails,
  useStructureServiceStructureData,
  useTaskInstanceServiceGetMappedTaskInstance,
} from "openapi/queries";
import { SearchParamsKeys } from "src/constants/searchParams";

const useSelectedVersion = (): number | undefined => {
  const [searchParams] = useSearchParams();

  const selectedVersionUrlStr = searchParams.get(SearchParamsKeys.VERSION_NUMBER);
  const selectedVersionUrl = selectedVersionUrlStr === null ? undefined : parseInt(selectedVersionUrlStr, 10);

  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();

  const { data: dagData } = useDagServiceGetDagDetails(
    {
      dagId,
    },
    undefined,
    { enabled: Boolean(dagId) },
  );

  const { data: runData } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId,
    },
    undefined,
    { enabled: Boolean(dagId) && Boolean(runId) },
  );

  // Use the structure data to know if a task is mapped or not.
  // Also handling the case for tasks inside mapped task group. Only the structure endpoint
  // propagate down and mark as "mapped" child tasks of a mapped task group.
  const { data: structureData } = useStructureServiceStructureData(
    {
      dagId,
      versionNumber: selectedVersionUrl,
    },
    undefined,
    { enabled: runData !== undefined },
  );

  const taskNode = structureData?.nodes.find((node) => node.id === taskId);
  const isMapped = taskNode?.is_mapped;
  const parsedMapIndex = parseInt(mapIndex, 10);

  const { data: mappedTaskInstanceData } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parsedMapIndex,
      taskId,
    },
    undefined,
    // Do not enable on a task instance summary. (mapped task but no mapIndex defined)
    {
      enabled:
        taskNode !== undefined && !Boolean(parsedMapIndex === -1 && isMapped) && !isNaN(parsedMapIndex),
    },
  );

  const selectedVersionNumber =
    selectedVersionUrl ??
    mappedTaskInstanceData?.dag_version?.version_number ??
    (runData?.dag_versions ?? []).at(-1)?.version_number ??
    dagData?.latest_dag_version?.version_number;

  return selectedVersionNumber;
};

export default useSelectedVersion;
