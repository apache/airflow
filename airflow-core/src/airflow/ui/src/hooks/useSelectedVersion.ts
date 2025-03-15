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
  useTaskInstanceServiceGetMappedTaskInstance,
  useTaskServiceGetTask,
} from "openapi/queries";
import { SearchParamsKeys } from "src/constants/searchParams";

const useSelectedVersion = (): number | undefined => {
  const [searchParams] = useSearchParams();

  const selectedVersionUrl = searchParams.get(SearchParamsKeys.VERSION_NUMBER);

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

  const { data: taskData } = useTaskServiceGetTask(
    {
      dagId,
      taskId,
    },
    undefined,
    { enabled: Boolean(dagId) && Boolean(runId) && Boolean(taskId) },
  );

  const { data: mappedTaskInstanceData } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parseInt(mapIndex, 10),
      taskId,
    },
    undefined,
    // Do not enable on a task instance summary. Mapped task but no mapIndex defined.
    { enabled: taskData !== undefined && !Boolean(mapIndex === "-1" && taskData.is_mapped) },
  );

  const selectedVersionNumber =
    (selectedVersionUrl === null ? undefined : parseInt(selectedVersionUrl, 10)) ??
    mappedTaskInstanceData?.dag_version?.version_number ??
    (runData?.dag_versions ?? []).at(-1)?.version_number ??
    dagData?.latest_dag_version?.version_number;

  return selectedVersionNumber;
};

export default useSelectedVersion;
