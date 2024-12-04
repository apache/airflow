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
import { Box } from "@chakra-ui/react";
import { LiaSlashSolid } from "react-icons/lia";
import { useParams, Link as RouterLink, Outlet } from "react-router-dom";

import {
  useDagServiceGetDagDetails,
  useTaskInstanceServiceGetTaskInstance,
} from "openapi/queries";
import { Breadcrumb } from "src/components/ui";
import { OpenGroupsProvider } from "src/context/openGroups";

import { Header } from "./Header";
import { TaskInstanceTabs } from "./Tabs";

export const TaskInstance = () => {
  const { dagId = "", runId = "", taskId = "" } = useParams();

  const { data: dag } = useDagServiceGetDagDetails({
    dagId,
  });

  const { data: taskInstance } = useTaskInstanceServiceGetTaskInstance({
    dagId,
    dagRunId: runId,
    taskId,
  });

  return (
    <OpenGroupsProvider dagId={dagId}>
      <Breadcrumb.Root mb={3} separator={<LiaSlashSolid />}>
        <Breadcrumb.Link asChild color="fg.info">
          <RouterLink to="/dags">Dags</RouterLink>
        </Breadcrumb.Link>
        <Breadcrumb.Link asChild color="fg.info">
          <RouterLink to={`/dags/${dagId}`}>{dagId}</RouterLink>
        </Breadcrumb.Link>
        <Breadcrumb.Link asChild color="fg.info">
          <RouterLink to={`/dags/${dagId}/runs/${runId}`}>{runId}</RouterLink>
        </Breadcrumb.Link>
        <Breadcrumb.CurrentLink>{taskId}</Breadcrumb.CurrentLink>
      </Breadcrumb.Root>
      {taskInstance === undefined ? undefined : (
        <Header taskInstance={taskInstance} />
      )}
      <TaskInstanceTabs dag={dag} />
      <Box overflow="auto">
        <Outlet />
      </Box>
    </OpenGroupsProvider>
  );
};
