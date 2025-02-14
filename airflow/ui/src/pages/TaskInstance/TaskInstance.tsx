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
import { LiaSlashSolid } from "react-icons/lia";
import { useParams, Link as RouterLink, useSearchParams } from "react-router-dom";

import { useDagServiceGetDagDetails, useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { Breadcrumb } from "src/components/ui";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";
import { isStatePending, useAutoRefresh } from "src/utils";

import { Header } from "./Header";

const tabs = [
  { label: "Logs", value: "" },
  { label: "Events", value: "events" },
  { label: "XCom", value: "xcom" },
  { label: "Code", value: "code" },
  { label: "Details", value: "details" },
  { label: "Rendered Templates", value: "rendered_templates" },
];

export const TaskInstance = () => {
  const { dagId = "", runId = "", taskId = "" } = useParams();
  const [searchParams] = useSearchParams();

  const mapIndexParam = searchParams.get("map_index");
  const mapIndex = parseInt(mapIndexParam ?? "-1", 10);

  const refetchInterval = useAutoRefresh({ dagId });

  const {
    data: dag,
    error: dagError,
    isLoading: isDagLoading,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  const {
    data: taskInstance,
    error,
    isLoading,
  } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId,
      mapIndex,
      taskId,
    },
    undefined,
    {
      refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false),
    },
  );

  const links = [
    { label: "Dags", value: "/dags" },
    { label: dag?.dag_display_name ?? dagId, value: `/dags/${dagId}` },
    { label: runId, value: `/dags/${dagId}/runs/${runId}` },
    { label: taskInstance?.task_display_name ?? taskId },
  ];

  if (mapIndexParam !== null) {
    links.push({ label: taskInstance?.rendered_map_index ?? mapIndexParam });
  }

  return (
    <DetailsLayout dag={dag} error={error ?? dagError} isLoading={isLoading || isDagLoading} tabs={tabs}>
      <Breadcrumb.Root mb={3} separator={<LiaSlashSolid />}>
        {links.map((link, index) => {
          if (index === links.length - 1) {
            return <Breadcrumb.CurrentLink key={link.label}>{link.label}</Breadcrumb.CurrentLink>;
          }

          return link.value === undefined ? (
            <Breadcrumb.Link color="fg.info" key={link.label}>
              {link.label}
            </Breadcrumb.Link>
          ) : (
            <Breadcrumb.Link asChild color="fg.info" key={link.label}>
              <RouterLink to={link.value}>{link.label}</RouterLink>
            </Breadcrumb.Link>
          );
        })}
      </Breadcrumb.Root>
      {taskInstance === undefined ? undefined : (
        <Header
          isRefreshing={Boolean(isStatePending(taskInstance.state) && Boolean(refetchInterval))}
          taskInstance={taskInstance}
        />
      )}
    </DetailsLayout>
  );
};
