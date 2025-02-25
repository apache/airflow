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
import { HStack, Stat } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { LiaSlashSolid } from "react-icons/lia";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import {
  useDagRunServiceGetDagRun,
  useDagServiceGetDagDetails,
  useTaskInstanceServiceGetMappedTaskInstance,
  useTaskServiceGetTask,
} from "openapi/queries";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";
import { TogglePause } from "src/components/TogglePause";
import { Breadcrumb } from "src/components/ui";

export const DagBreadcrumb = () => {
  const { dagId = "", runId, taskId } = useParams();

  const [searchParams] = useSearchParams();
  const mapIndexParam = searchParams.get("map_index");
  const mapIndex = parseInt(mapIndexParam ?? "-1", 10);

  const { data: dag } = useDagServiceGetDagDetails({
    dagId,
  });

  const { data: dagRun } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId ?? "",
    },
    undefined,
    {
      enabled: Boolean(runId),
    },
  );

  const { data: task } = useTaskServiceGetTask({ dagId, taskId }, undefined, { enabled: Boolean(taskId) });

  const { data: taskInstance } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId ?? "",
      mapIndex,
      taskId: taskId ?? "",
    },
    undefined,
    {
      enabled: Boolean(runId) && Boolean(taskId),
    },
  );

  const links: Array<{ label: ReactNode | string; title?: string; value?: string }> = [
    { label: "Dags", value: "/dags" },
    {
      label: (
        <HStack>
          <TogglePause
            dagDisplayName={dag?.dag_display_name}
            dagId={dagId}
            isPaused={Boolean(dag?.is_paused)}
            skipConfirm
          />
          {dag?.dag_display_name ?? dagId}
        </HStack>
      ),
      title: "Dag",
      value: `/dags/${dagId}`,
    },
  ];

  // Add dag run breadcrumb
  if (runId !== undefined) {
    links.push({
      label:
        dagRun === undefined ? (
          runId
        ) : (
          <HStack>
            <StateBadge fontSize="xs" state={dagRun.state} />
            <Time datetime={dagRun.run_after} />
          </HStack>
        ),
      title: "Dag Run",
      value: `/dags/${dagId}/runs/${runId}`,
    });
  }

  // Add task breadcrumb
  if (runId !== undefined && taskId !== undefined) {
    links.push({ label: taskInstance?.task_display_name ?? taskId, title: "Task" });
  }

  if (runId === undefined && taskId !== undefined) {
    links.push({ label: "All Runs", title: "Dag Run", value: `/dags/${dagId}/runs/` });
    links.push({ label: task?.task_display_name ?? taskId, title: "Task" });
  }

  if (mapIndexParam !== null) {
    links.push({ label: mapIndexParam, title: "Map Index" });
  }

  return (
    <Breadcrumb.Root mb={1} separator={<LiaSlashSolid />}>
      {links.map((link, index) => (
        // eslint-disable-next-line react/no-array-index-key
        <Stat.Root gap={0} key={`${link.title}-${index}`}>
          <Stat.Label fontSize="xs" fontWeight="bold">
            {link.title}
          </Stat.Label>
          <Stat.ValueText fontSize="sm" fontWeight="normal">
            {index === links.length - 1 ? (
              <Breadcrumb.CurrentLink>{link.label}</Breadcrumb.CurrentLink>
            ) : (
              <Breadcrumb.Link asChild color="fg.info">
                <RouterLink to={link.value ?? ""}>{link.label}</RouterLink>
              </Breadcrumb.Link>
            )}
          </Stat.ValueText>
        </Stat.Root>
      ))}
    </Breadcrumb.Root>
  );
};
