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
import { queryOptions } from "@tanstack/react-query";
import { createBrowserRouter } from "react-router-dom";

import { UseConfigServiceGetConfigsKeyFn } from "openapi/queries";
import { ConfigService } from "openapi/requests/services.gen";
import { BaseLayout } from "src/layouts/BaseLayout";
import { DagsLayout } from "src/layouts/DagsLayout";
import { AssetsList } from "src/pages/AssetsList";
import { Dag } from "src/pages/Dag";
import { Code } from "src/pages/Dag/Code";
import { Overview } from "src/pages/Dag/Overview";
import { Tasks } from "src/pages/Dag/Tasks";
import { DagRuns } from "src/pages/DagRuns";
import { DagsList } from "src/pages/DagsList";
import { Dashboard } from "src/pages/Dashboard";
import { ErrorPage } from "src/pages/Error";
import { Events } from "src/pages/Events";
import { Plugins } from "src/pages/Plugins";
import { Pools } from "src/pages/Pools";
import { Providers } from "src/pages/Providers";
import { Run } from "src/pages/Run";
import { Details as DagRunDetails } from "src/pages/Run/Details";
import { Task } from "src/pages/Task";
import { Overview as TaskOverview } from "src/pages/Task/Overview";
import { TaskInstance, Logs } from "src/pages/TaskInstance";
import { Details } from "src/pages/TaskInstance/Details";
import { RenderedTemplates } from "src/pages/TaskInstance/RenderedTemplates";
import { TaskInstances } from "src/pages/TaskInstances";
import { Variables } from "src/pages/Variables";
import { XCom } from "src/pages/XCom";

import { queryClient } from "./queryClient";

export const routerConfig = [
  {
    children: [
      {
        element: <Dashboard />,
        index: true,
      },
      {
        element: <DagsList />,
        path: "dags",
      },
      {
        element: (
          <DagsLayout>
            <DagRuns />
          </DagsLayout>
        ),
        path: "dag_runs",
      },
      {
        element: (
          <DagsLayout>
            <TaskInstances />
          </DagsLayout>
        ),
        path: "task_instances",
      },
      {
        element: <AssetsList />,
        path: "assets",
      },
      {
        element: <Events />,
        path: "events",
      },
      {
        element: <XCom />,
        path: "xcoms",
      },
      {
        element: <Variables />,
        path: "variables",
      },
      {
        element: <Pools />,
        path: "pools",
      },
      {
        element: <Providers />,
        path: "providers",
      },
      {
        element: <Plugins />,
        path: "plugins",
      },
      {
        children: [
          { element: <Overview />, index: true },
          { element: <DagRuns />, path: "runs" },
          { element: <Tasks />, path: "tasks" },
          { element: <Events />, path: "events" },
          { element: <Code />, path: "code" },
        ],
        element: <Dag />,
        path: "dags/:dagId",
      },
      {
        children: [
          { element: <TaskInstances />, index: true },
          { element: <Events />, path: "events" },
          { element: <Code />, path: "code" },
          { element: <DagRunDetails />, path: "details" },
        ],
        element: <Run />,
        path: "dags/:dagId/runs/:runId",
      },
      {
        children: [
          { element: <Logs />, index: true },
          { element: <Events />, path: "events" },
          { element: <XCom />, path: "xcom" },
          { element: <Code />, path: "code" },
          { element: <Details />, path: "details" },
          { element: <RenderedTemplates />, path: "rendered_templates" },
        ],
        element: <TaskInstance />,
        path: "dags/:dagId/runs/:runId/tasks/:taskId",
      },
      {
        children: [
          { element: <TaskOverview />, index: true },
          { element: <TaskInstances />, path: "task_instances" },
          { element: <Events />, path: "events" },
        ],
        element: <Task />,
        path: "dags/:dagId/tasks/:taskId",
      },
    ],
    element: <BaseLayout />,
    errorElement: (
      <BaseLayout>
        <ErrorPage />
      </BaseLayout>
    ),
    // Use react router loader to ensure we have the config before any other requests are made
    loader: async () => {
      const data = await queryClient.ensureQueryData(
        queryOptions({
          queryFn: ConfigService.getConfigs,
          queryKey: UseConfigServiceGetConfigsKeyFn(),
        }),
      );

      return data;
    },
    path: "/",
  },
];

const baseUrl = document.querySelector("base")?.href ?? "http://localhost:9091/";
const basename = new URL(baseUrl).pathname;

export const router = createBrowserRouter(routerConfig, { basename });
