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
import { Dag } from "src/pages/Dag";
import { Code } from "src/pages/Dag/Code";
import { Overview } from "src/pages/Dag/Overview";
import { Runs } from "src/pages/Dag/Runs";
import { Tasks } from "src/pages/Dag/Tasks";
import { DagsList } from "src/pages/DagsList";
import { Dashboard } from "src/pages/Dashboard";
import { ErrorPage } from "src/pages/Error";
import { Events } from "src/pages/Events";
import { Run } from "src/pages/Run";
import { Details as DagRunDetails } from "src/pages/Run/Details";
import { TaskInstances } from "src/pages/Run/TaskInstances";
import { Task, Instances } from "src/pages/Task";
import { TaskInstance, Logs } from "src/pages/TaskInstance";
import { Details } from "src/pages/TaskInstance/Details";
import { XCom } from "src/pages/XCom";

import { Pools } from "./pages/Pools";
import { Variables } from "./pages/Variables";
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
        children: [
          { element: <Overview />, index: true },
          { element: <Runs />, path: "runs" },
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
        ],
        element: <TaskInstance />,
        path: "dags/:dagId/runs/:runId/tasks/:taskId",
      },
      {
        children: [
          { element: <Instances />, index: true },
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

export const router = createBrowserRouter(routerConfig, { basename: "/webapp" });
