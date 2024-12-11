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
import { createBrowserRouter } from "react-router-dom";

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
import { TaskInstances } from "src/pages/Run/TaskInstances";
import { TaskInstance } from "src/pages/TaskInstance";

import { Variables } from "./pages/Variables";

export const router = createBrowserRouter(
  [
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
          element: <Variables />,
          path: "variables",
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
          ],
          element: <Run />,
          path: "dags/:dagId/runs/:runId",
        },
        {
          children: [
            { element: <div>Logs</div>, index: true },
            { element: <Events />, path: "events" },
            { element: <div>Xcom</div>, path: "xcom" },
            { element: <Code />, path: "code" },
            { element: <div>Details</div>, path: "details" },
          ],
          element: <TaskInstance />,
          path: "dags/:dagId/runs/:runId/tasks/:taskId",
        },
      ],
      element: <BaseLayout />,
      errorElement: (
        <BaseLayout>
          <ErrorPage />
        </BaseLayout>
      ),
      path: "/",
    },
  ],
  {
    basename: "/webapp",
  },
);
