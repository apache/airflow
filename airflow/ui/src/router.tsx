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
import { DagsList } from "src/pages/DagsList";
import { Dag } from "src/pages/DagsList/Dag";
import { Code } from "src/pages/DagsList/Dag/Code";
import { Overview } from "src/pages/DagsList/Dag/Overview";
import { Runs } from "src/pages/DagsList/Dag/Runs";
import { Tasks } from "src/pages/DagsList/Dag/Tasks";
import { Run } from "src/pages/DagsList/Run";
import { Dashboard } from "src/pages/Dashboard";
import { ErrorPage } from "src/pages/Error";
import { Events } from "src/pages/Events";

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
        { element: <Run />, path: "dags/:dagId/runs/:runId" },
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
