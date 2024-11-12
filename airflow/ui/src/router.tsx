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

import { DagsList } from "src/pages/DagsList";
import { Dashboard } from "src/pages/Dashboard";

import { BaseLayout } from "./layouts/BaseLayout";
import { Dag } from "./pages/DagsList/Dag";
import { Code } from "./pages/DagsList/Dag/Code";
import { ErrorPage } from "./pages/Error";
import { Events } from "./pages/Events";

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
            { element: <div>Overview</div>, path: "" },
            { element: <div>Runs</div>, path: "runs" },
            { element: <div>Tasks</div>, path: "tasks" },
            { element: <Events />, path: "events" },
            { element: <Code />, path: "code" },
          ],
          element: <Dag />,
          path: "dags/:dagId",
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
