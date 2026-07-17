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
import { Asset } from "src/pages/Asset";
import { AssetEvents } from "src/pages/Asset/AssetEvents";
import { AssetStateStore } from "src/pages/Asset/AssetStateStore";
import { AssetsList } from "src/pages/AssetsList";
import { Configs } from "src/pages/Configs";
import { Connections } from "src/pages/Connections";
import { Dag } from "src/pages/Dag";
import { Backfills } from "src/pages/Dag/Backfills";
import { Calendar } from "src/pages/Dag/Calendar/Calendar";
import { Code } from "src/pages/Dag/Code";
import { Details as DagDetails } from "src/pages/Dag/Details";
import { Overview } from "src/pages/Dag/Overview";
import { Tasks } from "src/pages/Dag/Tasks";
import { DagRuns } from "src/pages/DagRuns";
import { DagsList } from "src/pages/DagsList";
import { Dashboard } from "src/pages/Dashboard";
import { Deadlines } from "src/pages/Deadlines";
import { ErrorPage } from "src/pages/Error";
import { Events } from "src/pages/Events";
import { ExternalView } from "src/pages/ExternalView";
import { GroupTaskInstance } from "src/pages/GroupTaskInstance";
import { HITLTaskInstances } from "src/pages/HITLTaskInstances";
import { Jobs } from "src/pages/Jobs";
import { MappedTaskInstance } from "src/pages/MappedTaskInstance";
import { Details as MappedTaskInstanceDetails } from "src/pages/MappedTaskInstance/Details";
import { Plugins } from "src/pages/Plugins";
import { Pools } from "src/pages/Pools";
import { Providers } from "src/pages/Providers";
import { Run } from "src/pages/Run";
import { AssetEvents as DagRunAssetEvents } from "src/pages/Run/AssetEvents";
import { Details as DagRunDetails } from "src/pages/Run/Details";
import { Security } from "src/pages/Security";
import { Task } from "src/pages/Task";
import { Overview as TaskOverview } from "src/pages/Task/Overview";
import { TaskInstance, Logs } from "src/pages/TaskInstance";
import { AssetEvents as TaskInstanceAssetEvents } from "src/pages/TaskInstance/AssetEvents";
import { Details as TaskInstanceDetails } from "src/pages/TaskInstance/Details";
import { HITLResponse } from "src/pages/TaskInstance/HITLResponse";
import { RenderedTemplates } from "src/pages/TaskInstance/RenderedTemplates";
import { TaskInstances } from "src/pages/TaskInstances";
import { TaskStateStore } from "src/pages/TaskStateStore";
import { Variables } from "src/pages/Variables";
import { XCom } from "src/pages/XCom";

import { StorageLayout } from "./layouts/StorageLayout";
import { client } from "./queryClient";

const pluginRoute = {
  element: <ExternalView />,
  path: "plugin/:page/*",
};

export const taskInstanceRoutes = [
  { element: <Logs />, index: true, path: undefined },
  { element: <Events />, path: "events" },
  {
    children: [
      { element: <TaskStateStore />, path: "task-state-store" },
      { element: <XCom />, path: "xcom" },
    ],
    element: <StorageLayout />,
  },
  { element: <Code />, path: "code" },
  { element: <TaskInstanceDetails />, path: "details" },
  { element: <RenderedTemplates />, path: "rendered_templates" },
  { element: <TaskInstances />, path: "task_instances" },
  { element: <TaskInstanceAssetEvents />, path: "asset_events" },
  { element: <HITLResponse />, path: "required_actions" },
  pluginRoute,
];

export const routerConfig = [
  {
    children: [
      {
        element: <Dashboard />,
        index: true,
      },
      {
        element: <HITLTaskInstances enableHITLReviewDrawer />,
        path: "required_actions",
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
        element: <Configs />,
        path: "configs",
      },
      {
        children: [
          { element: <AssetEvents />, index: true },
          { element: <AssetStateStore />, path: "asset-state-store" },
        ],
        element: <Asset />,
        path: "assets/:assetId",
      },
      {
        element: <Deadlines />,
        path: "deadlines",
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
        element: <Security />,
        path: "security/:page",
      },
      {
        element: <Connections />,
        path: "connections",
      },
      {
        element: <Jobs />,
        path: "jobs",
      },
      pluginRoute,
      {
        children: [
          { element: <Overview />, index: true },
          { element: <DagRuns />, path: "runs" },
          { element: <Tasks />, path: "tasks" },
          { element: <Calendar />, path: "calendar" },
          // The Required Actions tab is now a button + modal; this keeps old /required_actions
          // deep links alive by rendering the overview, where the route sync opens the modal.
          { element: <Overview />, path: "required_actions" },
          { element: <Backfills />, path: "backfills" },
          { element: <Events />, path: "events" },
          { element: <Code />, path: "code" },
          { element: <DagDetails />, path: "details" },
          pluginRoute,
        ],
        element: <Dag />,
        path: "dags/:dagId",
      },
      {
        children: [
          { element: <TaskInstances />, index: true },
          // The Required Actions tab is now a button + modal; this keeps old /required_actions
          // deep links alive by rendering the task instances, where the route sync opens the modal.
          { element: <TaskInstances />, path: "required_actions" },
          { element: <Events />, path: "events" },
          { element: <Code />, path: "code" },
          { element: <DagRunDetails />, path: "details" },
          { element: <DagRunAssetEvents />, path: "asset_events" },
          pluginRoute,
        ],
        element: <Run />,
        path: "dags/:dagId/runs/:runId",
      },
      {
        children: taskInstanceRoutes,
        element: <TaskInstance />,
        path: "dags/:dagId/runs/:runId/tasks/:taskId",
      },
      {
        children: [
          { element: <TaskInstances />, index: true },
          { element: <MappedTaskInstanceDetails />, path: "details" },
        ],
        element: <MappedTaskInstance />,
        path: "dags/:dagId/runs/:runId/tasks/:taskId/mapped",
      },
      {
        children: [{ element: <TaskInstances />, index: true }],
        element: <GroupTaskInstance />,
        path: "dags/:dagId/runs/:runId/tasks/group/:groupId",
      },
      {
        children: [
          { element: <TaskOverview />, index: true },
          { element: <TaskInstances />, path: "task_instances" },
          { element: <HITLTaskInstances />, path: "required_actions" },
          pluginRoute,
        ],
        element: <Task />,
        path: "dags/:dagId/tasks/group/:groupId",
      },
      {
        children: taskInstanceRoutes,
        element: <TaskInstance />,
        path: "dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex",
      },
      {
        children: [
          { element: <TaskOverview />, index: true },
          { element: <TaskInstances />, path: "task_instances" },
          { element: <HITLTaskInstances />, path: "required_actions" },
          { element: <Events />, path: "events" },
          pluginRoute,
        ],
        element: <Task />,
        path: "dags/:dagId/tasks/:taskId",
      },
      {
        element: <ErrorPage />,
        path: "*",
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
      const data = await client.ensureQueryData(
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

const baseUrl = document.querySelector("base")?.href ?? "http://localhost:8080/";
const basename = new URL(baseUrl).pathname;

export const router = createBrowserRouter(routerConfig, { basename });
