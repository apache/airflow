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
import type { ComponentType } from "react";

import { queryOptions } from "@tanstack/react-query";
import { createBrowserRouter } from "react-router-dom";

import { UseConfigServiceGetConfigsKeyFn } from "openapi/queries";
import { ConfigService } from "openapi/requests/services.gen";

import { BaseLayout } from "src/layouts/BaseLayout";

import { ErrorPage } from "src/pages/Error";

import { TabEntity, TabName, TaskInstanceTab } from "src/constants/tab";

import { client } from "./queryClient";

const ROUTE_MODULE_RELOAD_KEY = "airflow-route-module-reload-attempted";

export const loadRouteModule = async <Module,>(factory: () => Promise<Module>) => {
  try {
    return await factory();
  } catch (error) {
    if (globalThis.sessionStorage.getItem(ROUTE_MODULE_RELOAD_KEY) === null) {
      globalThis.sessionStorage.setItem(ROUTE_MODULE_RELOAD_KEY, "true");
      globalThis.location.reload();
    }

    throw error;
  }
};

const lazyRoute =
  <Module,>(factory: () => Promise<Module>, name: keyof Module) =>
  async () => {
    const module = await loadRouteModule(factory);

    return { Component: module[name] as ComponentType };
  };

const loadAsset = lazyRoute(() => import("src/pages/Asset"), "Asset");
const loadAssetEvents = lazyRoute(() => import("src/pages/Asset/AssetEvents"), "AssetEvents");
const loadAssetStateStore = lazyRoute(() => import("src/pages/Asset/AssetStateStore"), "AssetStateStore");
const loadAssetsList = lazyRoute(() => import("src/pages/AssetsList"), "AssetsList");
const loadConfigs = lazyRoute(() => import("src/pages/Configs"), "Configs");
const loadConnections = lazyRoute(() => import("src/pages/Connections"), "Connections");
const loadDag = lazyRoute(() => import("src/pages/Dag"), "Dag");
const loadBackfills = lazyRoute(() => import("src/pages/Dag/Backfills"), "Backfills");
const loadCalendar = lazyRoute(() => import("src/pages/Dag/Calendar/Calendar"), "Calendar");
const loadCode = lazyRoute(() => import("src/pages/Dag/Code"), "Code");
const loadDagDetails = lazyRoute(() => import("src/pages/Dag/Details"), "Details");
const loadOverview = lazyRoute(() => import("src/pages/Dag/Overview"), "Overview");
const loadTasks = lazyRoute(() => import("src/pages/Dag/Tasks"), "Tasks");
const loadVersions = lazyRoute(() => import("src/pages/Dag/Versions"), "Versions");
const loadDagBundle = lazyRoute(() => import("src/pages/DagBundle"), "DagBundle");
const loadDagBundles = lazyRoute(() => import("src/pages/DagBundles"), "DagBundles");
const loadDagRuns = lazyRoute(() => import("src/pages/DagRuns"), "DagRuns");
const loadDagsList = lazyRoute(() => import("src/pages/DagsList"), "DagsList");
const loadDashboard = lazyRoute(() => import("src/pages/Dashboard"), "Dashboard");
const loadDeadlines = lazyRoute(() => import("src/pages/Deadlines"), "Deadlines");
const loadEvents = lazyRoute(() => import("src/pages/Events"), "Events");
const loadExternalView = lazyRoute(() => import("src/pages/ExternalView"), "ExternalView");
const loadGroupTaskInstance = lazyRoute(() => import("src/pages/GroupTaskInstance"), "GroupTaskInstance");
const loadGroupTaskInstances = lazyRoute(() => import("src/pages/GroupTaskInstance"), "GroupTaskInstances");
const loadHITLTaskInstances = lazyRoute(() => import("src/pages/HITLTaskInstances"), "HITLTaskInstances");
const loadJobs = lazyRoute(() => import("src/pages/Jobs"), "Jobs");
const loadLandingPage = lazyRoute(() => import("src/pages/LandingPage"), "LandingPage");
const loadMappedTaskInstance = lazyRoute(() => import("src/pages/MappedTaskInstance"), "MappedTaskInstance");
const loadMappedTaskInstanceDetails = lazyRoute(
  () => import("src/pages/MappedTaskInstance/Details"),
  "Details",
);
const loadPlugins = lazyRoute(() => import("src/pages/Plugins"), "Plugins");
const loadPools = lazyRoute(() => import("src/pages/Pools"), "Pools");
const loadProviders = lazyRoute(() => import("src/pages/Providers"), "Providers");
const loadRun = lazyRoute(() => import("src/pages/Run"), "Run");
const loadDagRunAssetEvents = lazyRoute(() => import("src/pages/Run/AssetEvents"), "AssetEvents");
const loadDagRunCallbacks = lazyRoute(() => import("src/pages/Run/Callbacks"), "Callbacks");
const loadDagRunDetails = lazyRoute(() => import("src/pages/Run/Details"), "Details");
const loadSecurity = lazyRoute(() => import("src/pages/Security"), "Security");
const loadSettings = lazyRoute(() => import("src/pages/Settings"), "Settings");
const loadTask = lazyRoute(() => import("src/pages/Task"), "Task");
const loadTaskOverview = lazyRoute(() => import("src/pages/Task/Overview"), "Overview");
const loadTaskInstance = lazyRoute(() => import("src/pages/TaskInstance"), "TaskInstance");
const loadLogs = lazyRoute(() => import("src/pages/TaskInstance"), "Logs");
const loadTaskInstanceAssetEvents = lazyRoute(
  () => import("src/pages/TaskInstance/AssetEvents"),
  "AssetEvents",
);
const loadTaskInstanceDefaultTab = lazyRoute(() => import("src/pages/TaskInstance/DefaultTab"), "DefaultTab");
const loadTaskInstanceDetails = lazyRoute(() => import("src/pages/TaskInstance/Details"), "Details");
const loadHITLResponse = lazyRoute(() => import("src/pages/TaskInstance/HITLResponse"), "HITLResponse");
const loadRenderedTemplates = lazyRoute(
  () => import("src/pages/TaskInstance/RenderedTemplates"),
  "RenderedTemplates",
);
const loadTaskInstances = lazyRoute(() => import("src/pages/TaskInstances"), "TaskInstances");
const loadTaskStateStore = lazyRoute(() => import("src/pages/TaskStateStore"), "TaskStateStore");
const loadVariables = lazyRoute(() => import("src/pages/Variables"), "Variables");
const loadXCom = lazyRoute(() => import("src/pages/XCom"), "XCom");

const loadDagRunsWithLayout = () =>
  loadRouteModule(async () => {
    const [{ DagsLayout }, { DagRuns: DagRunsPage }] = await Promise.all([
      import("src/layouts/DagsLayout"),
      import("src/pages/DagRuns"),
    ]);

    return {
      element: (
        <DagsLayout>
          <DagRunsPage />
        </DagsLayout>
      ),
    };
  });

const loadTaskInstancesWithLayout = () =>
  loadRouteModule(async () => {
    const [{ DagsLayout }, { TaskInstances: TaskInstancesPage }] = await Promise.all([
      import("src/layouts/DagsLayout"),
      import("src/pages/TaskInstances"),
    ]);

    return {
      element: (
        <DagsLayout>
          <TaskInstancesPage />
        </DagsLayout>
      ),
    };
  });

const loadHITLTaskInstancesWithDrawer = () =>
  loadRouteModule(async () => {
    const { HITLTaskInstances: HITLTaskInstancesPage } = await import("src/pages/HITLTaskInstances");

    return { element: <HITLTaskInstancesPage enableHITLReviewDrawer /> };
  });

const pluginRoute = {
  lazy: loadExternalView,
  path: "plugin/:page/*",
};

export const taskInstanceRoutes = [
  { index: true, lazy: loadTaskInstanceDefaultTab, path: undefined },
  { lazy: loadLogs, path: TaskInstanceTab.Logs },
  { lazy: loadEvents, path: TaskInstanceTab.Events },
  { lazy: loadXCom, path: TaskInstanceTab.XCom },
  { lazy: loadTaskStateStore, path: TaskInstanceTab.TaskStateStore },
  { lazy: loadCode, path: TaskInstanceTab.Code },
  { lazy: loadTaskInstanceDetails, path: TaskInstanceTab.Details },
  { lazy: loadRenderedTemplates, path: TaskInstanceTab.RenderedTemplates },
  { lazy: loadTaskInstances, path: TaskInstanceTab.TaskInstances },
  { lazy: loadTaskInstanceAssetEvents, path: TaskInstanceTab.AssetEvents },
  { lazy: loadHITLResponse, path: TaskInstanceTab.RequiredActions },
  pluginRoute,
];

export const routerConfig = [
  {
    children: [
      {
        index: true,
        lazy: loadLandingPage,
      },
      {
        lazy: loadDashboard,
        path: "home",
      },
      {
        lazy: loadHITLTaskInstancesWithDrawer,
        path: "required_actions",
      },
      {
        lazy: loadDagsList,
        path: "dags",
      },
      {
        lazy: loadDagRunsWithLayout,
        path: "dag_runs",
      },
      {
        lazy: loadTaskInstancesWithLayout,
        path: "task_instances",
      },
      {
        lazy: loadAssetsList,
        path: "assets",
      },
      {
        lazy: loadConfigs,
        path: "configs",
      },
      {
        lazy: loadSettings,
        path: "settings",
      },
      {
        children: [
          { index: true, lazy: loadAssetEvents },
          { lazy: loadAssetStateStore, path: "asset-state-store" },
          pluginRoute,
        ],
        lazy: loadAsset,
        path: "assets/:assetId",
      },
      {
        lazy: loadDagBundles,
        path: "dag_bundles",
      },
      {
        lazy: loadDagBundle,
        path: "dag_bundles/:bundleName",
      },
      {
        lazy: loadDeadlines,
        path: "deadlines",
      },
      {
        lazy: loadEvents,
        path: "events",
      },
      {
        lazy: loadXCom,
        path: "xcoms",
      },
      {
        lazy: loadVariables,
        path: "variables",
      },
      {
        lazy: loadPools,
        path: "pools",
      },
      {
        lazy: loadProviders,
        path: "providers",
      },
      {
        lazy: loadPlugins,
        path: "plugins",
      },
      {
        lazy: loadSecurity,
        path: "security/:page/*",
      },
      {
        lazy: loadConnections,
        path: "connections",
      },
      {
        lazy: loadJobs,
        path: "jobs",
      },
      pluginRoute,
      {
        children: [
          { handle: { entity: TabEntity.Dag, tab: TabName.Overview }, index: true, lazy: loadOverview },
          { handle: { entity: TabEntity.Dag, tab: TabName.Runs }, lazy: loadDagRuns, path: "runs" },
          { handle: { entity: TabEntity.Dag, tab: TabName.Tasks }, lazy: loadTasks, path: "tasks" },
          {
            handle: { entity: TabEntity.Dag, tab: TabName.Calendar },
            lazy: loadCalendar,
            path: "calendar",
          },
          // The Required Actions tab is now a button + modal; this keeps old /required_actions
          // deep links alive by rendering the overview, where the route sync opens the modal.
          { lazy: loadOverview, path: "required_actions" },
          {
            handle: { entity: TabEntity.Dag, tab: TabName.Backfills },
            lazy: loadBackfills,
            path: "backfills",
          },
          { lazy: loadBackfills, path: "backfills/:backfillId" },
          { handle: { entity: TabEntity.Dag, tab: TabName.Events }, lazy: loadEvents, path: "events" },
          { handle: { entity: TabEntity.Dag, tab: TabName.Code }, lazy: loadCode, path: "code" },
          {
            handle: { entity: TabEntity.Dag, tab: TabName.Versions },
            lazy: loadVersions,
            path: "versions",
          },
          {
            handle: { entity: TabEntity.Dag, tab: TabName.Details },
            lazy: loadDagDetails,
            path: "details",
          },
          pluginRoute,
        ],
        lazy: loadDag,
        path: "dags/:dagId",
      },
      {
        children: [
          { index: true, lazy: loadTaskInstances },
          // The Required Actions tab is now a button + modal; this keeps old /required_actions
          // deep links alive by rendering the task instances, where the route sync opens the modal.
          { lazy: loadTaskInstances, path: "required_actions" },
          { lazy: loadEvents, path: "events" },
          { lazy: loadCode, path: "code" },
          { lazy: loadDagRunDetails, path: "details" },
          { lazy: loadDagRunAssetEvents, path: "asset_events" },
          { lazy: loadDagRunCallbacks, path: "callbacks" },
          pluginRoute,
        ],
        lazy: loadRun,
        path: "dags/:dagId/runs/:runId",
      },
      {
        children: taskInstanceRoutes,
        lazy: loadTaskInstance,
        path: "dags/:dagId/runs/:runId/tasks/:taskId",
      },
      {
        children: [
          { index: true, lazy: loadTaskInstances },
          { lazy: loadMappedTaskInstanceDetails, path: "details" },
        ],
        lazy: loadMappedTaskInstance,
        path: "dags/:dagId/runs/:runId/tasks/:taskId/mapped",
      },
      {
        children: [{ index: true, lazy: loadGroupTaskInstances }],
        lazy: loadGroupTaskInstance,
        path: "dags/:dagId/runs/:runId/tasks/group/:groupId",
      },
      {
        children: [
          { index: true, lazy: loadTaskOverview },
          { lazy: loadTaskInstances, path: "task_instances" },
          { lazy: loadHITLTaskInstances, path: "required_actions" },
          pluginRoute,
        ],
        lazy: loadTask,
        path: "dags/:dagId/tasks/group/:groupId",
      },
      {
        children: taskInstanceRoutes,
        lazy: loadTaskInstance,
        path: "dags/:dagId/runs/:runId/tasks/:taskId/mapped/:mapIndex",
      },
      {
        children: [
          { index: true, lazy: loadTaskOverview },
          { lazy: loadTaskInstances, path: "task_instances" },
          { lazy: loadHITLTaskInstances, path: "required_actions" },
          { lazy: loadEvents, path: "events" },
          pluginRoute,
        ],
        lazy: loadTask,
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
