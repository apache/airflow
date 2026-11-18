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

import type {
  AssetResponse,
  DAGDetailsResponse,
  DAGRunResponse,
  ReactAppResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

import { loadRouteModule, reloadAfterFailedRouteModuleLoad } from "src/utils/routeModule";

export type PluginProps = {
  asset?: AssetResponse;
  assetId?: string;
  assetUri?: string;
  dag?: DAGDetailsResponse;
  dagId?: string;
  dagRun?: DAGRunResponse;
  mapIndex?: string;
  runId?: string;
  taskId?: string;
  taskInstance?: TaskInstanceResponse;
};

export type PluginComponentType = ComponentType<PluginProps>;

const loadPluginDependencies = async () => {
  const { registerPluginDependencies } = await import("src/utils/registerPluginDependencies");

  registerPluginDependencies();
};

export const loadPlugin = (
  reactApp: ReactAppResponse,
  importBundle: (url: string) => Promise<unknown> = (url) => import(/* @vite-ignore */ url),
  loadDependencies: () => Promise<void> = loadPluginDependencies,
): Promise<{ default: PluginComponentType }> => {
  (globalThis as Record<string, unknown>).AirflowPlugin = undefined;

  // We are assuming the plugin manager is trusted and the bundle_url is safe
  return loadRouteModule(() =>
    loadDependencies().then(() => importBundle(new URL(reactApp.bundle_url, document.baseURI).href)),
  )
    .then(() => {
      // Store components in globalThis[reactApp.name] to avoid conflicts with the shared globalThis.AirflowPlugin
      // global variable.
      let pluginComponent = (globalThis as Record<string, unknown>)[reactApp.name] as
        | PluginComponentType
        | undefined;

      if (pluginComponent === undefined) {
        pluginComponent = (globalThis as Record<string, unknown>).AirflowPlugin as PluginComponentType;

        (globalThis as Record<string, unknown>)[reactApp.name] = pluginComponent;
      }

      if (typeof pluginComponent !== "function") {
        throw new TypeError(`Expected function, got ${typeof pluginComponent} for plugin ${reactApp.name}`);
      }

      return { default: pluginComponent };
    })
    .catch((error: unknown) => {
      reloadAfterFailedRouteModuleLoad();

      // eslint-disable-next-line no-console
      console.error("Component failed to load:", error);

      // A plugin that will not load is a deployment problem, not this page's. Rendering an error
      // page in its slot would bury whatever the user actually came for, so the slot stays empty
      // and the console carries the reason.
      return { default: () => null };
    });
};
