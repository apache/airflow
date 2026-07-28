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
import { Spinner } from "@chakra-ui/react";
import { type FC, lazy, Suspense } from "react";
import { useParams } from "react-router-dom";

import {
  useAssetServiceGetAsset,
  useDagRunServiceGetDagRun,
  useDagServiceGetDagDetails,
  useTaskInstanceServiceGetMappedTaskInstance,
} from "openapi/queries";
import type {
  AssetResponse,
  DAGDetailsResponse,
  DAGRunResponse,
  ReactAppResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

import { ErrorPage } from "./Error";

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

type PluginComponentType = FC<PluginProps>;

const loadPlugin = (reactApp: ReactAppResponse): Promise<{ default: PluginComponentType }> =>
  // We are assuming the plugin manager is trusted and the bundle_url is safe
  import(/* @vite-ignore */ new URL(reactApp.bundle_url, document.baseURI).href)
    .then(() => {
      // Store components in globalThis[reactApp.name] to avoid conflicts with the shared globalThis.AirflowPlugin
      // global variable.
      let pluginComponent = (globalThis as Record<string, unknown>)[reactApp.name] as
        PluginComponentType | undefined;

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
      // eslint-disable-next-line no-console
      console.error("Component failed to load:", error);

      return { default: ErrorPage };
    });

export const ReactPlugin = ({ reactApp }: { readonly reactApp: ReactAppResponse }) => {
  const { assetId, dagId, mapIndex, runId, taskId } = useParams();

  // Context objects are not part of the route, so resolve them from the query cache. Each query
  // is gated on the route params it needs, so it stays disabled where those params are absent
  // (e.g. base/dashboard mounts) and is a cache hit where the parent details page already fetched.
  const { data: asset } = useAssetServiceGetAsset(
    { assetId: assetId === undefined ? 0 : parseInt(assetId, 10) },
    undefined,
    { enabled: Boolean(assetId) },
  );
  const assetUri = asset?.uri;

  const { data: dag } = useDagServiceGetDagDetails({ dagId: dagId ?? "" }, undefined, {
    enabled: Boolean(dagId),
  });

  const { data: dagRun } = useDagRunServiceGetDagRun(
    { dagId: dagId ?? "", dagRunId: runId ?? "" },
    undefined,
    { enabled: Boolean(dagId) && Boolean(runId) },
  );

  const { data: taskInstance } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId: dagId ?? "",
      dagRunId: runId ?? "",
      mapIndex: mapIndex === undefined ? -1 : parseInt(mapIndex, 10),
      taskId: taskId ?? "",
    },
    undefined,
    { enabled: Boolean(dagId) && Boolean(runId) && Boolean(taskId) },
  );

  const pluginProps: PluginProps = {
    asset,
    assetId,
    assetUri,
    dag,
    dagId,
    dagRun,
    mapIndex,
    runId,
    taskId,
    taskInstance,
  };

  // If the plugin component was already registered on the global object by a previous load,
  // render it directly without going through Suspense/lazy (avoids flashing the spinner).
  const existing = (globalThis as Record<string, unknown>)[reactApp.name];

  if (typeof existing === "function") {
    const Plugin = existing as PluginComponentType;

    return <Plugin {...pluginProps} />;
  }

  // Otherwise, lazy-load the bundle once. When it resolves, it must set a function component
  // under globalThis[reactApp.name], which we then use as the default export.
  const LazyPlugin = lazy(() => loadPlugin(reactApp));

  return (
    <Suspense fallback={<Spinner />}>
      <LazyPlugin {...pluginProps} />
    </Suspense>
  );
};
