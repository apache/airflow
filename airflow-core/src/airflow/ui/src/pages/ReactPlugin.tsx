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

import type { ReactAppResponse } from "openapi/requests/types.gen";

import { ErrorPage } from "./Error";

type PluginComponentType = FC<{
  dagId?: string;
  mapIndex?: string;
  runId?: string;
  taskId?: string;
}>;

export const ReactPlugin = ({ reactApp }: { readonly reactApp: ReactAppResponse }) => {
  const { dagId, mapIndex, runId, taskId } = useParams();

  // If the plugin component was already registered on the global object by a previous load,
  // render it directly without going through Suspense/lazy (avoids flashing the spinner).
  const existing = (globalThis as Record<string, unknown>)[reactApp.name];

  if (typeof existing === "function") {
    const Plugin = existing as PluginComponentType;

    return <Plugin dagId={dagId} mapIndex={mapIndex} runId={runId} taskId={taskId} />;
  }

  // Otherwise, lazy-load the bundle once. When it resolves, it must set a function component
  // under globalThis[reactApp.name], which we then use as the default export.
  const LazyPlugin = lazy(() =>
    import(/* @vite-ignore */ reactApp.bundle_url)
      .then(() => {
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
        console.error("Component Failed Loading:", error);

        return { default: ErrorPage };
      }),
  );

  return (
    <Suspense fallback={<Spinner />}>
      <LazyPlugin dagId={dagId} mapIndex={mapIndex} runId={runId} taskId={taskId} />
    </Suspense>
  );
};
