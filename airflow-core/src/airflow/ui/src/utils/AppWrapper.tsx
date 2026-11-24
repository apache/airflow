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
import { RouterProvider, createMemoryRouter, matchRoutes, type RouteObject } from "react-router-dom";

import { TimezoneProvider } from "src/context/timezone";
import { routerConfig } from "src/router";
import { BaseWrapper } from "src/utils/Wrapper";

type Props = {
  readonly initialEntries?: Array<string>;
  readonly router?: ReturnType<typeof createAppRouter>;
};

export const preloadAppRoutes = async (initialEntries: Array<string>) => {
  const routeLoaders = new Set<() => Promise<unknown>>();

  for (const entry of initialEntries) {
    for (const { route } of matchRoutes(routerConfig as Array<RouteObject>, entry) ?? []) {
      if (typeof route.lazy === "function") {
        routeLoaders.add(route.lazy);
      }
    }
  }

  await Promise.all([...routeLoaders].map((loadRoute) => loadRoute()));
};

export const createAppRouter = (initialEntries: Array<string>, initialIndex?: number) =>
  createMemoryRouter(
    // Strip the config loader from tests
    routerConfig.map((route) => ({ ...route, loader: undefined })),
    { basename: "/", initialEntries, initialIndex },
  );

const rootEntries = ["/"];

export const AppWrapper = ({ initialEntries = rootEntries, router }: Props) => (
  <BaseWrapper>
    <TimezoneProvider>
      <RouterProvider router={router ?? createAppRouter(initialEntries)} />
    </TimezoneProvider>
  </BaseWrapper>
);
