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
import { lazy, Suspense } from "react";
import { useParams } from "react-router-dom";

import type { ReactAppResponse } from "openapi/requests/types.gen";

import { ErrorPage } from "./Error";

export const ReactPlugin = ({ reactApp }: { readonly reactApp: ReactAppResponse }) => {
  const { dagId, mapIndex, runId, taskId } = useParams();

  const Plugin = lazy(() =>
    // We are assuming the plugin manager is trusted and the bundle_url is safe
    import(/* @vite-ignore */ reactApp.bundle_url).catch((error: unknown) => {
      console.error("Component Failed Loading:", error);

      return {
        default: <ErrorPage />,
      };
    }),
  );

  return (
    <Suspense fallback={<Spinner />}>
      <Plugin dagId={dagId} mapIndex={mapIndex} runId={runId} taskId={taskId} />
    </Suspense>
  );
};
