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
import { useCallback, useState } from "react";

import { ReactFlowProvider } from "@xyflow/react";
import { useParams } from "react-router-dom";

import { useDagServiceGetDagDetails } from "openapi/queries";
import { ApiError } from "openapi/requests/core/ApiError";

import { DagNotFound } from "src/pages/Dag/DagNotFound";

import { DetailsLayout } from "./DetailsLayout";
import { SetDetailsErrorContext } from "./detailsError";

/**
 * Route element for `dags/:dagId` — the one mount of the Dag detail screen.
 *
 * Every level below it (a run, a task, a task instance) is a child route rendering into the
 * layout's outlet, so clicking through the Grid never rebuilds the Grid.
 */
export const DagDetailsRoute = () => {
  const { dagId = "" } = useParams();
  const { error } = useDagServiceGetDagDetails({ dagId });
  // Collected from whichever level is open: its panel renders below the chip that shows the error.
  const [levelError, setLevelError] = useState<unknown>(undefined);
  const reportLevelError = useCallback((reported: unknown) => setLevelError(() => reported), []);

  // A Dag that isn't there has nothing to show at any of its levels, so the whole screen gives
  // way rather than framing an empty Grid.
  if (error instanceof ApiError && error.status === 404) {
    return <DagNotFound dagId={dagId} />;
  }

  return (
    <ReactFlowProvider>
      <SetDetailsErrorContext.Provider value={reportLevelError}>
        <DetailsLayout error={levelError} />
      </SetDetailsErrorContext.Provider>
    </ReactFlowProvider>
  );
};
