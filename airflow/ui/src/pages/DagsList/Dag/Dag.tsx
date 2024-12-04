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
import { Box, Button } from "@chakra-ui/react";
import { useState } from "react";
import { FiChevronsLeft } from "react-icons/fi";
import { Outlet, Link as RouterLink, useParams } from "react-router-dom";

import {
  useDagServiceGetDagDetails,
  useDagsServiceRecentDagRuns,
} from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar } from "src/components/ui";
import { Toaster } from "src/components/ui/toaster.tsx";
import { OpenGroupsProvider } from "src/context/openGroups";

import { Header } from "./Header";
import { DagTabs } from "./Tabs";

export const Dag = () => {
  const { dagId } = useParams();
  const [isDocsOpen, setIsDocsOpen] = useState(false);

  const {
    data: dag,
    error,
    isLoading,
  } = useDagServiceGetDagDetails({
    dagId: dagId ?? "",
  });

  // TODO: replace with with a list dag runs by dag id request
  const {
    data: runsData,
    error: runsError,
    isLoading: isLoadingRuns,
  } = useDagsServiceRecentDagRuns({ dagIdPattern: dagId ?? "" }, undefined, {
    enabled: Boolean(dagId),
  });

  const runs =
    runsData?.dags.find((dagWithRuns) => dagWithRuns.dag_id === dagId)
      ?.latest_dag_runs ?? [];

  return (
    <OpenGroupsProvider dagId={dagId ?? ""}>
      <Box>
        <Toaster />
        <Button asChild colorPalette="blue" variant="ghost">
          <RouterLink to="/dags">
            <FiChevronsLeft />
            Back to all dags
          </RouterLink>
        </Button>
        <Header
          dag={dag}
          dagId={dagId}
          latestRun={runs[0]}
          setIsDocsOpen={setIsDocsOpen}
        />
        <ErrorAlert error={error ?? runsError} />
        <ProgressBar
          size="xs"
          visibility={isLoading || isLoadingRuns ? "visible" : "hidden"}
        />
        <DagTabs
          dag={dag}
          isDocsOpen={isDocsOpen}
          setIsDocsOpen={setIsDocsOpen}
        />
      </Box>
      <Box overflow="auto">
        <Outlet />
      </Box>
    </OpenGroupsProvider>
  );
};
