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
import { Box, Button, Tabs } from "@chakra-ui/react";
import { FiChevronsLeft } from "react-icons/fi";
import {
  Outlet,
  Link as RouterLink,
  useLocation,
  useParams,
} from "react-router-dom";

import {
  useDagServiceGetDagDetails,
  useDagsServiceRecentDagRuns,
} from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar } from "src/components/ui";
import { capitalize } from "src/utils";

import { Header } from "./Header";

const tabs = ["runs", "tasks", "events", "code"];

export const Dag = () => {
  const { dagId } = useParams();

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

  const { pathname } = useLocation();

  const runs =
    runsData?.dags.find((dagWithRuns) => dagWithRuns.dag_id === dagId)
      ?.latest_dag_runs ?? [];

  const activeTab =
    tabs.find((tab) => pathname.endsWith(`/${tab}`)) ?? "overview";

  return (
    <>
      <Box bg="bg" position="sticky" top={0} zIndex={1}>
        <Button asChild colorPalette="blue" variant="ghost">
          <RouterLink to="/dags">
            <FiChevronsLeft />
            Back to all dags
          </RouterLink>
        </Button>
        <Header dag={dag} dagId={dagId} latestRun={runs[0]} />
        <ErrorAlert error={error ?? runsError} />
        <ProgressBar
          size="xs"
          visibility={isLoading || isLoadingRuns ? "visible" : "hidden"}
        />
        <Tabs.Root value={activeTab}>
          <Tabs.List>
            <Tabs.Trigger asChild value="overview">
              <RouterLink to={`/dags/${dagId}`}>Overview</RouterLink>
            </Tabs.Trigger>
            {tabs.map((tab) => (
              <Tabs.Trigger asChild key={tab} value={tab}>
                <RouterLink to={`/dags/${dagId}/${tab}`}>
                  {capitalize(tab)}
                </RouterLink>
              </Tabs.Trigger>
            ))}
          </Tabs.List>
        </Tabs.Root>
      </Box>
      <Outlet />
    </>
  );
};
