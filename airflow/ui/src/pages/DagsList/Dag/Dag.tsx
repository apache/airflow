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
import { Link as RouterLink, useParams } from "react-router-dom";

import {
  useDagServiceGetDagDetails,
  useDagsServiceRecentDagRuns,
} from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar } from "src/components/ui";

import { Header } from "./Header";

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
  } = useDagsServiceRecentDagRuns({ dagIdPattern: dagId ?? "" });

  const runs =
    runsData?.dags.find((dagWithRuns) => dagWithRuns.dag_id === dagId)
      ?.latest_dag_runs ?? [];

  return (
    <Box>
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
      <Tabs.Root>
        <Tabs.List>
          <Tabs.Trigger value="overview">Overview</Tabs.Trigger>
          <Tabs.Trigger value="runs">Runs</Tabs.Trigger>
          <Tabs.Trigger value="tasks">Tasks</Tabs.Trigger>
          <Tabs.Trigger value="events">Events</Tabs.Trigger>
        </Tabs.List>

        <Tabs.Content value="overview">
          <p>one!</p>
        </Tabs.Content>
        <Tabs.Content value="runs">
          <p>two!</p>
        </Tabs.Content>
        <Tabs.Content value="tasks">
          <p>three!</p>
        </Tabs.Content>
        <Tabs.Content value="events">
          <p>four!</p>
        </Tabs.Content>
      </Tabs.Root>
    </Box>
  );
};
