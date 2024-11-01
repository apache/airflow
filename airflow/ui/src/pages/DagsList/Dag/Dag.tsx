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
import {
  Box,
  Button,
  Progress,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
} from "@chakra-ui/react";
import { FiChevronsLeft } from "react-icons/fi";
import { Link as RouterLink, useParams } from "react-router-dom";

import {
  useDagServiceGetDagDetails,
  useDagsServiceRecentDagRuns,
} from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";

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
      <Button
        as={RouterLink}
        color="blue.400"
        leftIcon={<FiChevronsLeft />}
        to="/dags"
        variant="link"
      >
        Back to all dags
      </Button>
      <Header dag={dag} dagId={dagId} latestRun={runs[0]} />
      <ErrorAlert error={error ?? runsError} />
      <Progress
        isIndeterminate
        size="xs"
        visibility={isLoading || isLoadingRuns ? "visible" : "hidden"}
      />
      <Tabs>
        <TabList>
          <Tab>Overview</Tab>
          <Tab>Runs</Tab>
          <Tab>Tasks</Tab>
          <Tab>Events</Tab>
        </TabList>

        <TabPanels>
          <TabPanel>
            <p>one!</p>
          </TabPanel>
          <TabPanel>
            <p>two!</p>
          </TabPanel>
          <TabPanel>
            <p>three!</p>
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Box>
  );
};
