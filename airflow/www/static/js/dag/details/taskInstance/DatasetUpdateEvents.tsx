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
import React, { useMemo } from "react";
import { Box, Heading, Text } from "@chakra-ui/react";

import {
  DatasetLink,
  Table,
  TimeCell,
  TriggeredRuns,
} from "src/components/Table";
import { useDatasetEvents } from "src/api";
import type { DagRun as DagRunType } from "src/types";
import { getMetaValue } from "src/utils";

interface Props {
  runId: DagRunType["runId"];
  taskId: string;
}

const dagId = getMetaValue("dag_id") || undefined;

const DatasetUpdateEvents = ({ runId, taskId }: Props) => {
  const {
    data: { datasetEvents = [] },
    isLoading,
  } = useDatasetEvents({
    sourceDagId: dagId,
    sourceRunId: runId,
    sourceTaskId: taskId,
  });

  const columns = useMemo(
    () => [
      {
        Header: "Dataset URI",
        accessor: "datasetUri",
        Cell: DatasetLink,
      },
      {
        Header: "When",
        accessor: "timestamp",
        Cell: TimeCell,
      },
      {
        Header: "Triggered Runs",
        accessor: "createdDagruns",
        Cell: TriggeredRuns,
      },
    ],
    []
  );

  const data = useMemo(() => datasetEvents, [datasetEvents]);

  return (
    <Box mt={3} flexGrow={1}>
      <Heading size="md">Dataset Events</Heading>
      <Text>Dataset updates caused by this task instance</Text>
      <Table data={data} columns={columns} isLoading={isLoading} />
    </Box>
  );
};

export default DatasetUpdateEvents;
