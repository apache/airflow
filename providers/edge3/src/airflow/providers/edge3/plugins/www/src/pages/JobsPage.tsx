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
import { Box, Table } from "@chakra-ui/react";
import { useUiServiceJobs } from "openapi/queries";

import { ErrorAlert } from "src/components/ErrorAlert";
import { autoRefreshInterval } from "src/utils";

export const JobsPage = () => {
  const { data, error } = useUiServiceJobs(undefined, {
    enabled: true,
    refetchInterval: autoRefreshInterval,
  });

  // TODO to make it proper
  // Beautification of state like in Airflow 2
  // Use DataTable as component from Airflow-Core UI
  // Add sorting
  // Add filtering
  // Add links to see job details / jobs list
  // Translation
  if (data)
    return (
      <Box p={2}>
        <Table.Root size="sm" interactive stickyHeader striped>
          <Table.Header>
            <Table.Row>
              <Table.ColumnHeader>Dag ID</Table.ColumnHeader>
              <Table.ColumnHeader>Run ID</Table.ColumnHeader>
              <Table.ColumnHeader>Task ID</Table.ColumnHeader>
              <Table.ColumnHeader>Map Index</Table.ColumnHeader>
              <Table.ColumnHeader>Try Number</Table.ColumnHeader>
              <Table.ColumnHeader>State</Table.ColumnHeader>
              <Table.ColumnHeader>Queue</Table.ColumnHeader>
              <Table.ColumnHeader>Queued DTTM</Table.ColumnHeader>
              <Table.ColumnHeader>Edge Worker</Table.ColumnHeader>
              <Table.ColumnHeader>Last Update</Table.ColumnHeader>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {data.jobs.map((job) => (
              <Table.Row
                key={`${job.dag_id}.${job.run_id}.${job.task_id}.${job.map_index}.${job.try_number}`}
              >
                <Table.Cell>{job.dag_id}</Table.Cell>
                <Table.Cell>{job.run_id}</Table.Cell>
                <Table.Cell>{job.task_id}</Table.Cell>
                <Table.Cell>{job.map_index}</Table.Cell>
                <Table.Cell>{job.try_number}</Table.Cell>
                <Table.Cell>{job.state}</Table.Cell>
                <Table.Cell>{job.queue}</Table.Cell>
                <Table.Cell>{job.queued_dttm}</Table.Cell>
                <Table.Cell>{job.edge_worker}</Table.Cell>
                <Table.Cell>{job.last_update}</Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      </Box>
    );
  if (error) {
    return (
      <Box p={2}>
        <p>Unable to load data:</p>
        <ErrorAlert error={error} />
      </Box>
    );
  }
  return <Box p={2}>Loading...</Box>;
};
