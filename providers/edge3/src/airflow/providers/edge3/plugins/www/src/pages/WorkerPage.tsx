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
import { useUiServiceWorker } from "openapi/queries";

import { ErrorAlert } from "src/components/ErrorAlert";
import { autoRefreshInterval } from "src/utils";

export const WorkerPage = () => {
  const { data, error } = useUiServiceWorker(undefined, {
    enabled: true,
    refetchInterval: autoRefreshInterval,
  });

  // TODO to make it proper
  // Beautification of state like in Airflow 2
  // Use DataTable as component from Airflow-Core UI
  // Add actions for maintenance / delete of orphan worker
  // Add sorting
  // Add filtering
  // Add links to see jobs on worker
  // Translation
  if (data)
    return (
      <Box p={2}>
        <Table.Root size="sm" interactive stickyHeader striped>
          <Table.Header>
            <Table.Row>
              <Table.ColumnHeader>Worker Name</Table.ColumnHeader>
              <Table.ColumnHeader>State</Table.ColumnHeader>
              <Table.ColumnHeader>Queues</Table.ColumnHeader>
              <Table.ColumnHeader>First Online</Table.ColumnHeader>
              <Table.ColumnHeader>Last Heartbeat</Table.ColumnHeader>
              <Table.ColumnHeader>Active Jobs</Table.ColumnHeader>
              <Table.ColumnHeader>System Information</Table.ColumnHeader>
              <Table.ColumnHeader>Operations</Table.ColumnHeader>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {data.workers.map((worker) => (
              <Table.Row key={worker.worker_name}>
                <Table.Cell>{worker.worker_name}</Table.Cell>
                <Table.Cell>{worker.state}</Table.Cell>
                <Table.Cell>{worker.queues}</Table.Cell>
                <Table.Cell>{worker.first_online}</Table.Cell>
                <Table.Cell>{worker.last_heartbeat}</Table.Cell>
                <Table.Cell>{worker.jobs_active}</Table.Cell>
                <Table.Cell>
                  {worker.sysinfo ? (
                    <ul>
                      {Object.entries(worker.sysinfo).map(([key, value]) => (
                        <li key={key}>
                          {key}: {value}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    "N/A"
                  )}
                </Table.Cell>
                <Table.Cell>{worker.maintenance_comments}</Table.Cell>
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
