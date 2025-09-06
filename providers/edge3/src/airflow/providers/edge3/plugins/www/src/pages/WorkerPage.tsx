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
import { useState } from "react";

import { ErrorAlert } from "src/components/ErrorAlert";
import { OperationsCell } from "src/components/OperationsCell";
import { WorkerStateBadge } from "src/components/WorkerStateBadge";
import { autoRefreshInterval } from "src/utils";

export const WorkerPage = () => {
  const { data, error, refetch } = useUiServiceWorker(undefined, {
    enabled: true,
    refetchInterval: autoRefreshInterval,
  });
  const [activeMaintenanceForm, setActiveMaintenanceForm] = useState<string | null>(null);

  const requestMaintenance = async (workerName: string, comment: string) => {
    try {
      console.log(`Requesting maintenance for worker: ${workerName}, comment: ${comment}`);

      // Get CSRF token from meta tag (common Airflow pattern)
      const csrfToken =
        document.querySelector('meta[name="csrf-token"]')?.getAttribute("content") ||
        document.querySelector('input[name="csrf_token"]')?.getAttribute("value");

      const headers: Record<string, string> = {
        "Content-Type": "application/json",
      };

      // Add CSRF token if available
      if (csrfToken) {
        headers["X-CSRFToken"] = csrfToken;
      }

      const response = await fetch(`/edge_worker/ui/worker/${workerName}/maintenance`, {
        body: JSON.stringify({ maintenance_comment: comment }),
        credentials: "same-origin",
        headers,
        method: "POST",
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error("Maintenance request failed:", response.status, errorText);
        throw new Error(`Failed to request maintenance: ${response.status} ${errorText}`);
      }

      console.log("Maintenance request successful");
      setActiveMaintenanceForm(null);
      refetch();
    } catch (error) {
      console.error("Error requesting maintenance:", error);
      alert(`Error requesting maintenance: ${error}`);
    }
  };

  const exitMaintenance = async (workerName: string) => {
    try {
      console.log(`Exiting maintenance for worker: ${workerName}`);

      // Get CSRF token from meta tag (common Airflow pattern)
      const csrfToken =
        document.querySelector('meta[name="csrf-token"]')?.getAttribute("content") ||
        document.querySelector('input[name="csrf_token"]')?.getAttribute("value");

      const headers: Record<string, string> = {};

      // Add CSRF token if available
      if (csrfToken) {
        headers["X-CSRFToken"] = csrfToken;
      }

      const response = await fetch(`/edge_worker/ui/worker/${workerName}/maintenance`, {
        credentials: "same-origin",
        headers,
        method: "DELETE",
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error("Exit maintenance failed:", response.status, errorText);
        throw new Error(`Failed to exit maintenance: ${response.status} ${errorText}`);
      }

      console.log("Exit maintenance successful");
      refetch();
    } catch (error) {
      console.error("Error exiting maintenance:", error);
      alert(`Error exiting maintenance: ${error}`);
    }
  };

  // TODO to make it proper
  // Use DataTable as component from Airflow-Core UI
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
                <Table.Cell>
                  <WorkerStateBadge state={worker.state}>{worker.state}</WorkerStateBadge>
                </Table.Cell>
                <Table.Cell>
                  {worker.queues ? (
                    <ul>
                      {worker.queues.map((queue) => (
                        <li key={queue}>{queue}</li>
                      ))}
                    </ul>
                  ) : (
                    "(default)"
                  )}
                </Table.Cell>
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
                <Table.Cell>
                  <OperationsCell
                    worker={worker}
                    activeMaintenanceForm={activeMaintenanceForm}
                    onSetActiveMaintenanceForm={setActiveMaintenanceForm}
                    onRequestMaintenance={requestMaintenance}
                    onExitMaintenance={exitMaintenance}
                  />
                </Table.Cell>
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
