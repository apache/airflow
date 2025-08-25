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

import { Box } from "@chakra-ui/react";

import { useUiServiceWorker } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";

export const WorkerPage = () => {
  const { data, error } = useUiServiceWorker();

  // TODO to make it proper
  // Use DataTable as component from Airflow-Core UI
  // Add auto-refresh
  // Add Actions for Maintenance
  // Add sorting
  // Add filtering
  if (data)
    return (
      <Box p={2}>
        <table cellPadding={2} cellSpacing={2} style={{ border: "1px solid gray" }}>
          <thead>
            <tr>
              <th style={{ border: "1px solid gray", padding: "2px" }}>Worker Name</th>
              <th style={{ border: "1px solid gray", padding: "2px" }}>State</th>
            </tr>
          </thead>
          <tbody>
            {data.workers.map((worker) => (
              <tr key={worker.worker_name}>
                <td style={{ border: "1px solid gray", padding: "2px" }}>{worker.worker_name}</td>
                <td style={{ border: "1px solid gray", padding: "2px" }}>{worker.state}</td>
              </tr>
            ))}
          </tbody>
        </table>
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
  return (<Box p={2}>Loading...</Box>);
};
