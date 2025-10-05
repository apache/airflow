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
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";

import { JobsPage } from "src/pages/JobsPage";
import { WorkerPage } from "src/pages/WorkerPage";

import { NavTabs } from "./NavTabs";

export const EdgeLayout = () => {
  const tabs = [
    { label: "Edge Worker", value: "worker" },
    { label: "Edge Jobs", value: "jobs" },
  ];

  // Need to check whether ReactRouterDOM is available globally
  // because in Airflow 3.1.0, the plugin system was missing this.
  if ((globalThis as Record<string, unknown>).ReactRouterDOM) {
    return (
      <Box p={2} /* Compensate for parent padding from ExternalView */>
        <NavTabs tabs={tabs} />
        <Routes>
          <Route index element={<Navigate to="worker" replace />} />
          <Route path="worker" element={<WorkerPage />} />
          <Route path="jobs" element={<JobsPage />} />
        </Routes>
      </Box>
    );
  } else {
    // Fallback in 3.1.0, can be removed if we drop support for it
    return (
      <Box p={2} /* Compensate for parent padding from ExternalView */>
        <NavTabs tabs={tabs} />
        <BrowserRouter>
          <Routes>
            <Route index element={<Navigate to="worker" replace />} />
            <Route path="worker" element={<WorkerPage />} />
            <Route path="jobs" element={<JobsPage />} />
          </Routes>
        </BrowserRouter>
      </Box>
    );
  }
};
