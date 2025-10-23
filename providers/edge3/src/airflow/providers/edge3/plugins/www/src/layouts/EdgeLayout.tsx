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
import { BrowserRouter, Route, Routes } from "react-router-dom";

import { JobsPage } from "src/pages/JobsPage";
import { WorkerPage } from "src/pages/WorkerPage";

import { NavTabs } from "./NavTabs";

export const EdgeLayout = () => {
  const tabs = [
    { label: "Edge Worker", value: "plugin/edge_worker" },
    { label: "Edge Jobs", value: "plugin/edge_jobs" },
  ];

  // Extract base URL from HTML base element to handle webserver.base_url configuration
  const baseUrl = document.querySelector("base")?.href ?? "http://localhost:8080/";
  const basename = new URL(baseUrl).pathname;

  return (
    <Box p={2} /* Compensate for parent padding from ExternalView */>
      <BrowserRouter basename={basename}>
        <NavTabs tabs={tabs} />
        <Routes>
          <Route path="plugin/edge_worker" element={<WorkerPage />} />
          <Route path="plugin/edge_jobs" element={<JobsPage />} />
        </Routes>
      </BrowserRouter>
    </Box>
  );
};
