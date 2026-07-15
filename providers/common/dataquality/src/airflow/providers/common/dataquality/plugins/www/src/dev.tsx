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

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import PluginComponent from "./main";

// Development entry point - mount with mock task instance params for local testing.
// `vite dev` proxies /dataquality to a running Airflow API server (see vite.config.ts), so point
// dagId/runId/taskId/mapIndex at a real task instance that has a persisted DQ run.
createRoot(document.querySelector("#root") as HTMLDivElement).render(
  <StrictMode>
    <PluginComponent dagId="example_dq_check" mapIndex="-1" runId="manual__2026-07-04" taskId="check_orders" />
  </StrictMode>,
);
