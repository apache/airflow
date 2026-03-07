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

import { StrictMode, type FC } from "react";
import { createRoot } from "react-dom/client";

import { ChatPage } from "src/components/ChatPage";
import { NoSession } from "src/components/NoSession";

export type PluginComponentProps = object;

/**
 * Main plugin component.
 *
 * Reads dag_id / run_id / task_id from the URL search params injected by
 * the Airflow external_views iframe.  If parameters are missing it shows
 * the "no session" fallback.
 */
const PluginComponent: FC<PluginComponentProps> = () => {
  const params = new URLSearchParams(globalThis.location.search);
  const dagId = params.get("dag_id") ?? "";
  const runId = (params.get("run_id") ?? "").replace(/ /g, "+");
  const taskId = params.get("task_id") ?? "";
  const rawMapIndex = params.get("map_index") ?? "-1";
  const mapIndex = /^-?\d+$/.test(rawMapIndex) ? parseInt(rawMapIndex, 10) : -1;

  if (!dagId || !runId || !taskId) {
    return <NoSession />;
  }

  return <ChatPage dagId={dagId} runId={runId} taskId={taskId} mapIndex={mapIndex} />;
};

const root = document.getElementById("root");
if (root) {
  createRoot(root).render(
    <StrictMode>
      <PluginComponent />
    </StrictMode>,
  );
}

export default PluginComponent;
