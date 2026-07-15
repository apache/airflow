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

import { Box, ChakraProvider } from "@chakra-ui/react";
import type { FC } from "react";

import { TaskDQOverview } from "src/components/TaskDQOverview";
import { TaskDQPanel } from "src/components/TaskDQPanel";

import { localSystem } from "./theme";

export interface PluginComponentProps {
  dagId?: string;
  mapIndex?: string;
  runId?: string;
  taskId?: string;
}

/**
 * Main plugin component for the Data Quality task and task-instance views.
 *
 * Receives dagId, runId, taskId, mapIndex as props from the Airflow React plugin host (from
 * route params), matching the react_apps contract.
 */
const PluginComponent: FC<PluginComponentProps> = ({
  dagId = "",
  runId = "",
  taskId = "",
  mapIndex: mapIndexProp = "-1",
}) => {
  if (!runId) {
    return <TaskDQOverview dagId={dagId} taskId={taskId} />;
  }

  const mapIndex = /^-?\d+$/.test(String(mapIndexProp)) ? parseInt(String(mapIndexProp), 10) : -1;

  return <TaskDQPanel dagId={dagId} mapIndex={mapIndex} runId={runId} taskId={taskId} />;
};

/**
 * Plugin component wrapped with ChakraProvider for consistent theming with the host.
 * Chakra semantic tokens handle light/dark mode automatically.
 */
const WrappedPluginComponent: FC<PluginComponentProps> = (props) => {
  const system =
    (globalThis as { ChakraUISystem?: typeof localSystem }).ChakraUISystem ?? localSystem;

  return (
    <ChakraProvider value={system}>
      <Box height="100%" minHeight={0}>
        <PluginComponent {...props} />
      </Box>
    </ChakraProvider>
  );
};

export default WrappedPluginComponent;
