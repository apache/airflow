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

import { ChatPage } from "src/components/ChatPage";
import { NoSession } from "src/components/NoSession";
import { Toaster } from "src/toaster";

import { localSystem } from "./theme";

export interface PluginComponentProps {
  dagId?: string;
  mapIndex?: string;
  runId?: string;
  taskId?: string;
  taskInstance?: { id?: string; region_id?: string; region_index?: number };
}

/**
 * Main plugin component for HITL Review.
 *
 * Receives dagId, runId, taskId, mapIndex as props from the Airflow React plugin
 * host (from route params). Renders ChatPage when all params are present,
 * otherwise shows NoSession fallback.
 */
const PluginComponent: FC<PluginComponentProps> = (props) => {
  const {
    dagId = "",
    runId = "",
    taskId = "",
    mapIndex: mapIndexProp = "-1",
    taskInstance,
  } = props;
  const mapIndex = /^-?\d+$/.test(String(mapIndexProp)) ? parseInt(String(mapIndexProp), 10) : -1;

  if (!dagId || !runId || !taskId || ("taskInstance" in props && taskInstance == null)) {
    return <NoSession />;
  }

  const region = taskInstance?.region_id !== undefined && taskInstance.region_index !== undefined
    ? { region_id: taskInstance.region_id, region_index: taskInstance.region_index }
    : undefined;

  return <ChatPage
    key={`${dagId}/${runId}/${taskId}/${mapIndex}/${taskInstance?.id ?? ""}/${region?.region_id ?? ""}/${region?.region_index ?? ""}`}
    dagId={dagId} runId={runId} taskId={taskId} mapIndex={mapIndex} region={region}
  />;
};

/**
 * Plugin component wrapped with ChakraProvider for consistent theming with the host.
 * Chakra semantic tokens handle light/dark mode automatically.
 */
const WrappedPluginComponent: FC<PluginComponentProps> = (props) => {
  const system = (globalThis as typeof globalThis & { ChakraUISystem?: typeof localSystem }).ChakraUISystem ?? localSystem;

  return (
    <ChakraProvider value={system}>
      <Box height="100%" minHeight={0}>
        <PluginComponent {...props} />
      </Box>
      <Toaster />
    </ChakraProvider>
  );
};

export default WrappedPluginComponent;
