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

import { Box, Button, ChakraProvider, Spinner, Stack } from "@chakra-ui/react";
import type { SystemContext } from "@chakra-ui/react";
import { type FC, useCallback, useEffect, useState } from "react";

import { ApiError, fetchTraceSummary } from "src/api";
import { TraceCard } from "src/components/TraceCard";
import { TraceList } from "src/components/TraceList";
import type { TraceSummary } from "src/types";

import { localSystem } from "./theme";

export interface PluginComponentProps {
  dagId?: string;
  mapIndex?: string;
  runId?: string;
  taskId?: string;
}

const NoTrace: FC<{ message: string }> = ({ message }) => (
  <Box bg="bg.subtle" borderRadius="lg" borderWidth="1px" color="fg.muted" fontSize="sm" p={5}>
    {message}
  </Box>
);

const PluginComponent: FC<PluginComponentProps> = ({
  dagId = "",
  runId = "",
  taskId = "",
  mapIndex: mapIndexProp = "-1",
}) => {
  const mapIndex = /^-?\d+$/.test(String(mapIndexProp)) ? parseInt(String(mapIndexProp), 10) : -1;
  const [summary, setSummary] = useState<TraceSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const isTaskInstanceContext = Boolean(dagId && runId && taskId);

  const load = useCallback(async () => {
    if (!isTaskInstanceContext) return;
    setLoading(true);
    try {
      const data = await fetchTraceSummary(dagId, runId, taskId, mapIndex);
      setSummary(data);
      setError(null);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : err instanceof Error ? err.message : String(err));
      setSummary(null);
    } finally {
      setLoading(false);
    }
  }, [isTaskInstanceContext, dagId, runId, taskId, mapIndex]);

  useEffect(() => {
    void load();
  }, [load]);

  if (!isTaskInstanceContext) {
    // dagId alone (no runId/taskId) -> the dag-level "Agent traces" tab, scoped
    // to that dag. All three absent -> the nav-level "AI Traces" destination,
    // unscoped.
    return <TraceList dagId={dagId || undefined} />;
  }

  if (loading) {
    return (
      <Box p={5}>
        <Spinner size="sm" />
      </Box>
    );
  }

  if (error || !summary) {
    return (
      <Stack gap={3} p={5}>
        <NoTrace message={error ?? "No trace found for this task instance."} />
        <Button onClick={() => void load()} size="sm" variant="outline">
          Retry
        </Button>
      </Stack>
    );
  }

  return (
    <Box p={5}>
      <TraceCard summary={summary} />
    </Box>
  );
};

const WrappedPluginComponent: FC<PluginComponentProps> = (props) => {
  const system = (globalThis as { ChakraUISystem?: SystemContext }).ChakraUISystem ?? localSystem;

  return (
    <ChakraProvider value={system}>
      <Box height="100%" minHeight={0}>
        <PluginComponent {...props} />
      </Box>
    </ChakraProvider>
  );
};

export default WrappedPluginComponent;
