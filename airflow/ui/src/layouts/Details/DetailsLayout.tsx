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
import { Box, HStack, IconButton, ButtonGroup, Flex } from "@chakra-ui/react";
import { useReactFlow } from "@xyflow/react";
import type { PropsWithChildren, ReactNode } from "react";
import { FiGrid } from "react-icons/fi";
import { MdOutlineAccountTree } from "react-icons/md";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import { Outlet, useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useDagServiceGetDag } from "openapi/queries";
import type { DAGResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchDagsButton } from "src/components/SearchDags";
import TriggerDAGButton from "src/components/TriggerDag/TriggerDAGButton";
import { ProgressBar } from "src/components/ui";
import { Toaster } from "src/components/ui";
import { OpenGroupsProvider } from "src/context/openGroups";

import { DagBreadcrumb } from "./DagBreadcrumb";
import { Graph } from "./Graph";
import { Grid } from "./Grid";
import { NavTabs } from "./NavTabs";

type Props = {
  readonly dag?: DAGResponse;
  readonly error?: unknown;
  readonly isLoading?: boolean;
  readonly tabs: Array<{ icon: ReactNode; label: string; value: string }>;
} & PropsWithChildren;

export const DetailsLayout = ({ children, error, isLoading, tabs }: Props) => {
  const { dagId = "" } = useParams();

  const { data: dag } = useDagServiceGetDag({ dagId });

  const [dagView, setDagView] = useLocalStorage<"graph" | "grid">(
    `dag_view-${dagId}`,
    dag && (dag.default_view === "graph" || dag.default_view === "grid") ? dag.default_view : "grid",
  );

  const { fitView, getZoom } = useReactFlow();

  return (
    <OpenGroupsProvider dagId={dagId}>
      <HStack justifyContent="space-between" mb={2}>
        <DagBreadcrumb />
        <Flex gap={1}>
          <SearchDagsButton />
          {dag === undefined ? undefined : <TriggerDAGButton dag={dag} />}
        </Flex>
      </HStack>
      <Toaster />
      <Box flex={1} minH={0}>
        <PanelGroup autoSaveId={dagId} direction="horizontal">
          <Panel defaultSize={20} minSize={6}>
            <Box height="100%" position="relative">
              <ButtonGroup
                attached
                left={0}
                position="absolute"
                size="sm"
                top={0}
                variant="outline"
                zIndex={1}
              >
                <IconButton
                  aria-label="Show Grid"
                  colorPalette="blue"
                  onClick={() => setDagView("grid")}
                  title="Show Grid"
                  variant={dagView === "grid" ? "solid" : "outline"}
                >
                  <FiGrid />
                </IconButton>
                <IconButton
                  aria-label="Show Graph"
                  colorPalette="blue"
                  onClick={() => setDagView("graph")}
                  title="Show Graph"
                  variant={dagView === "graph" ? "solid" : "outline"}
                >
                  <MdOutlineAccountTree />
                </IconButton>
              </ButtonGroup>
              {dagView === "graph" ? <Graph /> : <Grid />}
            </Box>
          </Panel>
          <PanelResizeHandle
            className="resize-handle"
            onDragging={(isDragging) => {
              if (!isDragging) {
                const zoom = getZoom();

                void fitView({ maxZoom: zoom, minZoom: zoom });
              }
            }}
          >
            <Box bg="fg.subtle" cursor="col-resize" h="100%" transition="background 0.2s" w={0.5} />
          </PanelResizeHandle>
          <Panel defaultSize={50} minSize={20}>
            <Box display="flex" flexDirection="column" h="100%">
              {children}
              <ErrorAlert error={error} />
              <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
              <NavTabs keepSearch tabs={tabs} />
              <Box h="100%" overflow="auto" px={2}>
                <Outlet />
              </Box>
            </Box>
          </Panel>
        </PanelGroup>
      </Box>
    </OpenGroupsProvider>
  );
};
