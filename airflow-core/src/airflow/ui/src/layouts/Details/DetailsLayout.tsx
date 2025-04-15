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
import { Box, HStack, Flex, useDisclosure } from "@chakra-ui/react";
import { useReactFlow } from "@xyflow/react";
import type { PropsWithChildren, ReactNode } from "react";
import { LuFileWarning } from "react-icons/lu";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import { Outlet, useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useDagServiceGetDag, useDagWarningServiceListDagWarnings } from "openapi/queries";
import type { DAGResponse } from "openapi/requests/types.gen";
import BackfillBanner from "src/components/Banner/BackfillBanner";
import { SearchDagsButton } from "src/components/SearchDags";
import TriggerDAGButton from "src/components/TriggerDag/TriggerDAGButton";
import { ProgressBar } from "src/components/ui";
import { Toaster } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";
import { DAGWarningsModal } from "src/components/ui/DagWarningsModal";
import { OpenGroupsProvider } from "src/context/openGroups";

import { DagBreadcrumb } from "./DagBreadcrumb";
import { Graph } from "./Graph";
import { Grid } from "./Grid";
import { NavTabs } from "./NavTabs";
import { PanelButtons } from "./PanelButtons";

type Props = {
  readonly dag?: DAGResponse;
  readonly error?: unknown;
  readonly isLoading?: boolean;
  readonly tabs: Array<{ icon: ReactNode; label: string; value: string }>;
} & PropsWithChildren;

export const DetailsLayout = ({ children, error, isLoading, tabs }: Props) => {
  const { dagId = "" } = useParams();

  const { data: dag } = useDagServiceGetDag({ dagId });
  const [defaultDagView] = useLocalStorage<"graph" | "grid">("default_dag_view", "grid");
  const [dagView, setDagView] = useLocalStorage<"graph" | "grid">(`dag_view-${dagId}`, defaultDagView);
  const [limit, setLimit] = useLocalStorage<number>(`dag_runs_limit-${dagId}`, 10);

  const { fitView, getZoom } = useReactFlow();

  const { data: warningData } = useDagWarningServiceListDagWarnings({
    dagId,
  });
  const { onClose, onOpen, open } = useDisclosure();

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
      <BackfillBanner dagId={dagId} />
      <Box flex={1} minH={0}>
        <PanelGroup autoSaveId={dagId} direction="horizontal">
          <Panel defaultSize={dagView === "graph" ? 70 : 20} minSize={6}>
            <Box height="100%" overflowY="auto" position="relative" pr={2}>
              <PanelButtons dagView={dagView} limit={limit} setDagView={setDagView} setLimit={setLimit} />
              {dagView === "graph" ? <Graph /> : <Grid limit={limit} />}
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
          <Panel defaultSize={dagView === "graph" ? 30 : 80} minSize={20}>
            <Box display="flex" flexDirection="column" h="100%">
              {children}
              {Boolean(error) || (warningData?.dag_warnings.length ?? 0) > 0 ? (
                <>
                  <ActionButton
                    actionName="Dag warnings/errors"
                    colorPalette={Boolean(error) ? "red" : "orange"}
                    icon={<LuFileWarning />}
                    margin="2"
                    marginBottom="-1"
                    onClick={onOpen}
                    rounded="full"
                    text={String(warningData?.total_entries ?? 0 + Number(error))}
                    variant="solid"
                  />

                  <DAGWarningsModal
                    error={error}
                    onClose={onClose}
                    open={open}
                    warnings={warningData?.dag_warnings}
                  />
                </>
              ) : undefined}
              <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
              <NavTabs tabs={tabs} />
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
