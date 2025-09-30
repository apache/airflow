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
import { Box, HStack, Flex, useDisclosure, IconButton } from "@chakra-ui/react";
import { useReactFlow } from "@xyflow/react";
import { useRef, useState } from "react";
import type { PropsWithChildren, ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { FaChevronLeft, FaChevronRight } from "react-icons/fa";
import { LuFileWarning } from "react-icons/lu";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import { Outlet, useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useDagServiceGetDag, useDagWarningServiceListDagWarnings } from "openapi/queries";
import type { DagRunType } from "openapi/requests/types.gen";
import BackfillBanner from "src/components/Banner/BackfillBanner";
import { SearchDagsButton } from "src/components/SearchDags";
import TriggerDAGButton from "src/components/TriggerDag/TriggerDAGButton";
import { ProgressBar } from "src/components/ui";
import { Toaster } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";
import { DAGWarningsModal } from "src/components/ui/DagWarningsModal";
import { Tooltip } from "src/components/ui/Tooltip";
import { HoverProvider } from "src/context/hover";
import { OpenGroupsProvider } from "src/context/openGroups";

import { DagBreadcrumb } from "./DagBreadcrumb";
import { Gantt } from "./Gantt/Gantt";
import { Graph } from "./Graph";
import { Grid } from "./Grid";
import { NavTabs } from "./NavTabs";
import { PanelButtons } from "./PanelButtons";

type Props = {
  readonly error?: unknown;
  readonly isLoading?: boolean;
  readonly tabs: Array<{ icon: ReactNode; label: string; value: string }>;
} & PropsWithChildren;

export const DetailsLayout = ({ children, error, isLoading, tabs }: Props) => {
  const { t: translate } = useTranslation();
  const { dagId = "", runId } = useParams();
  const { data: dag } = useDagServiceGetDag({ dagId });
  const [defaultDagView] = useLocalStorage<"graph" | "grid">("default_dag_view", "grid");
  const panelGroupRef = useRef(null);
  const [dagView, setDagView] = useLocalStorage<"graph" | "grid">(`dag_view-${dagId}`, defaultDagView);
  const [limit, setLimit] = useLocalStorage<number>(`dag_runs_limit-${dagId}`, 10);
  const [runTypeFilter, setRunTypeFilter] = useLocalStorage<DagRunType | undefined>(
    `run_type_filter-${dagId}`,
    undefined,
  );
  const [triggeringUserFilter, setTriggeringUserFilter] = useLocalStorage<string | undefined>(
    `triggering_user_filter-${dagId}`,
    undefined,
  );

  const [showGantt, setShowGantt] = useLocalStorage<boolean>(`show_gantt-${dagId}`, false);
  const { fitView, getZoom } = useReactFlow();
  const { data: warningData } = useDagWarningServiceListDagWarnings({ dagId });
  const { onClose, onOpen, open } = useDisclosure();
  const [isRightPanelCollapsed, setIsRightPanelCollapsed] = useState(false);
  const { i18n } = useTranslation();
  const direction = i18n.dir();

  return (
    <HoverProvider>
      <OpenGroupsProvider dagId={dagId}>
        <HStack justifyContent="space-between" mb={2}>
          <DagBreadcrumb />
          <Flex gap={1}>
            <SearchDagsButton />
            {dag === undefined ? undefined : (
              <TriggerDAGButton
                dagDisplayName={dag.dag_display_name}
                dagId={dag.dag_id}
                isPaused={dag.is_paused}
              />
            )}
          </Flex>
        </HStack>
        <Toaster />
        <BackfillBanner dagId={dagId} />
        <Box flex={1} minH={0}>
          {isRightPanelCollapsed ? (
            <Tooltip content={translate("common:showDetailsPanel")}>
              <IconButton
                aria-label={translate("common:showDetailsPanel")}
                bg="fg.subtle"
                borderRadius={direction === "ltr" ? "100% 0 0 100%" : "0 100% 100% 0"}
                boxShadow="md"
                left={direction === "rtl" ? "-5px" : undefined}
                onClick={() => setIsRightPanelCollapsed(false)}
                position="absolute"
                right={direction === "ltr" ? "-5px" : undefined}
                size="2xs"
                top="50%"
                zIndex={10}
              >
                {direction === "ltr" ? <FaChevronLeft /> : <FaChevronRight />}
              </IconButton>
            </Tooltip>
          ) : undefined}
          <PanelGroup
            autoSaveId={`${dagView}-${direction}`}
            dir={direction}
            direction="horizontal"
            key={`${dagView}-${direction}`}
            ref={panelGroupRef}
          >
            <Panel
              defaultSize={dagView === "graph" ? 70 : 20}
              id="main-panel"
              minSize={showGantt && dagView === "grid" && Boolean(runId) ? 35 : 6}
              order={1}
            >
              <Box height="100%" marginInlineEnd={2} overflowY="auto" paddingRight={4} position="relative">
                <PanelButtons
                  dagView={dagView}
                  limit={limit}
                  panelGroupRef={panelGroupRef}
                  runTypeFilter={runTypeFilter}
                  setDagView={setDagView}
                  setLimit={setLimit}
                  setRunTypeFilter={setRunTypeFilter}
                  setShowGantt={setShowGantt}
                  setTriggeringUserFilter={setTriggeringUserFilter}
                  showGantt={showGantt}
                  triggeringUserFilter={triggeringUserFilter}
                />
                {dagView === "graph" ? (
                  <Graph />
                ) : (
                  <HStack alignItems="flex-end" gap={0}>
                    <Grid
                      limit={limit}
                      runType={runTypeFilter}
                      showGantt={Boolean(runId) && showGantt}
                      triggeringUser={triggeringUserFilter}
                    />
                    {showGantt ? (
                      <Gantt limit={limit} runType={runTypeFilter} triggeringUser={triggeringUserFilter} />
                    ) : undefined}
                  </HStack>
                )}
              </Box>
            </Panel>
            {!isRightPanelCollapsed && (
              <>
                <PanelResizeHandle
                  className="resize-handle"
                  onDragging={(isDragging) => {
                    if (!isDragging) {
                      const zoom = getZoom();

                      void fitView({ maxZoom: zoom, minZoom: zoom });
                    }
                  }}
                >
                  <Box
                    alignItems="center"
                    bg="border.emphasized"
                    cursor="col-resize"
                    display="flex"
                    h="100%"
                    justifyContent="center"
                    position="relative"
                    w={0.5}
                    // onClick={(e) => console.log(e)}
                  />
                </PanelResizeHandle>

                {/* Collapse button positioned next to the resize handle */}

                <Panel defaultSize={dagView === "graph" ? 30 : 80} id="details-panel" minSize={20} order={2}>
                  <Box display="flex" flexDirection="column" h="100%" position="relative">
                    <Tooltip content={translate("common:collapseDetailsPanel")}>
                      <IconButton
                        aria-label={translate("common:collapseDetailsPanel")}
                        bg="fg.subtle"
                        borderRadius={direction === "ltr" ? "0 100% 100% 0" : "100% 0 0 100%"}
                        boxShadow="md"
                        left={direction === "ltr" ? "-5px" : undefined}
                        onClick={() => setIsRightPanelCollapsed(true)}
                        position="absolute"
                        right={direction === "rtl" ? "-5px" : undefined}
                        size="2xs"
                        top="50%"
                        zIndex={2}
                      >
                        {direction === "ltr" ? <FaChevronRight /> : <FaChevronLeft />}
                      </IconButton>
                    </Tooltip>
                    {children}
                    {Boolean(error) || (warningData?.dag_warnings.length ?? 0) > 0 ? (
                      <>
                        <ActionButton
                          actionName={translate("common:dagWarnings")}
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
                    <Box flexGrow={1} overflow="auto" px={2}>
                      <Outlet />
                    </Box>
                  </Box>
                </Panel>
              </>
            )}
          </PanelGroup>
        </Box>
      </OpenGroupsProvider>
    </HoverProvider>
  );
};
