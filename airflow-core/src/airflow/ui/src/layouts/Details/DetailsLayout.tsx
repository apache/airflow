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
import { useState, useEffect, useCallback } from "react";
import type { PropsWithChildren, ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { FaChevronLeft, FaChevronRight } from "react-icons/fa";
import { LuFileWarning } from "react-icons/lu";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import { Outlet, useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { useDagServiceGetDag, useDagWarningServiceListDagWarnings } from "openapi/queries";
import BackfillBanner from "src/components/Banner/BackfillBanner";
import { SearchDagsButton } from "src/components/SearchDags";
import TriggerDAGButton from "src/components/TriggerDag/TriggerDAGButton";
import { ProgressBar } from "src/components/ui";
import { Toaster } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";
import { DAGWarningsModal } from "src/components/ui/DagWarningsModal";
import { Tooltip } from "src/components/ui/Tooltip";
import { OpenGroupsProvider } from "src/context/openGroups";

import { DagBreadcrumb } from "./DagBreadcrumb";
import { Graph } from "./Graph";
import { Grid } from "./Grid";
import { NavTabs } from "./NavTabs";
import { PanelButtons } from "./PanelButtons";

const PANEL_RESET_DELAY = 10;
const DEFAULT_GRAPH_SIZES = [70, 30] as const;
const DEFAULT_GRID_SIZES = [20, 80] as const;

type PanelSizes = readonly [number, number];

const isPanelSizes = (value: unknown): value is PanelSizes =>
  Array.isArray(value) && value.length === 2 && value.every((size) => typeof size === "number" && size > 0);

type Props = {
  readonly error?: unknown;
  readonly isLoading?: boolean;
  readonly tabs: Array<{ icon: ReactNode; label: string; value: string }>;
} & PropsWithChildren;

export const DetailsLayout = ({ children, error, isLoading, tabs }: Props) => {
  const { t: translate } = useTranslation();
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
  const [isRightPanelCollapsed, setIsRightPanelCollapsed] = useState(false);

  const getDefaultSizes = useCallback(
    (): PanelSizes => (dagView === "graph" ? DEFAULT_GRAPH_SIZES : DEFAULT_GRID_SIZES),
    [dagView],
  );

  const getSavedSizes = useCallback((): PanelSizes => {
    const storageKey = `panel-${dagId}-${dagView}`;

    try {
      const cached = sessionStorage.getItem(storageKey);

      if (cached !== null && cached !== "") {
        const parsed: unknown = JSON.parse(cached);

        if (isPanelSizes(parsed)) {
          return parsed;
        }
      }
    } catch {
      // Failed to parse saved panel sizes - fallback to default
    }

    return getDefaultSizes();
  }, [dagId, dagView, getDefaultSizes]);

  const savePanelSizes = useCallback(
    (sizes: Array<number>) => {
      try {
        const storageKey = `panel-${dagId}-${dagView}`;

        sessionStorage.setItem(storageKey, JSON.stringify(sizes));
      } catch {
        // Failed to save panel sizes - continue silently
      }
    },
    [dagId, dagView],
  );

  const [currentSizes, setCurrentSizes] = useState<PanelSizes>(getSavedSizes);

  const [showPanel, setShowPanel] = useState(false);

  useEffect(() => {
    const newSizes = getSavedSizes();

    setCurrentSizes(newSizes);

    setShowPanel(false);
    const timer = setTimeout(() => {
      setShowPanel(true);
    }, PANEL_RESET_DELAY);

    return () => clearTimeout(timer);
  }, [dagView, dagId, getSavedSizes]);

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
        {isRightPanelCollapsed ? (
          <Tooltip content={translate("common:showDetailsPanel")}>
            <IconButton
              aria-label={translate("common:showDetailsPanel")}
              bg="bg.surface"
              borderRadius="full"
              boxShadow="md"
              cursor="pointer"
              onClick={() => setIsRightPanelCollapsed(false)}
              position="absolute"
              right={0}
              size="sm"
              top="50%"
              zIndex={10}
            >
              <FaChevronLeft />
            </IconButton>
          </Tooltip>
        ) : undefined}
        {showPanel ? (
          <PanelGroup
            direction="horizontal"
            onLayout={(sizes) => {
              if (isPanelSizes(sizes)) {
                setCurrentSizes(sizes);
                savePanelSizes(sizes);
              }
            }}
          >
            <Panel defaultSize={currentSizes[0]} id="main-panel" minSize={6} order={1}>
              <Box height="100%" overflowY="auto" position="relative" pr={2}>
                <PanelButtons dagView={dagView} limit={limit} setDagView={setDagView} setLimit={setLimit} />
                {dagView === "graph" ? <Graph /> : <Grid limit={limit} />}
              </Box>
            </Panel>
            {!isRightPanelCollapsed && (
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
                  bg="fg.subtle"
                  cursor="col-resize"
                  display="flex"
                  h="100%"
                  justifyContent="center"
                  position="relative"
                  w={0.5}
                >
                  <Tooltip content={translate("common:collapseDetailsPanel")}>
                    <IconButton
                      aria-label={translate("common:collapseDetailsPanel")}
                      bg="bg.surface"
                      borderRadius="full"
                      boxShadow="md"
                      cursor="pointer"
                      onClick={() => setIsRightPanelCollapsed(true)}
                      size="xs"
                      zIndex={2}
                    >
                      <FaChevronRight />
                    </IconButton>
                  </Tooltip>
                </Box>
              </PanelResizeHandle>
            )}
            {!isRightPanelCollapsed && (
              <Panel defaultSize={currentSizes[1]} id="details-panel" minSize={20} order={2}>
                <Box display="flex" flexDirection="column" h="100%">
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
            )}
          </PanelGroup>
        ) : (
          <Box alignItems="center" display="flex" h="100%" justifyContent="center">
            Loading panel...
          </Box>
        )}
      </Box>
    </OpenGroupsProvider>
  );
};
