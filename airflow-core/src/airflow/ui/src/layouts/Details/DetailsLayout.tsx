/* eslint-disable max-lines */

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
import { Box, Flex, HStack, IconButton, useDisclosure } from "@chakra-ui/react";
import { useReactFlow } from "@xyflow/react";
import { useEffect, useRef, useState } from "react";
import type { PropsWithChildren, ReactNode, RefObject } from "react";
import { useTranslation } from "react-i18next";
import { FaChevronLeft, FaChevronRight } from "react-icons/fa";
import { LuFileWarning } from "react-icons/lu";
import {
  type ImperativePanelGroupHandle,
  Panel,
  PanelGroup,
  PanelResizeHandle,
} from "react-resizable-panels";
import { Outlet, useParams, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import {
  useDagRunServiceGetDagRun,
  useDagServiceGetDag,
  useDagWarningServiceListDagWarnings,
} from "openapi/queries";
import type { DagRunState, DagRunType } from "openapi/requests/types.gen";
import BackfillBanner from "src/components/Banner/BackfillBanner";
import { DAGWarningsModal } from "src/components/DAGWarningsModal";
import { SearchDagsButton } from "src/components/SearchDags";
import { TriggerDAGButton } from "src/components/TriggerDag/TriggerDAGButton";
import { ProgressBar } from "src/components/ui";
import { Toaster } from "src/components/ui";
import { Tooltip } from "src/components/ui/Tooltip";
import type { DagView } from "src/constants/dagView";
import { DEFAULT_DAG_VIEW_KEY } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";
import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { HoverProvider, useHover } from "src/context/hover";
import { OpenGroupsProvider } from "src/context/openGroups";
import { useGridRuns } from "src/queries/useGridRuns.ts";

import { DagBreadcrumb } from "./DagBreadcrumb";
import { Gantt } from "./Gantt/Gantt";
import { Graph } from "./Graph";
import { Grid } from "./Grid";
import { NavTabs } from "./NavTabs";
import { PanelButtons } from "./PanelButtons";

// Separate component so useHover can be called inside HoverProvider.
const SharedScrollBox = ({
  children,
  scrollRef,
}: {
  readonly children: ReactNode;
  readonly scrollRef: RefObject<HTMLDivElement | null>;
}) => {
  const { setHoveredTaskId } = useHover();

  return (
    <Box
      height="100%"
      minH={0}
      minW={0}
      onMouseLeave={() => setHoveredTaskId(undefined)}
      overflowX="hidden"
      overflowY="auto"
      ref={scrollRef}
      style={{ scrollbarGutter: "stable" }}
      w="100%"
    >
      {children}
    </Box>
  );
};

type Props = {
  readonly error?: unknown;
  readonly isLoading?: boolean;
  readonly tabs: Array<{ icon: ReactNode; label: string; value: string }>;
} & PropsWithChildren;

export const DetailsLayout = ({ children, error, isLoading, tabs }: Props) => {
  const { t: translate } = useTranslation();
  const { dagId = "", runId } = useParams();
  const { data: dag } = useDagServiceGetDag({ dagId });
  const [dagView, setDagView] = useLocalStorage<DagView>(DEFAULT_DAG_VIEW_KEY, "grid");
  const panelGroupRef = useRef<ImperativePanelGroupHandle | null>(null);
  const [searchParams, setSearchParams] = useSearchParams();

  // Global setting: applies to all Dags (intentionally not scoped to dagId)
  const [showVersionIndicatorMode, setShowVersionIndicatorMode] = useLocalStorage(
    `version_indicator_display_mode`,
    VersionIndicatorOptions.ALL,
  );

  // Helper that updates a single search param without touching the rest.
  // Uses replace so filter tweaks don't pollute browser history.
  const setParam = (key: string, value: string | undefined) => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);

        if (value === undefined) {
          next.delete(key);
        } else {
          next.set(key, value);
        }

        return next;
      },
      { replace: true },
    );
  };

  // --- Read state from URL ---
  const limit = Number(searchParams.get(SearchParamsKeys.LIMIT) ?? "10");
  const runAfterGte = searchParams.get(SearchParamsKeys.RUN_AFTER_GTE) ?? undefined;
  const runAfterLte = searchParams.get(SearchParamsKeys.RUN_AFTER_LTE) ?? undefined;
  const runTypeFilter = (searchParams.get(SearchParamsKeys.RUN_TYPE) as DagRunType | null) ?? undefined;
  const triggeringUserFilter = searchParams.get(SearchParamsKeys.TRIGGERING_USER_NAME_PATTERN) ?? undefined;
  const dagRunStateFilter = (searchParams.get(SearchParamsKeys.STATE) as DagRunState | null) ?? undefined;

  // --- Setters that write back to URL ---
  const setLimit = (value: number) => setParam(SearchParamsKeys.LIMIT, String(value));
  // Only LTE is needed directly: ceiling logic and jump-to-latest both touch it.
  // GTE and the filter params (state, run_type, triggering_user) are managed by GridFilters/FilterBar.
  const setRunAfterLte = (value: string | undefined) => setParam(SearchParamsKeys.RUN_AFTER_LTE, value);

  // Reset to grid when there is no runId. Remove this when we do gantt averages.
  useEffect(() => {
    if (!Boolean(runId) && dagView === "gantt") {
      setDagView("grid");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId, dagView]);

  // Transient pagination offset — reset whenever the user changes filters
  const [offset, setOffset] = useState(0);

  // Guard so the run_after_lte ceiling is set at most once on initial load —
  // navigating between runs after the page has loaded should never shift the grid window.
  const runAfterLteSetRef = useRef(false);

  const noDateFilters = runAfterGte === undefined && runAfterLte === undefined;

  // Initial grid page (no date ceiling) — loaded first so we can check whether
  // the selected run is already visible before fetching it individually.
  const { data: initialGridRuns } = useGridRuns({
    dagRunState: dagRunStateFilter,
    limit,
    runType: runTypeFilter,
    triggeringUser: triggeringUserFilter,
  });

  // Whether the selected run is absent from the initial grid page and we don't
  // yet have a ceiling set. Only true once initialGridRuns has loaded.
  const runMissingFromGrid =
    Boolean(runId) &&
    noDateFilters &&
    !runAfterLteSetRef.current &&
    initialGridRuns !== undefined &&
    !initialGridRuns.some((dr) => dr.run_id === runId);

  // Only fetch the individual dag run once we know it isn't already visible.
  const { data: selectedDagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId: runId ?? "" }, undefined, {
    enabled: runMissingFromGrid,
  });

  // Once we have the run's run_after, write it into the URL as the ceiling.
  // Both deps are needed: dag_run_id so we react when the fetch resolves,
  // runMissingFromGrid so we react when initialGridRuns loads and confirms
  // the run is absent (handles the case where selectedDagRun is already cached).
  useEffect(() => {
    if (selectedDagRun && runMissingFromGrid) {
      runAfterLteSetRef.current = true;
      setRunAfterLte(selectedDagRun.run_after);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDagRun?.dag_run_id, runMissingFromGrid]);

  // Reset offset whenever URL filters change so we start from the top of the new window.
  useEffect(() => {
    setOffset(0);
  }, [runAfterGte, runAfterLte, runTypeFilter, dagRunStateFilter, triggeringUserFilter, limit]);

  // Jump to the very latest runs: clear the URL ceiling and reset offset.
  const handleJumpToLatest = () => {
    setOffset(0);
    setRunAfterLte(undefined);
  };

  const { fitView, getZoom } = useReactFlow();
  const { data: warningData } = useDagWarningServiceListDagWarnings({ dagId });
  const { onClose, onOpen, open } = useDisclosure();
  const [isRightPanelCollapsed, setIsRightPanelCollapsed] = useState(false);
  const { i18n } = useTranslation();
  const direction = i18n.dir();
  const sharedGridGanttScrollRef = useRef<HTMLDivElement | null>(null);
  // Treat "gantt" as "grid" for panel layout persistence so switching between them doesn't reset sizes.
  const panelViewKey = dagView === "gantt" ? "grid" : dagView;

  return (
    <HoverProvider>
      <OpenGroupsProvider dagId={dagId}>
        <HStack justifyContent="space-between" mb={2}>
          <DagBreadcrumb />
          <Flex gap={1}>
            <SearchDagsButton />
            {dag === undefined ? undefined : (
              <TriggerDAGButton
                allowedRunTypes={dag.allowed_run_types}
                dagDisplayName={dag.dag_display_name}
                dagId={dag.dag_id}
                isPaused={dag.is_paused}
                variant="outline"
                withText
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
            autoSaveId={`${panelViewKey}-${direction}`}
            dir={direction}
            direction="horizontal"
            key={`${panelViewKey}-${direction}`}
            ref={panelGroupRef}
          >
            <Panel
              defaultSize={dagView === "graph" ? 70 : 20}
              id="main-panel"
              minSize={dagView === "gantt" && Boolean(runId) ? 35 : 6}
              order={1}
            >
              <Flex flexDirection="column" height="100%">
                <PanelButtons
                  dagView={dagView}
                  limit={limit}
                  panelGroupRef={panelGroupRef}
                  setDagView={setDagView}
                  setLimit={setLimit}
                  setShowVersionIndicatorMode={setShowVersionIndicatorMode}
                  showVersionIndicatorMode={showVersionIndicatorMode}
                />
                <Box flex={1} minH={0} overflow="hidden">
                  {dagView === "graph" ? (
                    <Graph />
                  ) : dagView === "gantt" && Boolean(runId) ? (
                    <SharedScrollBox scrollRef={sharedGridGanttScrollRef}>
                      <Flex alignItems="flex-start" gap={0} maxW="100%" minW={0} overflow="clip" w="100%">
                        <Grid
                          dagRunState={dagRunStateFilter}
                          limit={limit}
                          offset={offset}
                          onJumpToLatest={handleJumpToLatest}
                          runAfterGte={runAfterGte}
                          runAfterLte={runAfterLte}
                          runType={runTypeFilter}
                          setOffset={setOffset}
                          sharedScrollContainerRef={sharedGridGanttScrollRef}
                          showGantt
                          showVersionIndicatorMode={showVersionIndicatorMode}
                          triggeringUser={triggeringUserFilter}
                        />
                        <Gantt
                          dagRunState={dagRunStateFilter}
                          limit={limit}
                          offset={offset}
                          runAfterGte={runAfterGte}
                          runAfterLte={runAfterLte}
                          runType={runTypeFilter}
                          sharedScrollContainerRef={sharedGridGanttScrollRef}
                          triggeringUser={triggeringUserFilter}
                        />
                      </Flex>
                    </SharedScrollBox>
                  ) : (
                    <HStack
                      alignItems="flex-start"
                      gap={0}
                      height="100%"
                      maxW="100%"
                      minW={0}
                      overflow="hidden"
                      w="100%"
                    >
                      <Grid
                        dagRunState={dagRunStateFilter}
                        limit={limit}
                        offset={offset}
                        onJumpToLatest={handleJumpToLatest}
                        runAfterGte={runAfterGte}
                        runAfterLte={runAfterLte}
                        runType={runTypeFilter}
                        setOffset={setOffset}
                        showVersionIndicatorMode={showVersionIndicatorMode}
                        triggeringUser={triggeringUserFilter}
                      />
                    </HStack>
                  )}
                </Box>
              </Flex>
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
                        <Tooltip
                          content={`${translate("common:dagWarnings")} (${warningData?.total_entries ?? 0 + Number(error)})`}
                        >
                          <IconButton
                            aria-label={`${translate("common:dagWarnings")} (${warningData?.total_entries ?? 0 + Number(error)})`}
                            colorPalette={Boolean(error) ? "red" : "orange"}
                            margin="2"
                            marginBottom="-1"
                            onClick={onOpen}
                            rounded="full"
                            size="md"
                            variant="solid"
                          >
                            <LuFileWarning />
                          </IconButton>
                        </Tooltip>

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
