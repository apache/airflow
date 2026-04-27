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
import { HStack, Box, Text, Code, Button, VStack } from "@chakra-ui/react";
import { useReactFlow } from "@xyflow/react";
import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { PanelGroup, Panel, PanelResizeHandle } from "react-resizable-panels";
import { useParams, useSearchParams } from "react-router-dom";

import { AssetEvents } from "src/components/Assets/AssetEvents";
import { BreadcrumbStats } from "src/components/BreadcrumbStats";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchBar } from "src/components/SearchBar";
import { ProgressBar } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { OpenGroupsProvider } from "src/context/openGroups";
import { useAssetLineage } from "src/queries/useAssetLineage";
import { useAssetDetailData, useAssetEventsData } from "src/queries/useMockAssetData";

import { AssetGraph } from "./AssetGraph";
import { AssetInsightsPanel } from "./AssetInsightsPanel";
import { AssetLineageGraph } from "./AssetLineageGraph";
import { AssetPanelButtons } from "./AssetPanelButtons";
import { CreateAssetEvent } from "./CreateAssetEvent";
import { Header } from "./Header";
import type { LineageDirection } from "./lineageHighlightUtils";

type LineageMode = "asset_only" | "full";

export const AssetLayout = () => {
  const { i18n, t: translate } = useTranslation(["assets", "common"]);
  const { assetId } = useParams();
  const direction = i18n.dir();
  const useMockAssets = new URLSearchParams(globalThis.location.search).get("mockAssets") === "true";
  const [dependencyType, setDependencyType] = useState<"data" | "lineage" | "scheduling">(
    useMockAssets ? "lineage" : "scheduling",
  );
  const [activeNodeId, setActiveNodeId] = useState<string | undefined>(
    assetId === undefined ? undefined : `asset:${assetId}`,
  );
  const [lineageSearch, setLineageSearch] = useState("");
  const [lineageDirection, setLineageDirection] = useState<LineageDirection>("downstream");
  const [lineageMode, setLineageMode] = useState<LineageMode>("full");

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-timestamp"];

  const { data: asset, isLoading } = useAssetDetailData(
    assetId === undefined ? undefined : parseInt(assetId, 10),
  );
  const {
    data: lineageData = { edges: [], nodes: [] },
    error: lineageError,
    isError: isLineageError,
    isLoading: isLineageLoading,
  } = useAssetLineage(assetId, { mode: lineageMode });

  const links = [
    {
      label: asset?.name,
      title: translate("common:asset_one"),
      value: `/assets/${assetId}`,
    },
  ];

  const { DAG_ID, END_DATE, START_DATE, TASK_ID } = SearchParamsKeys;
  const [searchParams] = useSearchParams();
  const { data, isLoading: isLoadingEvents } = useAssetEventsData({
    assetId: asset?.id,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
    sourceDagId: searchParams.get(DAG_ID) ?? undefined,
    sourceTaskId: searchParams.get(TASK_ID) ?? undefined,
    timestampGte: searchParams.get(START_DATE) ?? undefined,
    timestampLte: searchParams.get(END_DATE) ?? undefined,
  });

  const setOrderBy = (value: string) => {
    setTableURLState({
      pagination,
      sorting: [
        {
          desc: value.startsWith("-"),
          id: value.replace("-", ""),
        },
      ],
    });
  };

  const { fitView, getZoom } = useReactFlow();

  useEffect(() => {
    setActiveNodeId(assetId === undefined ? undefined : `asset:${assetId}`);
  }, [assetId]);

  const lineageNodesById = useMemo(
    () => new Map(lineageData.nodes.map((node) => [node.id, node])),
    [lineageData.nodes],
  );
  const activeAssetColumnLineage = useMemo(
    () =>
      lineageMode !== "asset_only" || activeNodeId === undefined
        ? []
        : lineageData.edges.filter((edge) => edge.target_id === activeNodeId && edge.column_lineage),
    [activeNodeId, lineageData.edges, lineageMode],
  );

  return (
    <>
      <HStack justifyContent="space-between" mb={2}>
        <BreadcrumbStats links={links} />
        <CreateAssetEvent asset={asset} />
      </HStack>
      <ProgressBar size="xs" visibility={Boolean(isLoading) ? "visible" : "hidden"} />
      <Box flex={1} minH={0}>
        <PanelGroup
          autoSaveId={`asset-${direction}`}
          dir={direction}
          direction="horizontal"
          key={`asset-${direction}`}
        >
          <Panel defaultSize={70} minSize={6}>
            <Box height="100%" position="relative" pr={2}>
              <AssetPanelButtons dependencyType={dependencyType} setDependencyType={setDependencyType} />
              {dependencyType === "lineage" ? (
                <Box left={3} position="absolute" right={3} top={14} zIndex={5}>
                  <HStack>
                    <Box flex={1}>
                      <SearchBar
                        defaultValue={lineageSearch}
                        hotkeyDisabled
                        onChange={setLineageSearch}
                        placeholder="Search lineage nodes"
                      />
                    </Box>
                    <Button
                      colorPalette={lineageMode === "full" ? "blue" : "gray"}
                      onClick={() => {
                        setLineageMode("full");
                      }}
                      size="sm"
                      variant={lineageMode === "full" ? "solid" : "outline"}
                    >
                      {translate("assets:lineage_full", { defaultValue: "Full" })}
                    </Button>
                    <Button
                      colorPalette={lineageMode === "asset_only" ? "blue" : "gray"}
                      onClick={() => {
                        setLineageMode("asset_only");
                      }}
                      size="sm"
                      variant={lineageMode === "asset_only" ? "solid" : "outline"}
                    >
                      {translate("assets:lineage_asset_only", { defaultValue: "Asset Only" })}
                    </Button>
                    <Button
                      colorPalette={lineageDirection === "upstream" ? "blue" : "gray"}
                      onClick={() => {
                        setLineageDirection("upstream");
                      }}
                      size="sm"
                      variant={lineageDirection === "upstream" ? "solid" : "outline"}
                    >
                      {translate("assets:lineage_upstream", { defaultValue: "Upstream" })}
                    </Button>
                    <Button
                      colorPalette={lineageDirection === "downstream" ? "blue" : "gray"}
                      onClick={() => {
                        setLineageDirection("downstream");
                      }}
                      size="sm"
                      variant={lineageDirection === "downstream" ? "solid" : "outline"}
                    >
                      {translate("assets:lineage_downstream", { defaultValue: "Downstream" })}
                    </Button>
                  </HStack>
                </Box>
              ) : undefined}
              <OpenGroupsProvider dagId="~">
                {dependencyType === "lineage" ? (
                  <AssetLineageGraph
                    activeNodeId={activeNodeId}
                    asset={asset}
                    error={lineageError}
                    highlightDirection={lineageDirection}
                    isError={isLineageError}
                    isLoading={isLineageLoading}
                    lineageData={lineageData}
                    searchTerm={lineageSearch}
                    setActiveNodeId={setActiveNodeId}
                  />
                ) : (
                  <AssetGraph asset={asset} dependencyType={dependencyType} />
                )}
              </OpenGroupsProvider>
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
          <Panel defaultSize={30} minSize={20}>
            <Box display="flex" flexDirection="column" h="100%" minH={0}>
              <Header asset={asset} />
              <Box flex={1} minH={0} overflow="auto">
                <AssetInsightsPanel asset={asset} />
                {lineageMode === "asset_only" && activeAssetColumnLineage.length > 0 ? (
                  <Box mb={3} mt={3} px={3}>
                    <Text fontWeight="bold" mb={2}>
                      {translate("assets:column_lineage", { defaultValue: "Column Lineage" })}
                    </Text>
                    <VStack align="stretch" gap={2}>
                      {activeAssetColumnLineage.map((edge) => {
                        const sourceName = lineageNodesById.get(edge.source_id)?.name ?? edge.source_id;
                        const targetName = lineageNodesById.get(edge.target_id)?.name ?? edge.target_id;

                        return (
                          <Box key={`${edge.source_id}-${edge.target_id}`}>
                            <Text fontWeight="medium" mb={1}>
                              {`${sourceName} -> ${targetName}`}
                            </Text>
                            <VStack align="stretch" gap={1} pl={3}>
                              {Object.entries(edge.column_lineage ?? {}).map(([targetColumn, sources]) =>
                                sources.map((source) => (
                                  <Text
                                    key={`${edge.source_id}-${targetColumn}-${source.source_asset_uri}-${source.source_column}`}
                                  >
                                    {`${targetName}.${targetColumn} <- ${sourceName}.${source.source_column}`}
                                  </Text>
                                )),
                              )}
                            </VStack>
                          </Box>
                        );
                      })}
                    </VStack>
                  </Box>
                ) : undefined}
                {asset?.extra && Object.keys(asset.extra).length > 0 ? (
                  <Box mb={3} mt={3} px={3}>
                    <Text fontWeight="bold" mb={2}>
                      {translate("assets:additional_data")}
                    </Text>
                    <Code
                      background="bg.subtle"
                      borderRadius="md"
                      color="fg.default"
                      display="block"
                      fontSize="sm"
                      p={2}
                      w="full"
                      whiteSpace="pre"
                    >
                      {JSON.stringify(asset.extra, undefined, 2)}
                    </Code>
                  </Box>
                ) : undefined}

                <Box pt={2}>
                  <AssetEvents
                    assetId={asset?.id}
                    data={data}
                    isLoading={isLoadingEvents}
                    setOrderBy={setOrderBy}
                    setTableUrlState={setTableURLState}
                    showFilters={true}
                    tableUrlState={tableURLState}
                  />
                </Box>
              </Box>
            </Box>
          </Panel>
        </PanelGroup>
      </Box>
    </>
  );
};
