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
import { Box, HStack } from "@chakra-ui/react";
import { ReactFlowProvider } from "@xyflow/react";
import { useCallback } from "react";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import { useParams } from "react-router-dom";

import { useAssetServiceGetAsset, useAssetServiceGetAssetEvents } from "openapi/queries";
import { AssetEvents } from "src/components/Assets/AssetEvents";
import { BreadcrumbStats } from "src/components/BreadcrumbStats";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ProgressBar, Toaster } from "src/components/ui";

import { AssetGraph } from "./AssetGraph";
import { CreateAssetEvent } from "./CreateAssetEvent";
import { Header } from "./Header";

export const Asset = () => {
  const { assetId } = useParams();

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-timestamp";

  const { data: asset, isLoading } = useAssetServiceGetAsset(
    { assetId: assetId === undefined ? 0 : parseInt(assetId, 10) },
    undefined,
    {
      enabled: Boolean(assetId),
    },
  );

  const links = [
    {
      label: asset?.name,
      title: "Asset",
      value: `/assets/${assetId}`,
    },
  ];

  const { data, isLoading: isLoadingEvents } = useAssetServiceGetAssetEvents(
    {
      assetId: asset?.id,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
    },
    undefined,
    { enabled: Boolean(asset?.id) },
  );

  const setOrderBy = useCallback(
    (value: string) => {
      setTableURLState({
        pagination,
        sorting: [
          {
            desc: value.startsWith("-"),
            id: value.replace("-", ""),
          },
        ],
      });
    },
    [pagination, setTableURLState],
  );

  return (
    <ReactFlowProvider>
      <Toaster />
      <HStack justifyContent="space-between" mb={2}>
        <BreadcrumbStats links={links} />
        <CreateAssetEvent asset={asset} />
      </HStack>
      <ProgressBar size="xs" visibility={Boolean(isLoading) ? "visible" : "hidden"} />
      <Box flex={1} minH={0}>
        <PanelGroup autoSaveId={assetId} direction="horizontal">
          <Panel defaultSize={70} minSize={6}>
            <Box height="100%" position="relative" pr={2}>
              <AssetGraph asset={asset} />
            </Box>
          </Panel>
          <PanelResizeHandle className="resize-handle">
            <Box bg="fg.subtle" cursor="col-resize" h="100%" transition="background 0.2s" w={0.5} />
          </PanelResizeHandle>
          <Panel defaultSize={30} minSize={20}>
            <Header asset={asset} />
            <Box h="100%" overflow="auto" pt={2}>
              <AssetEvents
                assetId={asset?.id}
                data={data}
                isLoading={isLoadingEvents}
                setOrderBy={setOrderBy}
                setTableUrlState={setTableURLState}
                tableUrlState={tableURLState}
              />
            </Box>
          </Panel>
        </PanelGroup>
      </Box>
    </ReactFlowProvider>
  );
};
