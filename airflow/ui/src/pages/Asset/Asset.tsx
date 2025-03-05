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
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";
import { useParams } from "react-router-dom";

import { useAssetServiceGetAsset } from "openapi/queries";
import { BreadcrumbStats } from "src/components/BreadcrumbStats";
import { ProgressBar } from "src/components/ui";

import { AssetEvents } from "../AssetEvents";
import { AssetGraph } from "./AssetGraph";
import { Header } from "./Header";

export const Asset = () => {
  const { assetId } = useParams();

  const { data: asset, isLoading } = useAssetServiceGetAsset(
    { assetId: assetId === undefined ? 0 : parseInt(assetId, 10) },
    undefined,
    {
      enabled: Boolean(assetId),
    },
  );

  const links = [
    { label: "Assets", value: "/assets" },
    {
      label: asset?.name,
      title: "Asset",
      value: `/assets/${assetId}`,
    },
  ];

  return (
    <ReactFlowProvider>
      <HStack justifyContent="space-between">
        <BreadcrumbStats links={links} />
      </HStack>
      <ProgressBar size="xs" visibility={Boolean(isLoading) ? "visible" : "hidden"} />
      <Box flex={1} minH={0}>
        <PanelGroup autoSaveId={assetId} direction="horizontal">
          <Panel defaultSize={20} minSize={6}>
            <Box height="100%" position="relative" pr={2}>
              <AssetGraph asset={asset} />
            </Box>
          </Panel>
          <PanelResizeHandle className="resize-handle">
            <Box bg="fg.subtle" cursor="col-resize" h="100%" transition="background 0.2s" w={0.5} />
          </PanelResizeHandle>
          <Panel defaultSize={50} minSize={20}>
            <Header asset={asset} />
            <Box h="100%" overflow="auto" px={2}>
              <AssetEvents />
            </Box>
          </Panel>
        </PanelGroup>
      </Box>
    </ReactFlowProvider>
  );
};
