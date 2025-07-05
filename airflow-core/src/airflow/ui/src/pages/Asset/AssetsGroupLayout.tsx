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
import { useMemo } from "react";
import { PanelGroup, Panel, PanelResizeHandle } from "react-resizable-panels";
import { useParams } from "react-router-dom";

import { useAssetServiceGetAssetGroups } from "openapi/queries";
import type { AssetResponse } from "openapi/requests/types.gen";
import { BreadcrumbStats } from "src/components/BreadcrumbStats";
import { ProgressBar } from "src/components/ui";

import { AssetGroupSidebar } from "./AssetGroupSidebar";
import { AssetsGroupGraph } from "./AssetsGroupGraph";

export const AssetGroupLayout = () => {
  const { groupName } = useParams<{ groupName: string }>();

  const orderBy = undefined;

  const { data, isLoading } = useAssetServiceGetAssetGroups({
    group: groupName,
    orderBy,
  });

  const assets: Array<AssetResponse> = useMemo(() => data?.groups[0]?.assets ?? [], [data]);

  const assetIds: Array<string> = useMemo(() => assets.map((asset) => String(asset.id)), [assets]);

  const safeGroupName = groupName ?? "";

  const links = [
    {
      label: safeGroupName,
      title: "Asset Group",
      value: `/assets/group/${safeGroupName}`,
    },
  ];

  return (
    <>
      <HStack justifyContent="space-between" mb={2}>
        <BreadcrumbStats links={links} />
      </HStack>
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      <Box flex={1} minH={0}>
        <PanelGroup autoSaveId={safeGroupName} direction="horizontal">
          <Panel defaultSize={70} minSize={6}>
            <Box height="100%" position="relative" pr={2}>
              <AssetsGroupGraph assetIds={assetIds} groupName={safeGroupName} />
            </Box>
          </Panel>
          <PanelResizeHandle className="resize-handle" />
          <Panel defaultSize={30} minSize={20}>
            <Box h="100%" overflow="auto" pt={2}>
              <AssetGroupSidebar assets={assets} groupName={safeGroupName} />
            </Box>
          </Panel>
        </PanelGroup>
      </Box>
    </>
  );
};
