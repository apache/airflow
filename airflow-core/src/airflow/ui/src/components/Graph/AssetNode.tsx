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
import { Flex, Heading, HStack, LinkOverlay, Text } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";
import { FiDatabase } from "react-icons/fi";
import { useParams, Link as RouterLink } from "react-router-dom";

import { useAssetServiceGetAssetEvents, useDagRunServiceGetUpstreamAssetEvents } from "openapi/queries";
import { pluralize } from "src/utils";

import Time from "../Time";
import { NodeWrapper } from "./NodeWrapper";
import type { CustomNodeProps } from "./reactflowUtils";

export const AssetNode = ({
  data: { height, id, isSelected, label, width },
}: NodeProps<NodeType<CustomNodeProps, "asset">>) => {
  const { dagId = "", runId = "" } = useParams();
  const { data: upstreamEventsData } = useDagRunServiceGetUpstreamAssetEvents(
    { dagId, dagRunId: runId },
    undefined,
    { enabled: Boolean(dagId) && Boolean(runId) },
  );

  const { data: downstreamEventsData } = useAssetServiceGetAssetEvents(
    { sourceDagId: dagId, sourceRunId: runId },
    undefined,
    { enabled: Boolean(dagId) && Boolean(runId) },
  );

  const assetEvent = [
    ...(upstreamEventsData?.asset_events ?? []),
    ...(downstreamEventsData?.asset_events ?? []),
  ].find((event) => event.name === label);

  const assetId = id.replace("asset:", "");

  return (
    <NodeWrapper>
      <Flex
        bg="bg"
        borderColor={isSelected ? "border.inverted" : "border"}
        borderRadius={5}
        borderWidth={isSelected ? 4 : 2}
        cursor="default"
        flexDirection="column"
        height={`${height}px`}
        px={3}
        py={isSelected ? 0 : 1}
        width={`${width}px`}
      >
        <HStack>
          <Heading ml={-2} size="sm">
            <FiDatabase />
          </Heading>
          <LinkOverlay asChild>
            <RouterLink to={`/assets/${assetId}`}>{label}</RouterLink>
          </LinkOverlay>
        </HStack>
        {assetEvent === undefined ? undefined : (
          <>
            <Text color="fg.muted">
              <Time datetime={assetEvent.timestamp} />
            </Text>
            {assetEvent.created_dagruns.length && assetEvent.created_dagruns.length > 1 ? (
              <Text color="fg.muted" fontSize="sm">
                +{pluralize("other Dag Run", assetEvent.created_dagruns.length)}
              </Text>
            ) : undefined}
          </>
        )}
      </Flex>
    </NodeWrapper>
  );
};
