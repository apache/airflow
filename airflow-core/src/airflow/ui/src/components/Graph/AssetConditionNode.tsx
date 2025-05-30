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
import { Box } from "@chakra-ui/react";
import type { NodeProps, Node as NodeType } from "@xyflow/react";
import { useTranslation } from "react-i18next";
import { TbLogicAnd, TbLogicOr } from "react-icons/tb";

import { NodeWrapper } from "./NodeWrapper";
import type { CustomNodeProps } from "./reactflowUtils";

export const AssetConditionNode = ({ data }: NodeProps<NodeType<CustomNodeProps, "asset-condition">>) => {
  const { t: translate } = useTranslation("common");

  return (
    <NodeWrapper>
      <Box
        bg="bg"
        borderRadius={4}
        borderWidth={1}
        height={`${data.height}px`}
        title={data.assetCondition === "and-gate" ? translate("expression.all") : translate("expression.any")}
        width={`${data.width}px`}
      >
        {data.assetCondition === "or-gate" ? (
          <TbLogicOr size={`${data.height}px`} />
        ) : (
          <TbLogicAnd size={`${data.height}px`} />
        )}
      </Box>
    </NodeWrapper>
  );
};
