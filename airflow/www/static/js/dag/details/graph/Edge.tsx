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

import React from "react";
import { Text } from "@chakra-ui/react";
import type { ElkEdgeSection, ElkLabel, ElkPoint, LayoutOptions } from "elkjs";

import Edge from "src/datasets/Graph/Edge";
import { Group } from "@visx/group";

interface EdgeProps {
  data?: {
    rest: {
      isSelected: boolean;
      sources: string[];
      targets: string[];
      sections: ElkEdgeSection[];
      junctionPoints?: ElkPoint[];
      id: string;
      labels?: ElkLabel[];
      layoutOptions?: LayoutOptions;
    };
  };
}

const CustomEdge = ({ data }: EdgeProps) => {
  if (!data) return null;
  const { rest } = data;
  return (
    <>
      {rest?.labels?.map(({ id, x, y, text, width, height }) => {
        if (!y || !x) return null;
        return (
          <Group top={y} left={x} height={height} width={width} key={id}>
            <foreignObject width={width} height={height}>
              <Text>{text}</Text>
            </foreignObject>
          </Group>
        );
      })}
      <Edge
        edge={rest}
        showMarker={false}
        isSelected={rest.isSelected || undefined}
      />
    </>
  );
};

export default CustomEdge;
