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
import { Text, useToken } from "@chakra-ui/react";
import { Group } from "@visx/group";
import { LinePath } from "@visx/shape";
import type { Edge as EdgeType } from "@xyflow/react";
import type { ElkPoint } from "elkjs";

import type { EdgeData } from "./reactflowUtils";

type Props = EdgeType<EdgeData>;

const CustomEdge = ({ data }: Props) => {
  const [strokeColor, blueColor] = useToken("colors", ["border.inverted", "blue.500"]);

  if (data === undefined) {
    return undefined;
  }
  const { rest } = data;

  return (
    <>
      {rest.labels?.map(({ height, id, text, width, x, y }) => {
        if (y === undefined || x === undefined) {
          return undefined;
        }

        return (
          <Group
            // Add a tiny bit of height so letters aren't cut off
            height={(height ?? 0) + 2}
            key={id}
            left={x}
            top={y}
            width={width}
          >
            <foreignObject height={(height ?? 0) + 2} width={width}>
              <Text>{text}</Text>
            </foreignObject>
          </Group>
        );
      })}
      {(rest.sections ?? []).map((section) => (
        <LinePath
          data={[section.startPoint, ...(section.bendPoints ?? []), section.endPoint]}
          key={section.id}
          stroke={rest.isSelected ? blueColor : strokeColor}
          strokeDasharray={rest.isSetupTeardown ? "10,5" : undefined}
          strokeWidth={rest.isSelected ? 3 : 1}
          x={(point: ElkPoint) => point.x}
          y={(point: ElkPoint) => point.y}
        />
      ))}
    </>
  );
};

export default CustomEdge;
