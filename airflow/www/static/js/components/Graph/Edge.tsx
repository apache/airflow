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
import { Text, useTheme } from "@chakra-ui/react";

import { Group } from "@visx/group";
import { LinePath } from "@visx/shape";
import type { EdgeData } from "src/types";

export interface EdgeProps {
  data?: EdgeData;
}

const CustomEdge = ({ data }: EdgeProps) => {
  const { colors } = useTheme();
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
      {(rest.sections || []).map((s) => (
        <LinePath
          key={s.id}
          stroke={rest.isSelected ? colors.blue[400] : colors.gray[400]}
          strokeWidth={rest.isSelected ? 3 : 2}
          x={(d) => d.x || 0}
          y={(d) => d.y || 0}
          data={[s.startPoint, ...(s.bendPoints || []), s.endPoint]}
          strokeDasharray={rest.isSetupTeardown ? "10,5" : undefined}
        />
      ))}
    </>
  );
};

export default CustomEdge;
