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
import { LinePath } from "@visx/shape";
import { MarkerArrow } from "@visx/marker";
import { useTheme } from "@chakra-ui/react";

interface Props {
  edge: {
    id: string;
    sources: string[];
    targets: string[];
    sections: Record<string, any>[];
  };
  isSelected: boolean;
}

const Edge = ({ edge, isSelected }: Props) => {
  const { colors } = useTheme();
  return (
    <>
      <MarkerArrow
        id="marker-arrow"
        fill={isSelected ? colors.blue[400] : colors.gray[400]}
        refX={6}
        size={6}
      />
      {edge.sections.map((s) => (
        <LinePath
          key={s.id}
          stroke={isSelected ? colors.blue[400] : colors.gray[400]}
          strokeWidth={isSelected ? 2 : 1}
          x={(d) => d.x || 0}
          y={(d) => d.y || 0}
          data={[s.startPoint, ...(s.bendPoints || []), s.endPoint]}
          markerEnd="url(#marker-arrow)"
        />
      ))}
    </>
  );
};

export default Edge;
