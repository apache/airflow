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

import React, { RefObject } from "react";
import { Box, Spinner } from "@chakra-ui/react";
import { Zoom } from "@visx/zoom";
import { Group } from "@visx/group";

import { useDatasetDependencies } from "src/api";

import Node from "./Node";
import Edge from "./Edge";
import Legend from "./Legend";

interface Props {
  onSelect: (datasetId: string) => void;
  selectedUri: string | null;
  height: number;
  width: number;
}

const Graph = ({ onSelect, selectedUri, height, width }: Props) => {
  const { data, isLoading } = useDatasetDependencies();

  if (isLoading && !data) return <Spinner />;
  if (!data || !data.fullGraph || !data.subGraphs) return null;
  const graph = selectedUri
    ? data.subGraphs.find((g) =>
        g.children.some((n) => n.id === `dataset:${selectedUri}`)
      )
    : data.fullGraph;
  if (!graph) return null;
  const { edges, children, width: graphWidth, height: graphHeight } = graph;

  const initialTransform = {
    scaleX: 1,
    scaleY: 1,
    translateX: 0,
    translateY: 0,
    skewX: 0,
    skewY: 0,
  };

  const selectedEdges = selectedUri
    ? edges?.filter(
        ({ sources, targets }) =>
          sources[0].includes(selectedUri) || targets[0].includes(selectedUri)
      )
    : [];
  const highlightedNodes = children.filter((n) =>
    selectedEdges.some(
      ({ sources, targets }) => sources[0] === n.id || targets[0] === n.id
    )
  );

  return (
    <Zoom
      width={width}
      height={height}
      scaleXMin={1 / 4}
      scaleXMax={1}
      scaleYMin={1 / 4}
      scaleYMax={1}
      initialTransformMatrix={initialTransform}
    >
      {(zoom) => (
        <Box>
          <svg
            id="GRAPH"
            width={width}
            height={height}
            ref={zoom.containerRef as RefObject<SVGSVGElement>}
            style={{
              cursor: zoom.isDragging ? "grabbing" : "grab",
              touchAction: "none",
            }}
          >
            <g transform={zoom.toString()}>
              <g height={graphHeight} width={graphWidth}>
                {edges.map((edge) => (
                  <Edge
                    key={edge.id}
                    edge={edge}
                    isSelected={selectedEdges.some((e) => e.id === edge.id)}
                  />
                ))}
                {children.map((node) => (
                  <Node
                    key={node.id}
                    node={node}
                    onSelect={onSelect}
                    isSelected={node.id === `dataset:${selectedUri}`}
                    isHighlighted={highlightedNodes.some(
                      (n) => n.id === node.id
                    )}
                  />
                ))}
              </g>
            </g>
            <Group top={height - 100} left={0} height={100} width={width}>
              <foreignObject width={150} height={100}>
                <Legend
                  zoom={zoom}
                  center={() =>
                    zoom.translateTo({
                      x: (width - (graphWidth ?? 0)) / 2,
                      y: (height - (graphHeight ?? 0)) / 2,
                    })
                  }
                />
              </foreignObject>
            </Group>
          </svg>
        </Box>
      )}
    </Zoom>
  );
};

export default Graph;
