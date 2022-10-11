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

import React, { useState, useEffect, RefObject } from 'react';
import { Box, Spinner } from '@chakra-ui/react';
import { Zoom } from '@visx/zoom';
import { Group } from '@visx/group';
import { debounce } from 'lodash';

import { useDatasetDependencies } from 'src/api';

import type { ElkExtendedEdge } from 'elkjs';

import Node from './Node';
import Edge from './Edge';
import Legend from './Legend';

interface Props {
  onSelect: (datasetId: string) => void;
  selectedUri: string | null;
}

interface Connection {
  nodeId: string;
  edgeId: string;
}

const getConnectedNodes = (edges: ElkExtendedEdge[], node: string, direction: 'upstream' | 'downstream') => {
  const nodes: Connection[] = [];
  edges.forEach((e) => {
    if (direction === 'downstream' && e.sources[0] === node) {
      nodes.push({ nodeId: e.targets[0], edgeId: e.id });
      const distantNodes = getConnectedNodes(edges, e.targets[0], direction);
      nodes.push(...distantNodes);
    } else if (direction === 'upstream' && e.targets[0] === node) {
      nodes.push({ nodeId: e.sources[0], edgeId: e.id });
      const distantNodes = getConnectedNodes(edges, e.sources[0], direction);
      nodes.push(...distantNodes);
    }
  });
  return nodes;
};

const Graph = ({ onSelect, selectedUri }: Props) => {
  const { data, isLoading } = useDatasetDependencies();
  const [dimensions, setDimensions] = useState({
    height: window.innerHeight,
    width: window.innerWidth,
  });

  // Reset the graph div when the window size changes
  useEffect(() => {
    const handleResize = debounce(() => {
      setDimensions({
        height: window.innerHeight,
        width: window.innerWidth,
      });
    }, 200);

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  });

  if (isLoading && !data) return <Spinner />;
  if (!data) return null;

  const initialTransform = {
    scaleX: 1,
    scaleY: 1,
    translateX: 0,
    translateY: 0,
    skewX: 0,
    skewY: 0,
  };

  let connectedNodes: Connection[] = [];
  if (selectedUri) {
    const upstream = getConnectedNodes(data.edges, `dataset:${selectedUri}`, 'upstream');
    const downstream = getConnectedNodes(data.edges, `dataset:${selectedUri}`, 'downstream');
    connectedNodes = [...upstream, ...downstream];
  }

  const width = dimensions.width - 600 || 200;
  const height = dimensions.height - 125 || 200;

  return (
    <Box position="relative" alignSelf="center" borderColor="gray.200" borderWidth={1}>
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
              style={{ cursor: zoom.isDragging ? 'grabbing' : 'grab', touchAction: 'none' }}
            >
              <g transform={zoom.toString()}>
                <g height={data.height} width={data.width}>
                  {data.edges.map((edge) => (
                    <Edge
                      key={edge.id}
                      edge={edge}
                      isSelected={connectedNodes.some((e) => e.edgeId === edge.id)}
                    />
                  ))}
                  {data.children.map((node) => (
                    <Node
                      key={node.id}
                      node={node}
                      onSelect={onSelect}
                      isSelected={node.id === `dataset:${selectedUri}`}
                      isHighlighted={connectedNodes.some((n) => n.nodeId === node.id)}
                    />
                  ))}
                </g>
              </g>
              <Group top={height - 50} left={0} height={50} width={width}>
                <foreignObject width={width} height={50}>
                  <Legend
                    zoom={zoom}
                    center={() => zoom.translateTo({
                      x: (width - (data.width ?? 0)) / 2,
                      y: (height - (data.height ?? 0)) / 2,
                    })}
                  />
                </foreignObject>
              </Group>
            </svg>
          </Box>
        )}
      </Zoom>
    </Box>
  );
};

export default Graph;
