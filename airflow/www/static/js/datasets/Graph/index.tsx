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

/* global depNodes, depEdges */

import React, { useState, useEffect, RefObject } from 'react';
import { Box, IconButton } from '@chakra-ui/react';
import ELK, { ElkExtendedEdge, ElkShape } from 'elkjs';
import { Zoom } from '@visx/zoom';
import { MdOutlineZoomOutMap } from 'react-icons/md';
import { debounce } from 'lodash';

import type { DepNode, DepEdge } from 'src/types';

import Node, { NodeType } from './Node';
import Edge from './Edge';

interface GenerateProps {
  nodes: DepNode[];
  edges: DepEdge[];
  font: string;
}

// Take text and font to calculate how long each node should be
function getTextWidth(text: string, font: string) {
  const context = document.createElement('canvas').getContext('2d');
  if (context) {
    context.font = font;
    const metrics = context.measureText(text);
    return metrics.width;
  }
  return text.length * 9;
}

const generateGraph = ({ nodes, edges, font }: GenerateProps) => ({
  id: 'root',
  layoutOptions: {
    'spacing.nodeNodeBetweenLayers': '40.0',
    'spacing.edgeNodeBetweenLayers': '10.0',
    'layering.strategy': 'INTERACTIVE',
    algorithm: 'layered',
    'spacing.edgeEdgeBetweenLayers': '10.0',
    'spacing.edgeNode': '10.0',
    'spacing.edgeEdge': '10.0',
    'spacing.nodeNode': '20.0',
    'elk.direction': 'DOWN',
  },
  children: nodes.map(({ id, value }) => ({
    id,
    // calculate text width and add space for padding/icon
    width: getTextWidth(value.label, font) + 36,
    height: 40,
    value,
  })),
  edges: edges.map((e) => ({ id: `${e.u}-${e.v}`, sources: [e.u], targets: [e.v] })),
});

const edges = depEdges.filter((e) => {
  const edgeNodes = depNodes.filter((n) => n.id === e.u || n.id === e.v);
  return edgeNodes.length === 2;
});

const nodes = depNodes.filter((n) => edges.some((e) => e.u === n.id || e.v === n.id));

interface Props {
  onSelect: (datasetId: string) => void;
  selectedUri: string | null;
}

interface Data extends ElkShape {
  children: NodeType[];
  edges: ElkExtendedEdge[];
}

const Graph = ({ onSelect, selectedUri }: Props) => {
  // get computed style to calculate how large each node should be
  const font = `bold ${16}px ${window.getComputedStyle(document.body).fontFamily}`;

  const elk = new ELK();
  const [data, setData] = useState<Data | undefined>();
  const [dimensions, setDimensions] = useState({
    height: window.innerHeight,
    width: window.innerWidth,
  });

  useEffect(() => {
    if (edges.length && nodes.length) {
      elk.layout(generateGraph({ nodes, edges, font }))
        .then((g) => setData(g as Data))
        .catch(console.error);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodes, edges]);

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

  if (!data) return null;

  const initialTransform = {
    scaleX: 1,
    scaleY: 1,
    translateX: 0,
    translateY: 0,
    skewX: 0,
    skewY: 0,
  };

  const selectedEdges = selectedUri
    ? data?.edges?.filter(({ sources, targets }) => (
      sources[0].includes(selectedUri) || targets[0].includes(selectedUri)))
    : [];
  const highlightedNodes = data.children
    .filter((n) => (
      selectedEdges.some(({ sources, targets }) => (
        sources[0] === n.id || targets[0] === n.id))));

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
                      isSelected={selectedEdges.some((e) => e.id === edge.id)}
                    />
                  ))}
                  {data.children.map((node) => (
                    <Node
                      key={node.id}
                      node={node}
                      onSelect={onSelect}
                      isSelected={node.id === `dataset:${selectedUri}`}
                      isHighlighted={highlightedNodes.some((n) => n.id === node.id)}
                    />
                  ))}
                </g>
              </g>
            </svg>
            <Box>
              <IconButton
                onClick={zoom.reset}
                fontSize="2xl"
                m={2}
                title="Reset zoom"
                aria-label="Reset zoom"
                icon={<MdOutlineZoomOutMap />}
              />
            </Box>
          </Box>
        )}
      </Zoom>
    </Box>
  );
};

export default Graph;
