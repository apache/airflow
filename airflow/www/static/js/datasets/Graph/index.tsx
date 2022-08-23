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
import { Box, IconButton, Spinner } from '@chakra-ui/react';
import { Zoom } from '@visx/zoom';
import { MdOutlineZoomOutMap, MdFilterCenterFocus } from 'react-icons/md';
import { debounce } from 'lodash';

import { useDatasetDependencies } from 'src/api';

import Node from './Node';
import Edge from './Edge';

interface Props {
  onSelect: (datasetId: string) => void;
  selectedUri: string | null;
}

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

  const selectedEdges = selectedUri
    ? data.edges?.filter(({ sources, targets }) => (
      sources[0].includes(selectedUri) || targets[0].includes(selectedUri)))
    : [];
  const highlightedNodes = data?.children
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
              <IconButton
                onClick={() => zoom.translateTo({
                  x: (width - (data.width ?? 0)) / 2,
                  y: (height - (data.height ?? 0)) / 2,
                })}
                fontSize="2xl"
                m={2}
                title="Center"
                aria-label="Center"
                icon={<MdFilterCenterFocus />}
              />
            </Box>
          </Box>
        )}
      </Zoom>
    </Box>
  );
};

export default Graph;
