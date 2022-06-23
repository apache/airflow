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

/*
  global d3, localStorage, dagreD3, dagNodes, edges, arrange, document,
*/

const highlightColor = '#000000';
const upstreamColor = '#2020A0';
const downstreamColor = '#0000FF';
const initialStrokeWidth = '3px';
const highlightStrokeWidth = '5px';
const duration = 500;

let nodes = dagNodes;
const fullNodes = nodes;
const filteredNodes = nodes.filter((n) => edges.some((e) => e.u === n.id || e.v === n.id));

// Preparation of DagreD3 data structures
let g = new dagreD3.graphlib.Graph()
  .setGraph({
    nodesep: 15,
    ranksep: 15,
    rankdir: arrange,
  })
  .setDefaultEdgeLabel(() => ({ lineInterpolate: 'basis' }));

const render = dagreD3.render();
const svg = d3.select('#graph-svg');
const innerSvg = d3.select('#graph-svg g');

// Returns true if a node's id or its children's id matches search_text
function nodeMatches(nodeId, searchText) {
  if (nodeId.indexOf(searchText) > -1) return true;
  return false;
}

function highlightNodes(nodesToHighlight, color, strokeWidth) {
  nodesToHighlight.forEach((nodeid) => {
    const myNode = g.node(nodeid).elem;
    d3.select(myNode)
      .selectAll('rect,circle')
      .style('stroke', color)
      .style('stroke-width', strokeWidth);
  });
}

let zoom = null;

function setUpZoomSupport() {
  // Set up zoom support for Graph
  zoom = d3.behavior.zoom().on('zoom', () => {
    innerSvg.attr('transform', `translate(${d3.event.translate})scale(${d3.event.scale})`);
  });
  svg.call(zoom);

  // Centering the DAG on load
  // Get Dagre Graph dimensions
  const graphWidth = g.graph().width;
  const graphHeight = g.graph().height;
  // Get SVG dimensions
  const padding = 20;
  const svgBb = svg.node().getBoundingClientRect();
  const width = svgBb.width - padding * 2;
  const height = svgBb.height - padding; // we are not centering the dag vertically

  // Calculate applicable scale for zoom
  const zoomScale = Math.min(
    Math.min(width / graphWidth, height / graphHeight),
    1.5, // cap zoom level to 1.5 so nodes are not too large
  );

  zoom.translate([(width / 2) - ((graphWidth * zoomScale) / 2) + padding, padding]);
  zoom.scale(zoomScale);
  zoom.event(innerSvg);
}

function setUpNodeHighlighting(focusItem = null) {
  d3.selectAll('g.node').on('mouseover', function (d) {
    d3.select(this).selectAll('rect').style('stroke', highlightColor);
    highlightNodes(g.predecessors(d), upstreamColor, highlightStrokeWidth);
    highlightNodes(g.successors(d), downstreamColor, highlightStrokeWidth);
    const adjacentNodeNames = [d, ...g.predecessors(d), ...g.successors(d)];
    d3.selectAll('g.nodes g.node')
      .filter((x) => !adjacentNodeNames.includes(x))
      .style('opacity', 0.2);
    const adjacentEdges = g.nodeEdges(d);
    d3.selectAll('g.edgePath')[0]
      // eslint-disable-next-line no-underscore-dangle
      .filter((x) => !adjacentEdges.includes(x.__data__))
      .forEach((x) => {
        d3.select(x).style('opacity', 0.2);
      });
  });

  d3.selectAll('g.node').on('mouseout', function (d) {
    d3.select(this).selectAll('rect,circle').style('stroke', null);
    highlightNodes(g.predecessors(d), null, initialStrokeWidth);
    highlightNodes(g.successors(d), null, initialStrokeWidth);
    d3.selectAll('g.node')
      .style('opacity', 1);
    d3.selectAll('g.node rect')
      .style('stroke-width', initialStrokeWidth);
    d3.selectAll('g.edgePath')
      .style('opacity', 1);
    if (focusItem) {
      localStorage.removeItem(focusItem);
    }
  });
}

function searchboxHighlighting(s) {
  let match = null;

  d3.selectAll('g.nodes g.node').filter(function forEach(d) {
    if (s === '') {
      d3.select('g.edgePaths')
        .transition().duration(duration)
        .style('opacity', 1);
      d3.select(this)
        .transition().duration(duration)
        .style('opacity', 1)
        .selectAll('rect')
        .style('stroke-width', initialStrokeWidth);
    } else {
      d3.select('g.edgePaths')
        .transition().duration(duration)
        .style('opacity', 0.2);
      if (nodeMatches(d, s)) {
        if (!match) match = this;
        d3.select(this)
          .transition().duration(duration)
          .style('opacity', 1)
          .selectAll('rect')
          .style('stroke-width', highlightStrokeWidth);
      } else {
        d3.select(this)
          .transition()
          .style('opacity', 0.2).duration(duration)
          .selectAll('rect')
          .style('stroke-width', initialStrokeWidth);
      }
    }
    return null;
  });

  // This moves the matched node to the center of the graph area
  if (match) {
    const transform = d3.transform(d3.select(match).attr('transform'));

    const svgBb = svg.node().getBoundingClientRect();
    transform.translate = [
      svgBb.width / 2 - transform.translate[0],
      svgBb.height / 2 - transform.translate[1],
    ];
    transform.scale = [1, 1];

    if (zoom !== null) {
      zoom.translate(transform.translate);
      zoom.scale(1);
      zoom.event(innerSvg);
    }
  }
}

d3.select('#searchbox').on('keyup', () => {
  const s = document.getElementById('searchbox').value;
  searchboxHighlighting(s);
});

const renderGraph = () => {
  g = new dagreD3.graphlib.Graph()
    .setGraph({
      nodesep: 15,
      ranksep: 15,
      rankdir: arrange,
    })
    .setDefaultEdgeLabel(() => ({ lineInterpolate: 'basis' }));

  // set nodes
  nodes.forEach((node) => {
    g.setNode(node.id, node.value);
  });

  // filter out edges that point to non-existent nodes
  const realEdges = edges.filter((e) => {
    const edgeNodes = nodes.filter((n) => n.id === e.u || n.id === e.v);
    return edgeNodes.length === 2;
  });

  // Set edges
  realEdges.forEach((edge) => {
    g.setEdge(edge.u, edge.v, {
      curve: d3.curveBasis,
      arrowheadClass: 'arrowhead',
    });
  });

  innerSvg.call(render, g);
  setUpNodeHighlighting();
  setUpZoomSupport();
};

// rerender graph when filtering dags with dependencies or not
document.getElementById('deps-filter').addEventListener('change', function onChange() {
  // reset searchbox
  document.getElementById('searchbox').value = '';
  if (this.checked) {
    nodes = filteredNodes;
  } else {
    nodes = fullNodes;
  }
  renderGraph();
});

// initial filter check and render
if (document.getElementById('deps-filter').checked) {
  nodes = filteredNodes;
}

renderGraph();
