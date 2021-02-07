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

const highlight_color = "#000000";
const upstream_color = "#2020A0";
const downstream_color = "#0000FF";
const initialStrokeWidth = '3px';
const highlightStrokeWidth = '5px';
const duration = 500;


function setUpZoomSupport(g) {
  // Set up zoom support for Graph
  zoom = d3.behavior.zoom().on("zoom", function() {
    innerSvg.attr("transform", `translate(${d3.event.translate})scale(${d3.event.scale})`);
  });
  svg.call(zoom);

  // Centering the DAG on load
  // Get Dagre Graph dimensions
  const graphWidth = g.graph().width;
  const graphHeight = g.graph().height;
  // Get SVG dimensions
  const padding = 20;
  let svgBb = svg.node().getBoundingClientRect();
  const width = svgBb.width - padding*2;
  const height = svgBb.height - padding;  // we are not centering the dag vertically

  // Calculate applicable scale for zoom
  const zoomScale = Math.min(
    Math.min(width / graphWidth, height / graphHeight),
    1.5,  // cap zoom level to 1.5 so nodes are not too large
  );

  zoom.translate([(width/2) - ((graphWidth*zoomScale)/2) + padding, padding]);
  zoom.scale(zoomScale);
  zoom.event(innerSvg);
}

function setUpNodeHighlighting(g) {
  d3.selectAll("g.node").on("mouseover", function (d) {
    d3.select(this).selectAll("rect").style("stroke", highlight_color);
    highlight_nodes(g, g.predecessors(d), upstream_color, highlightStrokeWidth);
    highlight_nodes(g, g.successors(d), downstream_color, highlightStrokeWidth)
    adjacent_node_names = [d, ...g.predecessors(d), ...g.successors(d)]
    d3.selectAll("g.nodes g.node")
      .filter(x => !adjacent_node_names.includes(x))
      .style("opacity", 0.2);
    adjacent_edges = g.nodeEdges(d)
    d3.selectAll("g.edgePath")[0]
      .filter(x => !adjacent_edges.includes(x.__data__))
      .forEach(function (x) {
        d3.select(x).style('opacity', .2)
      })
  });

  d3.selectAll("g.node").on("mouseout", function (d) {
    d3.select(this).selectAll("rect").style("stroke", null);
    highlight_nodes(g.predecessors(d), null, initialStrokeWidth)
    highlight_nodes(g.successors(d), null, initialStrokeWidth)
    d3.selectAll("g.node")
      .style("opacity", 1);
    d3.selectAll("g.node rect")
      .style("stroke-width", initialStrokeWidth);
    d3.selectAll("g.edgePath")
      .style("opacity", 1);
  });
}


function highlight_nodes(g, nodes, color, stroke_width) {
  nodes.forEach (function (nodeid) {
    const my_node = g.node(nodeid).elem
    d3.select(my_node)
      .selectAll("rect,circle")
      .style("stroke", color)
      .style("stroke-width", stroke_width) ;
  })
}

function searchboxHighlighting(s) {

  let match = null;

  d3.selectAll("g.nodes g.node").filter(function(d, i){
    if (s==""){
      d3.select("g.edgePaths")
        .transition().duration(duration)
        .style("opacity", 1);
      d3.select(this)
        .transition().duration(duration)
        .style("opacity", 1)
        .selectAll("rect")
        .style("stroke-width", initialStrokeWidth);
    }
    else{
      d3.select("g.edgePaths")
        .transition().duration(duration)
        .style("opacity", 0.2);
      if (node_matches(d, s)) {
        if (!match)
          match = this;
        d3.select(this)
          .transition().duration(duration)
          .style("opacity", 1)
          .selectAll("rect")
          .style("stroke-width", highlightStrokeWidth);
      } else {
        d3.select(this)
          .transition()
          .style("opacity", 0.2).duration(duration)
          .selectAll("rect")
          .style("stroke-width", initialStrokeWidth);
      }
    }
  });

  // This moves the matched node to the center of the graph area
  if(match) {
    let transform = d3.transform(d3.select(match).attr("transform"));

    let svgBb = svg.node().getBoundingClientRect();
    transform.translate = [
      svgBb.width / 2 - transform.translate[0],
      svgBb.height / 2 - transform.translate[1]
    ];
    transform.scale = [1, 1];

    if(zoom != null) {
      zoom.translate(transform.translate);
      zoom.scale(1);
      zoom.event(innerSvg);
    }
  };


}

// Returns true if a node's id or its children's id matches search_text
function node_matches(node_id, search_text) {
  if (node_id.indexOf(search_text) > -1)
    return true;
}

module.exports = {
  setUpZoomSupport: setUpZoomSupport,
  setUpNodeHighlighting: setUpNodeHighlighting,
  searchboxHighlighting: searchboxHighlighting
}
