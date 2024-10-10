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

import axios, { AxiosResponse } from "axios";
import { useQuery } from "react-query";
import ELK, { ElkShape, ElkExtendedEdge } from "elkjs";

import { getMetaValue } from "src/utils";
import { getTextWidth } from "src/utils/graph";

import type { NodeType, DepEdge, DepNode } from "src/types";

export interface DatasetDependencies {
  edges: DepEdge[];
  nodes: DepNode[];
}
interface GenerateProps {
  nodes: DepNode[];
  edges: DepEdge[];
  font: string;
}

export interface DatasetGraph extends ElkShape {
  children: NodeType[];
  edges: ElkExtendedEdge[];
}

const generateGraph = ({ nodes, edges, font }: GenerateProps) => ({
  id: "root",
  layoutOptions: {
    "spacing.nodeNodeBetweenLayers": "40.0",
    "spacing.edgeNodeBetweenLayers": "10.0",
    "layering.strategy": "INTERACTIVE",
    algorithm: "layered",
    "crossingMinimization.semiInteractive": "true",
    "spacing.edgeEdgeBetweenLayers": "10.0",
    "spacing.edgeNode": "10.0",
    "spacing.edgeEdge": "10.0",
    "spacing.nodeNode": "20.0",
    "elk.direction": "DOWN",
  },
  children: nodes.map(({ id, value }) => ({
    id,
    // calculate text width and add space for padding/icon
    width: getTextWidth(value.label, font) + 36,
    height: 40,
    value,
  })),
  edges: edges.map((e) => ({
    id: `${e.source}-${e.target}`,
    sources: [e.source],
    targets: [e.target],
  })),
});

const formatDependencies = async ({ edges, nodes }: DatasetDependencies) => {
  const elk = new ELK();

  // get computed style to calculate how large each node should be
  const font = `bold ${16}px ${
    window.getComputedStyle(document.body).fontFamily
  }`;

  const graph = await elk.layout(generateGraph({ nodes, edges, font }));

  return graph as DatasetGraph;
};

export default function useAssetDependencies() {
  return useQuery("datasetDependencies", async () => {
    const datasetDepsUrl = getMetaValue("dataset_dependencies_url");
    return axios.get<AxiosResponse, DatasetDependencies>(datasetDepsUrl);
  });
}

export const useAssetGraphs = () => {
  const { data: datasetDependencies } = useAssetDependencies();
  return useQuery(["datasetGraphs", datasetDependencies], () => {
    if (datasetDependencies) {
      return formatDependencies(datasetDependencies);
    }
    return undefined;
  });
};
