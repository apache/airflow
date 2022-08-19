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

import axios, { AxiosResponse } from 'axios';
import { useQuery } from 'react-query';
import type { DepEdge, DepNode } from 'src/types';

import { getMetaValue } from 'src/utils';

interface DatasetDependencies {
  edges: DepEdge[];
  nodes: DepNode[];
}

// Make sure we only show edges that are connected to two nodes.
// Then filter out any nodes without an edge.
const filterDependencies = ({ edges, nodes }: DatasetDependencies) => {
  const newEdges = edges.filter((e) => {
    const edgeNodes = nodes.filter((n) => n.id === e.u || n.id === e.v);
    return edgeNodes.length === 2;
  });

  const newNodes = nodes.filter((n) => newEdges.some((e) => e.u === n.id || e.v === n.id));
  return {
    edges: newEdges,
    nodes: newNodes,
  };
};

const emptyDeps = {
  edges: [],
  nodes: [],
};

export default function useDatasetDependencies() {
  const query = useQuery(
    'datasetDependencies',
    () => {
      const datasetDepsUrl = getMetaValue('dataset_dependencies_url');
      return axios.get<AxiosResponse, DatasetDependencies>(datasetDepsUrl);
    },
    {
      select: filterDependencies,
    },
  );

  return {
    ...query,
    data: query.data ?? emptyDeps,
  };
}
