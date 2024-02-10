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

import { useQuery } from "react-query";
import axios, { AxiosResponse } from "axios";

import { getMetaValue } from "src/utils";
import useFilters, {
  FILTER_DOWNSTREAM_PARAM,
  FILTER_UPSTREAM_PARAM,
  ROOT_PARAM,
} from "src/dag/useFilters";
import type { WebserverEdge, DepNode } from "src/types";

const DAG_ID_PARAM = "dag_id";

const dagId = getMetaValue(DAG_ID_PARAM);
const graphDataUrl = getMetaValue("graph_data_url");

interface GraphData {
  edges: WebserverEdge[];
  nodes: DepNode;
  arrange: string;
}

const useGraphData = () => {
  const {
    filters: { root, filterDownstream, filterUpstream },
  } = useFilters();

  return useQuery(
    ["graphData", root, filterUpstream, filterDownstream],
    async () => {
      const params = {
        [DAG_ID_PARAM]: dagId,
        [ROOT_PARAM]: root,
        [FILTER_UPSTREAM_PARAM]: filterUpstream,
        [FILTER_DOWNSTREAM_PARAM]: filterDownstream,
      };
      return axios.get<AxiosResponse, GraphData>(graphDataUrl, { params });
    }
  );
};

export default useGraphData;
