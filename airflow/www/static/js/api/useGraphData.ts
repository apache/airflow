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
import type { DepNode } from "src/types";

const DAG_ID_PARAM = "dag_id";

const dagId = getMetaValue(DAG_ID_PARAM);
const graphDataUrl = getMetaValue("graph_data_url");
const urlRoot = getMetaValue("root");

interface GraphData {
  edges: WebserverEdge[];
  nodes: DepNode;
  arrange: string;
}
export interface WebserverEdge {
  label?: string;
  sourceId: string;
  targetId: string;
}

const useGraphData = () =>
  useQuery("graphData", async () => {
    const params = {
      [DAG_ID_PARAM]: dagId,
      root: urlRoot || undefined,
      filter_upstream: true,
      filter_downstream: true,
    };
    return axios.get<AxiosResponse, GraphData>(graphDataUrl, { params });
  });

export default useGraphData;
