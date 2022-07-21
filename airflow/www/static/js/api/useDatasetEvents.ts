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

import { getMetaValue } from 'src/utils';
import type { API } from 'src/types';

interface DatasetEventsData {
  datasetEvents: API.DatasetEvent[];
  totalEntries: number;
}

interface Props {
  datasetId?: string;
  dagId?: string;
  taskId?: string;
  runId?: string;
  mapIndex?: number;
  limit?: number;
  offset?: number;
  order?: string;
}

export default function useDatasetEvents({
  datasetId, dagId, runId, taskId, mapIndex, limit, offset, order,
}: Props) {
  const query = useQuery(
    ['datasets-events', datasetId, dagId, runId, taskId, mapIndex, limit, offset, order],
    () => {
      const datasetsUrl = getMetaValue('dataset_events_api') || '/api/v1/datasets/events';

      const params = new URLSearchParams();

      if (limit) params.set('limit', limit.toString());
      if (offset) params.set('offset', offset.toString());
      if (order) params.set('order_by', order);
      if (datasetId) params.set('dataset_id', datasetId);
      if (dagId) params.set('source_dag_id', dagId);
      if (runId) params.set('source_run_id', runId);
      if (taskId) params.set('source_task_id', taskId);
      if (mapIndex) params.set('source_map_index', mapIndex.toString());

      return axios.get<AxiosResponse, DatasetEventsData>(datasetsUrl, {
        params,
      });
    },
    {
      keepPreviousData: true,
    },
  );
  return {
    ...query,
    data: query.data ?? { datasetEvents: [], totalEntries: 0 },
  };
}
