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
import type { DatasetListItem } from 'src/types';

interface DatasetsData {
  datasets: DatasetListItem[];
  totalEntries: number;
}

interface Props {
  limit?: number;
  offset?: number;
  order?: string;
  uri?: string;
}

export default function useDatasets({
  limit, offset, order, uri,
}: Props) {
  const query = useQuery(
    ['datasets', limit, offset, order, uri],
    () => {
      const datasetsUrl = getMetaValue('datasets_api');
      const orderParam = order ? { order_by: order } : {};
      const uriParam = uri ? { uri_pattern: uri } : {};
      return axios.get<AxiosResponse, DatasetsData>(
        datasetsUrl,
        {
          params: {
            offset, limit, ...orderParam, ...uriParam,
          },
        },
      );
    },
    {
      keepPreviousData: true,
    },
  );
  return {
    ...query,
    data: query.data ?? { datasets: [], totalEntries: 0 },
  };
}
