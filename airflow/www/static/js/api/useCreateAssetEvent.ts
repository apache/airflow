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
import { useMutation, useQueryClient } from "react-query";

import { getMetaValue } from "src/utils";
import type { API } from "src/types";
import useErrorToast from "src/utils/useErrorToast";

interface Props {
  datasetId?: number;
  uri?: string;
}

const createAssetUrl = getMetaValue("create_asset_event_api");

export default function useCreateAssetEvent({ datasetId, uri }: Props) {
  const queryClient = useQueryClient();
  const errorToast = useErrorToast();

  return useMutation(
    ["createAssetEvent", uri],
    (extra?: API.AssetEvent["extra"]) =>
      axios.post<AxiosResponse, API.CreateAssetEventVariables>(createAssetUrl, {
        asset_uri: uri,
        extra: extra || {},
      }),
    {
      onSuccess: () => {
        queryClient.invalidateQueries(["datasets-events", datasetId]);
      },
      onError: (error: Error) => errorToast({ error }),
    }
  );
}
