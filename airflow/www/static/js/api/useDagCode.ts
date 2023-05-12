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
import { useRef, useEffect, useState } from "react";

import { getMetaValue } from "src/utils";
import type { API } from "src/types";
import useErrorToast from "../utils/useErrorToast";

interface DagCodeData {
  lastParsedTime?: string;
  codeSource?: string;
}

export default function useDagCode() {
  const errorToast = useErrorToast();
  const [data, setData] = useState<DagCodeData>({
    lastParsedTime: "N/A",
    codeSource: "",
  });

  const dagQuery = useQuery(
    ["dagQuery"],
    () => {
      const dagApiUrl = getMetaValue("dag_api");
      return axios.get<AxiosResponse, API.DAG>(dagApiUrl);
    },
    {
      onError: (error: Error) => {
        errorToast({
          error,
        });
      },
    }
  );

  const dagSourceQuery = useQuery(
    ["dagSourceQuery"],
    () => {
      const fileToken = dagQuery.data?.fileToken || "";
      const dagSourceApiUrl = getMetaValue("dag_source_api").replace(
        "_FILE_TOKEN_",
        fileToken
      );
      return axios.get<AxiosResponse, string>(dagSourceApiUrl, {
        headers: { Accept: "text/plain" },
      });
    },
    {
      enabled: false,
      onError: (error: Error) => {
        errorToast({
          error,
        });
      },
    }
  );
  const dagSourceQueryRef = useRef(dagSourceQuery);
  useEffect(() => {
    if (dagQuery.isSuccess) {
      dagSourceQueryRef.current.refetch();
    }
  }, [dagQuery.isSuccess, dagSourceQueryRef]);

  useEffect(() => {
    if (dagQuery.isSuccess && dagSourceQuery.isSuccess) {
      setData({
        lastParsedTime: dagQuery.data?.lastParsedTime || "N/A",
        codeSource: dagSourceQuery.data || "",
      });
    }
  }, [
    dagQuery.isSuccess,
    dagQuery.data,
    dagSourceQuery.isSuccess,
    dagSourceQuery.data,
  ]);

  return data;
}
