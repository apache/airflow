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

import { emptyGridData } from "./useGridData";
import type { GridData } from "./useGridData";

const setDagRunNoteURI = getMetaValue("set_dag_run_note");

interface Props {
  dagId: string;
  runId: string;
}

export default function useSetDagRunNote({ dagId, runId }: Props) {
  const queryClient = useQueryClient();
  const errorToast = useErrorToast();
  const setDagRunNote = setDagRunNoteURI.replace("_DAG_RUN_ID_", runId);

  return useMutation(
    ["setDagRunNote", dagId, runId],
    (note: string | null) =>
      axios.patch<AxiosResponse, API.DAGRun>(setDagRunNote, { note }),
    {
      onSuccess: async (data) => {
        const note = data.note ?? null;

        const updateGridData = (oldGridData: GridData | undefined) =>
          !oldGridData
            ? emptyGridData
            : {
                ...oldGridData,
                dagRuns: oldGridData.dagRuns.map((dr) =>
                  dr.runId === runId ? { ...dr, note } : dr
                ),
              };

        await queryClient.cancelQueries("gridData");
        queryClient.setQueriesData("gridData", updateGridData);
      },
      onError: (error: Error) => errorToast({ error }),
    }
  );
}
