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

import axios from 'axios';
import { useMutation, useQueryClient } from 'react-query';
import { getMetaValue } from 'src/utils';
import useErrorToast from '../utils/useErrorToast';
import type { GridData } from './useGridData';
import { emptyGridData } from './useGridData';

const setDagRunNotesURI = getMetaValue('set_dag_run_note');

export default function useSetDagRunNotes(
  dagId: string,
  runId: string,
  newNotesValue: string,
) {
  const queryClient = useQueryClient();
  const errorToast = useErrorToast();
  const setDagRunNotes = setDagRunNotesURI.replace('_DAG_RUN_ID_', runId);

  const updateGridData = (oldValue: GridData | undefined) => {
    if (oldValue == null) return emptyGridData;
    const run = oldValue.dagRuns.find((dr) => dr.runId === runId);
    if (run) {
      run.notes = newNotesValue;
    }
    return oldValue;
  };

  return useMutation(
    ['setDagRunNotes', dagId, runId],
    () => axios.patch(setDagRunNotes, { notes: newNotesValue }),
    {

      onSuccess: async () => {
        // Cancel any outgoing refetches (so they don't overwrite our optimistic update)
        await queryClient.cancelQueries('gridData');
        // Optimistically update to the new value
        queryClient.setQueriesData('gridData', updateGridData);
      },
      onError: (error: Error) => errorToast({ error }),
    },
  );
}
