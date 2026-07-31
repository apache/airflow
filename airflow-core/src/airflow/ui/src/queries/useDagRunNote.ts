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
import type { DAGRunResponse } from "openapi/requests/types.gen";

import { useNoteEditor } from "./useNoteEditor";
import { usePatchDagRun } from "./usePatchDagRun";

/**
 * Note-editing state and save logic for a Dag run.
 * Used by both the detail-page NotePreview and the list-page RunNoteButton
 * so mutation + cache invalidation stays in one place.
 */
export const useDagRunNote = (dagRun: DAGRunResponse) => {
  const { isPending, mutate } = usePatchDagRun({
    dagId: dagRun.dag_id,
    dagRunId: dagRun.dag_run_id,
  });

  return useNoteEditor({
    isPending,
    mutateNote: (note, options) =>
      mutate({ dagId: dagRun.dag_id, dagRunId: dagRun.dag_run_id, requestBody: { note } }, options),
    savedNote: dagRun.note,
  });
};
