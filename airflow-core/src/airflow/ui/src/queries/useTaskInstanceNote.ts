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
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { useNoteEditor } from "./useNoteEditor";
import { usePatchTaskInstance } from "./usePatchTaskInstance";

/**
 * Note-editing state and save logic for a task instance.
 * Used by both the detail-page NotePreview and the list-page TaskInstanceNoteButton
 * so mutation + cache invalidation stays in one place.
 */
export const useTaskInstanceNote = (taskInstance: TaskInstanceResponse) => {
  const { isPending, mutate } = usePatchTaskInstance({
    dagId: taskInstance.dag_id,
    dagRunId: taskInstance.dag_run_id,
    mapIndex: taskInstance.map_index,
    taskId: taskInstance.task_id,
  });

  return useNoteEditor({
    isPending,
    mutateNote: (note, options) =>
      mutate(
        {
          dagId: taskInstance.dag_id,
          dagRunId: taskInstance.dag_run_id,
          mapIndex: taskInstance.map_index,
          requestBody: { note },
          taskId: taskInstance.task_id,
        },
        options,
      ),
    savedNote: taskInstance.note,
  });
};
