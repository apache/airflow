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
import { useState, useCallback } from "react";
import type { Task } from "../types";

interface SelectionProps {
  runId?: string | null;
  taskId?: string | null;
  mapIndex?: number;
}

const useMultipleSelection = (
  groups: Task,
  dagRunIds: string[],
  openGroupIds: string[]
) => {
  const [selectedTaskInstances, setSelectedTaskInstances] = useState<
    SelectionProps[]
  >([]);

  const clearSelectionTasks = useCallback(() => {
    setSelectedTaskInstances([]);
  }, []);

  const onAddSelectedTask = useCallback(
    ({ runId, taskId, mapIndex }: SelectionProps) => {
      setSelectedTaskInstances((tis) => {
        const isRepeatedTask = tis.some(
          (ti) => ti.taskId === taskId && ti.runId === runId
        );
        if (!isRepeatedTask) {
          return [...tis, { runId, taskId, mapIndex }];
        }
        return tis;
      });
    },
    []
  );

  const selectedRunIds = useCallback(
    (topRunId?: string | null, botRunId?: string | null) => {
      if (topRunId === botRunId) return [topRunId];

      const selectedRunIdsArr: string[] = [];
      let isInRunBlock = false;

      dagRunIds.forEach((runId: string) => {
        const isBlockLimit = topRunId === runId || botRunId === runId;
        if (!isInRunBlock && isBlockLimit) {
          isInRunBlock = true;
        } else if (isInRunBlock && isBlockLimit) {
          isInRunBlock = false;
          selectedRunIdsArr.push(runId);
        }
        if (isInRunBlock) {
          selectedRunIdsArr.push(runId);
        }
      });

      return selectedRunIdsArr;
    },
    [dagRunIds]
  );

  const addTaskBlock = useCallback(
    (
      tasks: Task[],
      topTask: SelectionProps,
      bottomTask: SelectionProps,
      selectedRunIdsArr: (string | null | undefined)[],
      isInTaskBlock = false
    ) => {
      tasks.forEach((task) => {
        const isOpen = openGroupIds.some((g) => g === task.id);
        const isBlockLimit =
          task.id === topTask.taskId || task.id === bottomTask.taskId;
        if (isBlockLimit) isInTaskBlock = !isInTaskBlock;
        if (isInTaskBlock || isBlockLimit) {
          task.instances.forEach((ti) => {
            const isInRunBlock = selectedRunIdsArr.some(
              (runId) => runId === ti.runId
            );
            if (isInRunBlock) {
              onAddSelectedTask({ runId: ti.runId, taskId: task.id });
            }
          });
          if (topTask.taskId === bottomTask.taskId) {
            isInTaskBlock = false;
          }
        }
        if (isOpen) {
          isInTaskBlock = addTaskBlock(
            task?.children || [],
            topTask,
            bottomTask,
            selectedRunIdsArr,
            isInTaskBlock
          );
        }
      });
      return isInTaskBlock;
    },
    [openGroupIds, onAddSelectedTask]
  );

  const onAddSelectedTaskBlock = useCallback(
    ({ runId, taskId, mapIndex }: SelectionProps) => {
      if (selectedTaskInstances.length === 0) {
        onAddSelectedTask({ runId, taskId, mapIndex });
      } else {
        const lastTask =
          selectedTaskInstances[selectedTaskInstances.length - 1];
        const selectedRunIdsArr = selectedRunIds(lastTask.runId, runId);
        addTaskBlock(
          groups?.children || [],
          lastTask,
          { runId, taskId, mapIndex },
          selectedRunIdsArr
        );
      }
    },
    [
      selectedTaskInstances,
      onAddSelectedTask,
      selectedRunIds,
      addTaskBlock,
      groups,
    ]
  );

  return {
    selectedTaskInstances,
    onAddSelectedTask,
    clearSelectionTasks,
    onAddSelectedTaskBlock,
  };
};

export default useMultipleSelection;
