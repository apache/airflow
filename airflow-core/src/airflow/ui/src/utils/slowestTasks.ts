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

import { median } from "./median";

// Only the fields the ranking needs, so callers can pass full task instances and
// tests can build minimal fixtures.
export type SlowestTaskInput = Pick<
  TaskInstanceResponse,
  "duration" | "state" | "task_display_name" | "task_id"
>;

export type TaskDurationSummary = {
  readonly latestState: TaskInstanceResponse["state"];
  readonly medianDuration: number;
  readonly runCount: number;
  readonly taskDisplayName: string;
  readonly taskId: string;
};

type Accumulator = {
  durations: Array<number>;
  latestState: TaskInstanceResponse["state"];
  taskDisplayName: string;
};

/**
 * Rank tasks by their median duration across the given task instances.
 *
 * Instances are expected newest-first (ordered by `-run_after`), so the first
 * one seen for a task supplies its latest state. Instances without a duration
 * (still running, queued, skipped) are ignored.
 */
export const aggregateSlowestTasks = (
  taskInstances: Array<SlowestTaskInput>,
  topN: number,
): Array<TaskDurationSummary> => {
  const byTask = new Map<string, Accumulator>();

  for (const taskInstance of taskInstances) {
    if (taskInstance.duration !== null) {
      const existing = byTask.get(taskInstance.task_id);

      if (existing) {
        existing.durations.push(taskInstance.duration);
      } else {
        byTask.set(taskInstance.task_id, {
          durations: [taskInstance.duration],
          latestState: taskInstance.state,
          taskDisplayName: taskInstance.task_display_name,
        });
      }
    }
  }

  return [...byTask.entries()]
    .map(([taskId, accumulator]) => ({
      latestState: accumulator.latestState,
      medianDuration: median(accumulator.durations),
      runCount: accumulator.durations.length,
      taskDisplayName: accumulator.taskDisplayName,
      taskId,
    }))
    .sort((first, second) => second.medianDuration - first.medianDuration)
    .slice(0, topN);
};
