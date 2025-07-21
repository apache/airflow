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
import type { GridTask, RunWithDuration } from "src/layouts/Details/Grid/utils";
import type { NavigationIndices } from "../useGridNavigation";

export type ArrowKey = "ArrowDown" | "ArrowLeft" | "ArrowRight" | "ArrowUp";

type NavigationContext = {
  groupId: string;
  runId: string;
  taskId: string;
};

type NavigationTarget = {
  isValid: boolean;
  run: RunWithDuration | undefined;
  task: GridTask | undefined;
};

export class NavigationCalculator {
  private readonly context: NavigationContext;
  private readonly flatNodes: Array<GridTask>;
  private readonly runs: Array<RunWithDuration>;

  public constructor(
    flatNodes: Array<GridTask>,
    runs: Array<RunWithDuration>,
    context: NavigationContext
  ) {
    this.flatNodes = flatNodes;
    this.runs = runs;
    this.context = context;
  }

  public static areIndicesEqual(first: NavigationIndices, second: NavigationIndices): boolean {
    return first.runIndex === second.runIndex && first.taskIndex === second.taskIndex;
  }

  private static isExpandedGroup(task: GridTask): boolean {
    return Boolean(
      task.isGroup &&
      task.firstChildIndex !== undefined
    );
  }

  public calculateNewIndices(
    key: ArrowKey,
    isJump: boolean,
    base?: NavigationIndices
  ): NavigationIndices {
    const current = base ?? this.getCurrentIndices();

    switch (key) {
      case "ArrowDown":
        return {
          ...current,
          taskIndex: this.getNextTaskIndex(current.taskIndex, 1, isJump)
        };
      case "ArrowLeft":
        return {
          ...current,
          runIndex: this.getNextRunIndex(current.runIndex, 1, isJump)
        };
      case "ArrowRight":
        return {
          ...current,
          runIndex: this.getNextRunIndex(current.runIndex, -1, isJump)
        };
      case "ArrowUp":
        return {
          ...current,
          taskIndex: this.getNextTaskIndex(current.taskIndex, -1, isJump)
        };
      default:
        return current;
    }
  }

  public getCurrentIndices = (): NavigationIndices => {
    const runIndex = Math.max(
      0,
      this.runs.findIndex((run) => run.dag_run_id === this.context.runId)
    );

    const currentTaskId = this.context.groupId || this.context.taskId;
    const taskIndex = Math.max(
      0,
      this.flatNodes.findIndex((node) => node.id === currentTaskId)
    );

    return { runIndex, taskIndex };
  };

  public getNavigationTarget(indices: NavigationIndices): NavigationTarget {
    const run = this.runs[indices.runIndex] ?? undefined;
    const task = this.flatNodes[indices.taskIndex] ?? undefined;
    const isValid = Boolean(run && task);

    return { isValid, run, task };
  }

  public getNextRunIndex(current: number, direction: -1 | 1, isJump: boolean): number {
    if (isJump) {
      return direction > 0 ? this.runs.length - 1 : 0;
    }

    return Math.max(0, Math.min(this.runs.length - 1, current + direction));
  }

  public getNextTaskIndex(current: number, direction: -1 | 1, isJump: boolean): number {
    if (isJump) {
      return direction > 0 ? this.flatNodes.length - 1 : 0;
    }

    const next = current + direction;

    if (next < 0 || next >= this.flatNodes.length) {
      return current;
    }

    const currentTask = this.flatNodes[current];
    const isMovingDown = direction === 1;

    if (isMovingDown && currentTask && NavigationCalculator.isExpandedGroup(currentTask)) {
      return currentTask.firstChildIndex ?? current;
    }

    if (currentTask && this.shouldReturnToParent(currentTask, isMovingDown)) {
      const parentIndex = this.findParentIndex(currentTask, current);

      return parentIndex === -1 ? next : parentIndex;
    }

    return next;
  }

  private findParentIndex(currentTask: GridTask, currentIndex: number): number {
    return this.flatNodes.findIndex((node, index) =>
      index < currentIndex && node.id === currentTask.parentId
    );
  }

  private shouldReturnToParent(
    currentTask: GridTask,
    isMovingDown: boolean
  ): boolean {
    return (
      !isMovingDown &&
      Boolean(this.context.taskId) &&
      !this.context.groupId &&
      Boolean(currentTask.isFirstChildOfParent) &&
      Boolean(currentTask.parentId)
    );
  }
}
