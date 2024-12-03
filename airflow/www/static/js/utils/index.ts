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

import Color from "color";
import type { DagRun, RunOrdering, Task, TaskInstance } from "src/types";

import { LogLevel } from "src/dag/details/taskInstance/Logs/utils";
import useOffsetTop from "./useOffsetTop";

// Delay in ms for various hover actions
const hoverDelay = 200;

function getMetaValue(name: string) {
  const elem = document.querySelector(`meta[name="${name}"]`);
  if (!elem) {
    return "";
  }
  return elem.getAttribute("content") || "";
}

const finalStatesMap = () =>
  new Map([
    ["success", 0],
    ["failed", 0],
    ["upstream_failed", 0],
    ["up_for_retry", 0],
    ["up_for_reschedule", 0],
    ["running", 0],
    ["deferred", 0],
    ["queued", 0],
    ["scheduled", 0],
    ["skipped", 0],
    ["no_status", 0],
  ]);

interface GroupSummaryProps {
  group: Task;
  runId?: string;
  mappedStates: TaskInstance["mappedStates"];
}

const getGroupAndMapSummary = ({
  group,
  runId,
  mappedStates,
}: GroupSummaryProps) => {
  let totalTasks = 0;
  const childTaskMap = finalStatesMap();
  if (!!group.children && !group.isMapped) {
    group.children?.forEach((child) => {
      const taskInstance = child.instances.find((ti) => ti.runId === runId);
      if (taskInstance) {
        const stateKey =
          taskInstance.state == null ? "no_status" : taskInstance.state;
        if (childTaskMap.has(stateKey)) {
          childTaskMap.set(stateKey, (childTaskMap.get(stateKey) || 0) + 1);
        }
      }
    });
  } else if (group.isMapped && mappedStates) {
    Object.entries(mappedStates).forEach(([key, value]) => {
      totalTasks += value;
      childTaskMap.set(key || "no_status", value);
    });
  }

  return {
    totalTasks,
    childTaskMap,
  };
};

const appendSearchParams = (
  url: string | null,
  params: URLSearchParams | string
) => {
  if (!url) return "";
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}${params}`;
};

interface GetTaskProps {
  task: Task;
  taskId: Task["id"];
}

const getTask = ({ taskId, task }: GetTaskProps) => {
  if (task.id === taskId) return task;
  if (task.children) {
    let foundTask;
    task.children.forEach((c) => {
      const childTask = getTask({ taskId, task: c });
      if (childTask) foundTask = childTask;
    });
    return foundTask;
  }
  return null;
};

interface SummaryProps {
  task: Task;
  taskCount?: number;
  groupCount?: number;
  operators?: Record<string, number>;
}

const getTaskSummary = ({
  task,
  taskCount = 0,
  groupCount = 0,
  operators = {},
}: SummaryProps) => {
  let tc = taskCount;
  let gc = groupCount;
  const op = operators;
  if (task.children) {
    if (task.id) {
      // Don't count the root
      gc += 1;
    }
    task.children.forEach((c) => {
      const childSummary = getTaskSummary({
        task: c,
        taskCount: tc,
        groupCount: gc,
        operators: op,
      });
      if (childSummary) {
        tc = childSummary.taskCount;
        gc = childSummary.groupCount;
      }
    });
  } else {
    if (task.operator) {
      if (!op[task.operator]) {
        op[task.operator] = 1;
      } else if (operators[task.operator]) {
        op[task.operator] += 1;
      }
    }
    tc += 1;
  }
  return {
    taskCount: tc,
    groupCount: gc,
    operators: op,
  };
};

interface RunLabelProps {
  dagRun: DagRun;
  ordering?: RunOrdering;
}

const getDagRunLabel = ({
  dagRun,
  ordering = ["executionDate"],
}: RunLabelProps) => dagRun[ordering[0]];

const getStatusBackgroundColor = (color: string, hasNote: boolean) =>
  hasNote
    ? `linear-gradient(-135deg, ${Color(color).hex()}60 5px, ${color} 0);`
    : color;

const toSentenceCase = (camelCase: string): string => {
  if (camelCase) {
    const result = camelCase.replace(/([A-Z])/g, " $1");
    return result[0].toUpperCase() + result.substring(1).toLowerCase();
  }
  return "";
};

const highlightByKeywords = (
  parsedLine: string,
  currentLogLevel: string,
  errorKeywords: string[],
  warningKeywords: string[],
  logGroupStart: RegExp,
  logGroupEnd: RegExp
): string => {
  // Don't color log marker lines that are already highlighted.
  if (logGroupStart.test(parsedLine) || logGroupEnd.test(parsedLine)) {
    return parsedLine;
  }

  const lowerParsedLine = parsedLine.toLowerCase();
  const red = (line: string) => `\x1b[1m\x1b[31m${line}\x1b[39m\x1b[0m`;
  const yellow = (line: string) => `\x1b[1m\x1b[33m${line}\x1b[39m\x1b[0m`;

  const containsError = errorKeywords.some((keyword) =>
    lowerParsedLine.includes(keyword)
  );

  if (containsError || currentLogLevel === (LogLevel.ERROR as string)) {
    return red(parsedLine);
  }

  const containsWarning = warningKeywords.some((keyword) =>
    lowerParsedLine.includes(keyword)
  );

  if (containsWarning || currentLogLevel === (LogLevel.WARNING as string)) {
    return yellow(parsedLine);
  }

  return parsedLine;
};

export {
  hoverDelay,
  finalStatesMap,
  getGroupAndMapSummary,
  getMetaValue,
  appendSearchParams,
  getTask,
  getTaskSummary,
  getDagRunLabel,
  getStatusBackgroundColor,
  useOffsetTop,
  toSentenceCase,
  highlightByKeywords,
};
