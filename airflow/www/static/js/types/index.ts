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

import type * as API from "./api-generated";

type RunState = "success" | "running" | "queued" | "failed";

type TaskState =
  | RunState
  | "removed"
  | "scheduled"
  | "shutdown"
  | "restarting"
  | "up_for_retry"
  | "up_for_reschedule"
  | "upstream_failed"
  | "skipped"
  | "deferred"
  | null;

interface Dag {
  id: string;
  rootDagId: string;
  isPaused: boolean;
  isSubdag: boolean;
  owners: Array<string>;
  description: string;
}

interface DagRun {
  runId: string;
  runType: "manual" | "backfill" | "scheduled" | "dataset_triggered";
  state: RunState;
  executionDate: string;
  dataIntervalStart: string;
  dataIntervalEnd: string;
  queuedAt: string | null;
  startDate: string | null;
  endDate: string | null;
  lastSchedulingDecision: string | null;
  externalTrigger: boolean;
  conf: string | null;
  confIsJson: boolean;
  note: string | null;
}

interface TaskInstance {
  runId: string;
  taskId: string;
  startDate: string | null;
  endDate: string | null;
  state: TaskState | null;
  mappedStates?: {
    [key: string]: number;
  };
  mapIndex?: number;
  tryNumber?: number;
  triggererJob?: Job;
  trigger?: Trigger;
  note: string | null;
}

interface Trigger {
  classpath: string | null;
  createdDate: string | null;
}

interface Job {
  latestHeartbeat: string | null;
  hostname: string | null;
}

interface Task {
  id: string | null;
  label: string | null;
  instances: TaskInstance[];
  tooltip?: string;
  children?: Task[];
  extraLinks?: string[];
  isMapped?: boolean;
  operator?: string;
  hasOutletDatasets?: boolean;
  triggerRule?: API.TriggerRule;
}

type RunOrdering = (
  | "dataIntervalStart"
  | "executionDate"
  | "dataIntervalEnd"
)[];

interface DepNode {
  id: string;
  value: {
    id?: string;
    class: "dag" | "dataset" | "trigger" | "sensor";
    label: string;
    rx: number;
    ry: number;
    isOpen?: boolean;
    isJoinNode?: boolean;
    childCount?: number;
  };
  children?: DepNode[];
}

interface DepEdge {
  source: string;
  target: string;
}

interface DatasetListItem extends API.Dataset {
  lastDatasetUpdate: string | null;
  totalUpdates: number;
}

type MinimalTaskInstance = Pick<TaskInstance, "taskId" | "mapIndex" | "runId">;

export type {
  API,
  MinimalTaskInstance,
  Dag,
  DagRun,
  DatasetListItem,
  DepEdge,
  DepNode,
  RunOrdering,
  RunState,
  Task,
  TaskInstance,
  TaskState,
};
