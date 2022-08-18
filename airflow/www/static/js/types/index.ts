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

import * as API from './api-generated';

type RunState = 'success' | 'running' | 'queued' | 'failed';

type TaskState = RunState
| 'removed'
| 'scheduled'
| 'shutdown'
| 'restarting'
| 'up_for_retry'
| 'up_for_reschedule'
| 'upstream_failed'
| 'skipped'
| 'deferred'
| null;

interface Dag {
  id: string,
  rootDagId: string,
  isPaused: boolean,
  isSubdag: boolean,
  owners: Array<string>,
  description: string,
}

interface DagRun {
  runId: string;
  runType: 'manual' | 'backfill' | 'scheduled' | 'dataset_triggered';
  state: RunState;
  executionDate: string;
  dataIntervalStart: string;
  dataIntervalEnd: string;
  startDate: string | null;
  endDate: string | null;
  lastSchedulingDecision: string | null;
}

interface TaskInstance {
  runId: string;
  taskId: string;
  startDate: string | null;
  endDate: string | null;
  state: TaskState | null;
  mappedStates?: {
    [key: string]: number;
  },
  mapIndex?: number;
  tryNumber?: number;
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
}

export type {
  Dag,
  DagRun,
  RunState,
  TaskState,
  TaskInstance,
  Task,
  API,
};
