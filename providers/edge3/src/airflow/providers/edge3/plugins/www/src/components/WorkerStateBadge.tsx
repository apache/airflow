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
import { Badge, type BadgeProps } from "@chakra-ui/react";
import type { EdgeWorkerState } from "openapi/requests/types.gen";
import * as React from "react";

import { WorkerStateIcon } from "./WorkerStateIcon";

const state2Color = (state: EdgeWorkerState | null | undefined) => {
  switch (state) {
    case "starting":
    case "maintenance request":
    case "maintenance exit":
      return "yellow";
    case "running":
      return "green";
    case "idle":
      return "teal";
    case "shutdown request":
    case "terminating":
      return "purple";
    case "offline":
    case "offline maintenance":
      return "black";
    case "unknown":
      return "red";
    case "maintenance mode":
    case "maintenance pending":
      return "orange";
    default:
      return "gray";
  }
};

const state2TooltipText = (state: EdgeWorkerState | null | undefined) => {
  switch (state) {
    // see enum mapping from providers/edge3/src/airflow/providers/edge3/models/edge_worker.py:EdgeWorkerState
    case "starting":
      return "Edge Worker is in initialization.";
    case "running":
      return "Edge Worker is actively running a task.";
    case "idle":
      return "Edge Worker is active and waiting for a task.";
    case "shutdown request":
      return "Request to shutdown Edge Worker is issued. It will be picked-up on the next heartbeat, tasks will drain and then worker will terminate.";
    case "terminating":
      return "Edge Worker is completing work (draining running tasks) and stopping.";
    case "offline":
      return "Edge Worker was shut down.";
    case "unknown":
      return "No heartbeat signal from worker for some time, Edge Worker probably down or got disconnected.";
    case "maintenance request":
      return "Worker was requested to enter maintenance mode. Once worker receives this message it will pause fetching tasks and drain tasks.";
    case "maintenance pending":
      return "Edge Worker received the request for maintenance, waiting for tasks to finish. Once tasks are finished will move to 'maintenance mode'.";
    case "maintenance mode":
      return "Edge Worker is in maintenance mode. It is online but pauses fetching tasks.";
    case "maintenance exit":
      return "Request Worker is requested to exit maintenance mode. Once the worker receives this state it will un-pause and fetch new tasks.";
    case "offline maintenance":
      return "Worker was shut down in maintenance mode. It will be in maintenance mode when restarted.";
    default:
      return undefined;
  }
};

export type Props = {
  readonly state?: EdgeWorkerState | null;
} & BadgeProps;

export const WorkerStateBadge = React.forwardRef<HTMLDivElement, Props>(
  ({ children, state, ...rest }, ref) => (
    <Badge
      borderRadius="full"
      colorPalette={state2Color(state)}
      fontSize="sm"
      px={children === undefined ? 1 : 2}
      py={1}
      ref={ref}
      title={state2TooltipText(state)}
      variant="solid"
      {...rest}
    >
      {state === undefined ? undefined : <WorkerStateIcon state={state} />}
      {children}
    </Badge>
  ),
);
