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
      variant="solid"
      {...rest}
    >
      {state === undefined ? undefined : <WorkerStateIcon state={state} />}
      {children}
    </Badge>
  ),
);
