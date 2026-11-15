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
import type { RefObject } from "react";

import type { IconButtonProps } from "@chakra-ui/react";

import type {
  DAGDetailsResponse,
  DAGResponse,
  DAGRunResponse,
  PluginLinkActionResponse,
  PluginReactActionResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";

import type { PluginProps } from "src/pages/ReactPlugin";

/**
 * The `ui_actions` element type.
 *
 * Declared by hand because the discriminated union is inlined into `PluginResponse` by the
 * generator -- a `Field(discriminator=...)` annotation emits an `anyOf` with no schema name of
 * its own, so there is nothing for it to name.
 */
export type PluginActionResponse = PluginLinkActionResponse | PluginReactActionResponse;

export type PluginActionDestination = PluginActionResponse["destination"];

/**
 * The record a surface is rendering actions for.
 *
 * Discriminated on `destination` so `pluginActionContext` is exhaustiveness-checked against it:
 * adding a destination to the API literal then fails to compile until the context it provides
 * is spelled out.
 */
export type PluginActionEntity =
  | {
      dag: DAGResponse;
      /**
       * The full Dag record, which only the Dag detail page has. A Dags-list row carries 15
       * fewer fields and so cannot stand in for one; actions there get `dagId` and fetch the
       * rest themselves if they need it.
       */
      dagDetails?: DAGDetailsResponse;
      destination: "dag";
    }
  | { dagRun: DAGRunResponse; destination: "dag_run" }
  | { destination: "task_instance"; taskInstance: TaskInstanceResponse };

/**
 * What a React action's component is handed, on top of the entity context every plugin gets.
 *
 * Declared here rather than on the shared `PluginProps`, so a `react_apps` plugin -- which is
 * mounted on a route and never gets these -- is not told it receives them.
 */
export type PluginActionProps = {
  /** Puts the trigger button back to its untoggled state. */
  onClose: () => void;
  /** Whether the button is currently toggled on; the component follows it rather than unmounting. */
  open: boolean;
  /** The trigger button, for a popover to anchor against: the button belongs to Airflow. */
  triggerRef: RefObject<HTMLButtonElement | null>;
} & PluginProps;

/** The subset of button styling a surface needs to make plugin actions match their neighbors. */
export type PluginActionButtonProps = Partial<Pick<IconButtonProps, "bg" | "size" | "variant">>;
