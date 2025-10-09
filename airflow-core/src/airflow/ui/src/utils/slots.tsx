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

/* eslint-disable perfectionist/sort-objects */
import type { PoolResponse } from "openapi/requests/types.gen";
import { StateIcon } from "src/components/StateIcon";

export type Slots = Omit<
  PoolResponse,
  "description" | "include_deferred" | "name" | "occupied_slots" | "slots"
>;
export type SlotConfig = {
  color: string;
  icon: JSX.Element;
  key: keyof Slots;
};

export const slotConfigs: Array<SlotConfig> = [
  {
    key: "open_slots",
    color: "success",
    icon: <StateIcon color="fg" state="success" />,
  },
  {
    key: "running_slots",
    color: "running",
    icon: <StateIcon color="fg" state="running" />,
  },
  {
    key: "queued_slots",
    color: "queued",
    icon: <StateIcon color="fg" state="queued" />,
  },
  {
    key: "scheduled_slots",
    color: "scheduled",
    icon: <StateIcon color="fg" state="scheduled" />,
  },
  {
    key: "deferred_slots",
    color: "deferred",
    icon: <StateIcon color="fg" state="deferred" />,
  },
];

export const slotKeys: Array<keyof Slots> = [
  "deferred_slots",
  "open_slots",
  "queued_slots",
  "running_slots",
  "scheduled_slots",
];
