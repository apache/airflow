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
import type { IconBaseProps } from "react-icons";
import { FiActivity, FiCalendar, FiRepeat, FiSkipForward, FiSlash, FiWatch, FiX } from "react-icons/fi";
import { LuCalendarSync, LuCheck, LuCircleDashed, LuCircleFadingArrowUp, LuRedo2 } from "react-icons/lu";
import { PiQueue } from "react-icons/pi";

import type { TaskInstanceState } from "openapi/requests/types.gen";

type Props = {
  readonly state?: TaskInstanceState | null;
} & IconBaseProps;

export const StateIcon = ({ state, ...rest }: Props) => {
  // false positive eslint - we have a default.
  // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
  switch (state) {
    case "deferred":
      return <FiWatch {...rest} />;
    case "failed":
      return <FiX {...rest} />;
    case "queued":
      return <PiQueue {...rest} />;
    case "removed":
      return <FiSlash {...rest} />;
    case "restarting":
      return <FiRepeat {...rest} />;
    case "running":
      return <FiActivity {...rest} />;
    case "scheduled":
      return <FiCalendar {...rest} />;
    case "skipped":
      return <FiSkipForward {...rest} />;
    case "success":
      return <LuCheck {...rest} />;
    case "up_for_reschedule":
      return <LuCalendarSync {...rest} />;
    case "up_for_retry":
      return <LuRedo2 {...rest} />;
    case "upstream_failed":
      return <LuCircleFadingArrowUp {...rest} />;
    default:
      return <LuCircleDashed {...rest} />;
  }
};
