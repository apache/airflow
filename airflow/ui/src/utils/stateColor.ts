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
import {
  FiActivity,
  FiAlertCircle,
  FiAlertTriangle,
  FiCalendar,
  FiCheckCircle,
  FiCircle,
  FiClock,
  FiList,
  FiRefreshCw,
  FiRepeat,
  FiSkipForward,
  FiSlash,
} from "react-icons/fi";
import { LuCalendarSync } from "react-icons/lu";

export const stateColor = {
  deferred: { color: "purple", icon: FiClock },
  failed: { color: "red", icon: FiAlertTriangle },
  none: { color: "lightblue", icon: FiCircle },
  null: { color: "lightblue", icon: FiCircle },
  queued: { color: "gray", icon: FiList },
  removed: { color: "lightgrey", icon: FiSlash },
  restarting: { color: "violet", icon: FiRepeat },
  running: { color: "lime", icon: FiActivity },
  scheduled: { color: "tan", icon: FiCalendar },
  skipped: { color: "pink", icon: FiSkipForward },
  success: { color: "green", icon: FiCheckCircle },
  up_for_reschedule: { color: "cyan", icon: LuCalendarSync },
  up_for_retry: { color: "yellow", icon: FiRefreshCw },
  upstream_failed: { color: "orange", icon: FiAlertCircle },
};
