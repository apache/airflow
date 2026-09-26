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

// The header banner and the per-try row both look the state up here, so a state added to one
// surface cannot be forgotten on the other: it has to bring a title with it.
const STATE_REASON_DISPLAY = {
  failed: { status: "error", titleKey: "failed" },
  up_for_retry: { status: "warning", titleKey: "upForRetry" },
} as const satisfies Record<string, { status: "error" | "warning"; titleKey: string }>;

export const stateReasonDisplay = (state: string | null | undefined) =>
  state === null || state === undefined
    ? undefined
    : (STATE_REASON_DISPLAY as Record<string, { status: "error" | "warning"; titleKey: string }>)[state];
